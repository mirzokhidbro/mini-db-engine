package storage

import (
	"errors"
	"fmt"
)

type NodeType int8

const (
	NodeTypeRootInternal NodeType = iota // Root node with children
	NodeTypeRootLeaf                     // Root node without children (also a leaf)
	NodeTypeInternal                     // Internal node (non-root, has children)
	NodeTypeLeaf                         // Leaf node (non-root, no children)
)

const (
	MaxKeys              = 5
	NodeSize             = 8192 // 8KB
	IndexHeaderSize      = 32
	RecordListHeaderSize = 16 // FreeSpacePointer(8) + BlockCount(8)
	RecordListBlockSize  = 109
)

type IndexHeader struct {
	RootPointer      int64
	FreeSpacePointer int64
	NodeCount        int64
	Height           int64
}

type RecordLocation struct {
	PageID   int64
	RecordID int16
}

type RecordListBlock struct {
	Locations []RecordLocation
	Count     int8
	NextBlock int64
}

type Node struct {
	Values        []ValueEntry
	ChildPointers []uint64
	NodeType      NodeType
	KeyCount      int8
	Address       int64
	ParentAddress int64
}

type ValueEntry struct {
	Value          int64
	RecordListHead int64
}

type RecordListFileHeader struct {
	FreeSpacePointer int64
	BlockCount       int64
}

func (tm *TableManager) CreateIndex(tableName string, columnName string) error {
	indexFileName := tableName + "_" + columnName + ".index"
	if tm.FileManager.FileExists(indexFileName) {
		return errors.New("index already exists")
	}

	_, err := tm.FileManager.CreateFile(indexFileName)
	if err != nil {
		return err
	}

	indexHeader := IndexHeader{
		RootPointer:      IndexHeaderSize,
		FreeSpacePointer: IndexHeaderSize + NodeSize,
		NodeCount:        1,
		Height:           1,
	}

	headerBinary := SerializeIndexHeader(indexHeader)
	rootBinary := createEmptyRootNode()
	binaryData := append(headerBinary, rootBinary...)

	if err := tm.FileManager.Write(indexFileName, 0, binaryData); err != nil {
		return err
	}

	if err := tm.CreateRecordListFile(tableName, columnName); err != nil {
		return err
	}

	return nil
}

func (tm *TableManager) InsetValueToIndex(indexName string, value int64, location RecordLocation) error {
	header, err := tm.GetIndexHeader(indexName)
	if err != nil {
		return fmt.Errorf("failed to get index header: %v", err)
	}

	root, err := tm.GetNode(indexName, int64(header.RootPointer))
	if err != nil {
		return fmt.Errorf("failed to get root node: %v", err)
	}

	recordListFileName := indexName[:len(indexName)-6] + ".record_list_file"

	if root.KeyCount == MaxKeys {
		if root.NodeType == NodeTypeRootLeaf {
			return tm.InsertNodeToWithoutChildRoot(root, value, location, header, indexName, recordListFileName)
		} else {
			root, header, err = tm.splitRoot(root, header, indexName)
			if err != nil {
				return fmt.Errorf("failed to split root: %v", err)
			}
		}
	}

	if root.NodeType == NodeTypeRootLeaf {
		return tm.InsertNodeToWithoutChildRoot(root, value, location, header, indexName, recordListFileName)
	} else {
		return tm.insertValueToNode(root, value, location, header, indexName, recordListFileName)
	}
}

func (tm *TableManager) InsertNodeToWithoutChildRoot(node Node, value int64, location RecordLocation, header IndexHeader, indexName string, recordListFileName string) error {
	idx := 0
	for i, v := range node.Values {
		if v.Value > value {
			idx = i
			break
		} else if v.Value == value {
			if _, err := tm.AddValueToRecordListFile(recordListFileName, v.RecordListHead, location); err != nil {
				return fmt.Errorf("failed to add to record list: %v", err)
			}
			return nil
		}
		idx = i + 1
	}

	recordListHead, err := tm.AddValueToRecordListFile(recordListFileName, 0, location)
	if err != nil {
		return fmt.Errorf("failed to create record list: %v", err)
	}

	valueEntry := ValueEntry{
		Value:          value,
		RecordListHead: recordListHead,
	}

	node.Values = append(
		node.Values[:idx],
		append([]ValueEntry{valueEntry}, node.Values[idx:]...)...,
	)

	node.KeyCount++

	if node.KeyCount > 5 {
		nodeBinaries := tm.SplitRootNode(node, int64(header.FreeSpacePointer))

		if err := tm.FileManager.Write(indexName, int64(header.FreeSpacePointer), nodeBinaries[0]); err != nil {
			return fmt.Errorf("failed to write left leaf: %w", err)
		}
		if err := tm.FileManager.Write(indexName, int64(header.FreeSpacePointer)+NodeSize, nodeBinaries[1]); err != nil {
			return fmt.Errorf("failed to write right leaf: %w", err)
		}
		if err := tm.FileManager.Write(indexName, int64(header.RootPointer), nodeBinaries[2]); err != nil {
			return fmt.Errorf("failed to write new root: %w", err)
		}

		header.NodeCount += 2
		header.Height++
		header.FreeSpacePointer += NodeSize * 2

		headerData := SerializeIndexHeader(header)
		if err := tm.FileManager.Write(indexName, 0, headerData); err != nil {
			return fmt.Errorf("failed to update index header: %w", err)
		}
		return nil
	} else {
		node_binary := SerializeIndexNode(node)
		return tm.FileManager.Write(indexName, node.Address, node_binary)
	}

}

func (tm *TableManager) insertValueToNode(currentNode Node, value int64, location RecordLocation, header IndexHeader, indexName string, recordListFileName string) error {
	if currentNode.NodeType == NodeTypeLeaf {
		return tm.insertToLeafNode(currentNode, value, location, header, indexName, recordListFileName)
	}

	childIdx := tm.findChildIndex(currentNode, value)

	childNode, err := tm.GetNode(indexName, int64(currentNode.ChildPointers[childIdx]))
	if err != nil {
		return fmt.Errorf("failed to get child node: %v", err)
	}

	childNode.ParentAddress = currentNode.Address

	if childNode.KeyCount == MaxKeys {
		currentNode, header, err = tm.splitNodeProactively(currentNode, childNode, childIdx, header, indexName)
		if err != nil {
			return fmt.Errorf("failed to split child proactively: %v", err)
		}

		childIdx = tm.findChildIndex(currentNode, value)

		childNode, err = tm.GetNode(indexName, int64(currentNode.ChildPointers[childIdx]))
		if err != nil {
			return fmt.Errorf("failed to get child node after split: %v", err)
		}
		childNode.ParentAddress = currentNode.Address
	}

	return tm.insertValueToNode(childNode, value, location, header, indexName, recordListFileName)
}

func (tm *TableManager) insertToLeafNode(currentNode Node, value int64, location RecordLocation, header IndexHeader, indexName string, recordListFileName string) error {
	idx := 0
	for i, v := range currentNode.Values {
		if v.Value > value {
			idx = i
			break
		} else if v.Value == value {
			if _, err := tm.AddValueToRecordListFile(recordListFileName, v.RecordListHead, location); err != nil {
				return fmt.Errorf("failed to add to record list: %v", err)
			}
			return nil
		}
		idx = i + 1
	}

	recordListHead, err := tm.AddValueToRecordListFile(recordListFileName, 0, location)
	if err != nil {
		return fmt.Errorf("failed to create record list: %v", err)
	}

	valueEntry := ValueEntry{
		Value:          value,
		RecordListHead: recordListHead,
	}

	currentNode.Values = append(
		currentNode.Values[:idx],
		append([]ValueEntry{valueEntry}, currentNode.Values[idx:]...)...,
	)

	currentNode.KeyCount++

	if currentNode.KeyCount > 5 {
		_, _, err := tm.SplitLeafNode(currentNode, currentNode, header, indexName)
		return err
	} else {
		node_binary := SerializeIndexNode(currentNode)
		return tm.FileManager.Write(indexName, currentNode.Address, node_binary)
	}
}

func (tm *TableManager) SplitRootNode(splittingNode Node, free_space_pointer int64) [][]byte {
	if splittingNode.NodeType == NodeTypeRootLeaf || splittingNode.NodeType == NodeTypeRootInternal {
		newRoot := Node{
			NodeType:      NodeTypeRootInternal,
			KeyCount:      1,
			ChildPointers: []uint64{uint64(free_space_pointer), uint64(free_space_pointer) + NodeSize},
			Values: []ValueEntry{
				{
					Value:          splittingNode.Values[2].Value,
					RecordListHead: splittingNode.Values[2].RecordListHead,
				},
			},
			Address: splittingNode.Address,
		}

		// Create left leaf
		leftLeaf := Node{
			NodeType: NodeTypeLeaf,
			KeyCount: 2,
			Values:   splittingNode.Values[:2],
			Address:  free_space_pointer,
		}

		// Create right leaf
		rightLeaf := Node{
			NodeType: NodeTypeLeaf,
			KeyCount: 3,
			Values:   splittingNode.Values[3:],
		}

		// Serialize all nodes
		newRootBinary := SerializeIndexNode(newRoot)
		leftLeafBinary := SerializeIndexNode(leftLeaf)
		rightLeafBinary := SerializeIndexNode(rightLeaf)

		// Pad all binaries to 8192 bytes
		binaries := make([][]byte, 0, 3)
		for _, bin := range [][]byte{leftLeafBinary, rightLeafBinary, newRootBinary} {
			padding := make([]byte, NodeSize-len(bin))
			binaries = append(binaries, append(bin, padding...))
		}

		return binaries
	}

	return nil
}

func (tm *TableManager) SplitLeafNode(fullNode Node, parentNode Node, header IndexHeader, indexName string) (Node, IndexHeader, error) {
	if fullNode.NodeType != NodeTypeLeaf {
		return Node{}, IndexHeader{}, errors.New("can only split leaf nodes")
	}

	middle := len(fullNode.Values) / 2
	if len(fullNode.Values)%2 == 0 {
		middle--
	}

	rightNode := Node{
		NodeType:      NodeTypeLeaf,
		KeyCount:      int8(len(fullNode.Values) - middle - 1),
		Values:        fullNode.Values[middle+1:],
		Address:       int64(header.FreeSpacePointer),
		ParentAddress: fullNode.ParentAddress,
	}

	leftNode := fullNode
	leftNode.Values = leftNode.Values[:middle]
	leftNode.KeyCount = int8(middle)

	middleValue := fullNode.Values[middle]

	leftNodeBinary := SerializeIndexNode(leftNode)
	if err := tm.FileManager.Write(indexName, leftNode.Address, padTo8KB(leftNodeBinary)); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to write left node: %v", err)
	}

	rightNodeBinary := SerializeIndexNode(rightNode)
	if err := tm.FileManager.Write(indexName, rightNode.Address, padTo8KB(rightNodeBinary)); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to write right node: %v", err)
	}

	insertPos := 0
	for i := 0; i < int(parentNode.KeyCount); i++ {
		if middleValue.Value < parentNode.Values[i].Value {
			break
		}
		insertPos++
	}

	parentNode.Values = append(parentNode.Values[:insertPos],
		append([]ValueEntry{middleValue}, parentNode.Values[insertPos:]...)...)

	if insertPos >= len(parentNode.ChildPointers) {
		parentNode.ChildPointers = append(parentNode.ChildPointers, uint64(rightNode.Address))
	} else {
		parentNode.ChildPointers = append(parentNode.ChildPointers[:insertPos+1],
			append([]uint64{uint64(rightNode.Address)}, parentNode.ChildPointers[insertPos+1:]...)...)
	}

	parentNode.KeyCount++

	parentBinary := SerializeIndexNode(parentNode)
	if err := tm.FileManager.Write(indexName, parentNode.Address, padTo8KB(parentBinary)); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to update parent node: %v", err)
	}

	header.NodeCount++
	header.FreeSpacePointer += NodeSize

	headerBinary := SerializeIndexHeader(header)
	if err := tm.FileManager.Write(indexName, 0, headerBinary); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to update header: %v", err)
	}

	return parentNode, header, nil
}

func (tm *TableManager) GetIndexHeader(fileName string) (IndexHeader, error) {
	indexHeaderBinary, err := tm.FileManager.Read(fileName, 0, IndexHeaderSize)
	if err != nil {
		return IndexHeader{}, err
	}

	indexHeader := DeserializeIndexHeader(indexHeaderBinary)

	return indexHeader, nil
}

func (tm *TableManager) GetNodeById(fileName string, node_id int64) (Node, error) {
	node_binary, err := tm.FileManager.Read(fileName, IndexHeaderSize+(node_id-1)*NodeSize, NodeSize)
	if err != nil {
		return Node{}, err
	}

	node := DeserializeIndexNode(node_binary)
	node.Address = node_id * NodeSize

	return node, nil
}

func (tm *TableManager) GetNode(fileName string, node_pointer int64) (Node, error) {
	node_binary, err := tm.FileManager.Read(fileName, node_pointer, NodeSize)
	if err != nil {
		return Node{}, err

	}

	node := DeserializeIndexNode(node_binary)
	node.Address = node_pointer

	return node, nil
}

func createEmptyRootNode() []byte {
	node := Node{
		KeyCount: 0,
		NodeType: NodeTypeRootLeaf,
	}
	data := SerializeIndexNode(node)

	padding := make([]byte, NodeSize-len(data))
	data = append(data, padding...)

	return data
}

func padTo8KB(bin []byte) []byte {
	padding := make([]byte, NodeSize-len(bin))
	return append(bin, padding...)
}

func (tm *TableManager) SplitExternalNode(fullNode Node, parentNode Node, header IndexHeader, indexName string) (Node, IndexHeader, error) {
	if fullNode.NodeType == NodeTypeLeaf {
		return Node{}, IndexHeader{}, errors.New("use SplitLeafNode for leaf nodes")
	}

	middle := len(fullNode.Values) / 2

	middleValue := fullNode.Values[middle]

	rightNode := Node{
		NodeType:      NodeTypeInternal,
		KeyCount:      int8(len(fullNode.Values) - middle - 1),
		Values:        fullNode.Values[middle+1:],
		ChildPointers: fullNode.ChildPointers[middle+1:],
		Address:       int64(header.FreeSpacePointer),
		ParentAddress: fullNode.ParentAddress,
	}

	leftNode := fullNode
	leftNode.Values = leftNode.Values[:middle]
	leftNode.ChildPointers = leftNode.ChildPointers[:middle+1]
	leftNode.KeyCount = int8(middle)

	leftNodeBinary := SerializeIndexNode(leftNode)
	if err := tm.FileManager.Write(indexName, leftNode.Address, padTo8KB(leftNodeBinary)); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to write left node: %v", err)
	}

	rightNodeBinary := SerializeIndexNode(rightNode)
	if err := tm.FileManager.Write(indexName, rightNode.Address, padTo8KB(rightNodeBinary)); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to write right node: %v", err)
	}

	insertPos := 0
	for i := 0; i < int(parentNode.KeyCount); i++ {
		if middleValue.Value < parentNode.Values[i].Value {
			break
		}
		insertPos++
	}

	parentNode.Values = append(parentNode.Values[:insertPos],
		append([]ValueEntry{middleValue}, parentNode.Values[insertPos:]...)...)

	parentNode.ChildPointers = append(parentNode.ChildPointers[:insertPos+1],
		append([]uint64{uint64(rightNode.Address)}, parentNode.ChildPointers[insertPos+1:]...)...)

	parentNode.KeyCount++

	parentBinary := SerializeIndexNode(parentNode)
	if err := tm.FileManager.Write(indexName, parentNode.Address, padTo8KB(parentBinary)); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to update parent node: %v", err)
	}

	header.NodeCount++
	header.FreeSpacePointer += NodeSize

	headerBinary := SerializeIndexHeader(header)
	if err := tm.FileManager.Write(indexName, 0, headerBinary); err != nil {
		return Node{}, IndexHeader{}, fmt.Errorf("failed to update header: %v", err)
	}

	return parentNode, header, nil
}

func (tm *TableManager) findChildIndex(node Node, value int64) int {
	idx := 0
	for i, v := range node.Values {
		if v.Value > value {
			return i
		}
		idx = i + 1
	}
	return idx
}

func (tm *TableManager) splitNodeProactively(parentNode Node, fullChildNode Node, childIdx int, header IndexHeader, indexName string) (Node, IndexHeader, error) {
	if fullChildNode.NodeType == NodeTypeLeaf {
		return tm.SplitLeafNode(fullChildNode, parentNode, header, indexName)
	} else {
		return tm.SplitExternalNode(fullChildNode, parentNode, header, indexName)
	}
}

func (tm *TableManager) splitRoot(root Node, header IndexHeader, indexName string) (Node, IndexHeader, error) {
	middle := len(root.Values) / 2
	middleValue := root.Values[middle]

	leftChild := Node{
		NodeType:      NodeTypeInternal,
		KeyCount:      int8(middle),
		Values:        root.Values[:middle],
		ChildPointers: root.ChildPointers[:middle+1],
		Address:       int64(header.FreeSpacePointer),
	}

	rightChild := Node{
		NodeType:      NodeTypeInternal,
		KeyCount:      int8(len(root.Values) - middle - 1),
		Values:        root.Values[middle+1:],
		ChildPointers: root.ChildPointers[middle+1:],
		Address:       int64(header.FreeSpacePointer) + NodeSize,
	}

	newRoot := Node{
		NodeType:      NodeTypeRootInternal,
		KeyCount:      1,
		Values:        []ValueEntry{middleValue},
		ChildPointers: []uint64{uint64(leftChild.Address), uint64(rightChild.Address)},
		Address:       root.Address,
	}

	leftBinary := SerializeIndexNode(leftChild)
	if err := tm.FileManager.Write(indexName, leftChild.Address, padTo8KB(leftBinary)); err != nil {
		return Node{}, IndexHeader{}, err
	}

	rightBinary := SerializeIndexNode(rightChild)
	if err := tm.FileManager.Write(indexName, rightChild.Address, padTo8KB(rightBinary)); err != nil {
		return Node{}, IndexHeader{}, err
	}

	rootBinary := SerializeIndexNode(newRoot)
	if err := tm.FileManager.Write(indexName, newRoot.Address, padTo8KB(rootBinary)); err != nil {
		return Node{}, IndexHeader{}, err
	}

	header.NodeCount += 2
	header.FreeSpacePointer += NodeSize * 2
	header.Height++

	headerBinary := SerializeIndexHeader(header)
	if err := tm.FileManager.Write(indexName, 0, headerBinary); err != nil {
		return Node{}, IndexHeader{}, err
	}

	return newRoot, header, nil
}

func (tm *TableManager) CreateRecordListFile(tableName string, columnName string) error {
	recordListFileName := tableName + "_" + columnName + ".record_list_file"

	if tm.FileManager.FileExists(recordListFileName) {
		return errors.New("record list file already exists")
	}

	_, err := tm.FileManager.CreateFile(recordListFileName)
	if err != nil {
		return err
	}

	header := RecordListFileHeader{
		FreeSpacePointer: RecordListHeaderSize,
		BlockCount:       0,
	}

	headerBinary := SerializeRecordListFileHeader(header)

	return tm.FileManager.Write(recordListFileName, 0, headerBinary)
}

func (tm *TableManager) GetRecordListFileHeader(fileName string) (RecordListFileHeader, error) {
	headerBinary, err := tm.FileManager.Read(fileName, 0, RecordListHeaderSize)

	if err != nil {
		return RecordListFileHeader{}, err
	}

	header := DeserializeRecordListFileHeader(headerBinary)

	return header, nil
}

func (tm *TableManager) AddValueToRecordListFile(fileName string, recordListHead int64, location RecordLocation) (int64, error) {
	header, err := tm.GetRecordListFileHeader(fileName)
	if err != nil {
		return 0, fmt.Errorf("failed to get record list header: %v", err)
	}

	if recordListHead == 0 {
		newBlock := RecordListBlock{
			Locations: []RecordLocation{location},
			Count:     1,
			NextBlock: 0,
		}

		blockBinary := SerializeRecordListBlock(newBlock)
		blockAddress := header.FreeSpacePointer

		if err := tm.FileManager.Write(fileName, blockAddress, blockBinary); err != nil {
			return 0, fmt.Errorf("failed to write new block: %v", err)
		}

		header.FreeSpacePointer += RecordListBlockSize
		header.BlockCount++

		headerBinary := SerializeRecordListFileHeader(header)
		if err := tm.FileManager.Write(fileName, 0, headerBinary); err != nil {
			return 0, fmt.Errorf("failed to update header: %v", err)
		}

		return blockAddress, nil
	}

	currentBlockAddress := recordListHead
	var currentBlock RecordListBlock

	for {
		blockBinary, err := tm.FileManager.Read(fileName, currentBlockAddress, RecordListBlockSize)
		if err != nil {
			return 0, fmt.Errorf("failed to read block at %d: %v", currentBlockAddress, err)
		}

		currentBlock = DeserializeRecordListBlock(blockBinary)

		if currentBlock.Count < 10 {
			currentBlock.Locations = append(currentBlock.Locations, location)
			currentBlock.Count++

			blockBinary := SerializeRecordListBlock(currentBlock)
			if err := tm.FileManager.Write(fileName, currentBlockAddress, blockBinary); err != nil {
				return 0, fmt.Errorf("failed to update block: %v", err)
			}

			return recordListHead, nil
		}

		if currentBlock.NextBlock == 0 {
			newBlock := RecordListBlock{
				Locations: []RecordLocation{location},
				Count:     1,
				NextBlock: 0,
			}

			newBlockAddress := header.FreeSpacePointer
			newBlockBinary := SerializeRecordListBlock(newBlock)

			if err := tm.FileManager.Write(fileName, newBlockAddress, newBlockBinary); err != nil {
				return 0, fmt.Errorf("failed to write new block: %v", err)
			}

			currentBlock.NextBlock = newBlockAddress
			currentBlockBinary := SerializeRecordListBlock(currentBlock)
			if err := tm.FileManager.Write(fileName, currentBlockAddress, currentBlockBinary); err != nil {
				return 0, fmt.Errorf("failed to update current block: %v", err)
			}

			header.FreeSpacePointer += RecordListBlockSize
			header.BlockCount++

			headerBinary := SerializeRecordListFileHeader(header)
			if err := tm.FileManager.Write(fileName, 0, headerBinary); err != nil {
				return 0, fmt.Errorf("failed to update header: %v", err)
			}

			return recordListHead, nil
		}

		currentBlockAddress = currentBlock.NextBlock
	}
}

func (tm *TableManager) SearchValue(indexName string, value int64) (int64, bool, error) {
	header, err := tm.GetIndexHeader(indexName)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get index header: %v", err)
	}

	currentNode, err := tm.GetNode(indexName, header.RootPointer)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get root node: %v", err)
	}

	for {
		for i, v := range currentNode.Values {
			if v.Value == value {
				return v.RecordListHead, true, nil
			}
			if v.Value > value {
				if currentNode.NodeType == NodeTypeLeaf || currentNode.NodeType == NodeTypeRootLeaf {
					return 0, false, nil
				}
				currentNode, err = tm.GetNode(indexName, int64(currentNode.ChildPointers[i]))
				if err != nil {
					return 0, false, fmt.Errorf("failed to get child node: %v", err)
				}
				break
			}
			if i == len(currentNode.Values)-1 {
				if currentNode.NodeType == NodeTypeLeaf || currentNode.NodeType == NodeTypeRootLeaf {
					return 0, false, nil
				}
				currentNode, err = tm.GetNode(indexName, int64(currentNode.ChildPointers[i+1]))
				if err != nil {
					return 0, false, fmt.Errorf("failed to get child node: %v", err)
				}
				break
			}
		}

		if len(currentNode.Values) == 0 {
			return 0, false, nil
		}
	}
}

func (tm *TableManager) GetRecordsByValue(fileName string, recordListHead int64) ([]RecordLocation, error) {
	if recordListHead == 0 {
		return []RecordLocation{}, nil
	}

	var allLocations []RecordLocation
	currentBlockAddress := recordListHead

	for {
		blockBinary, err := tm.FileManager.Read(fileName, currentBlockAddress, RecordListBlockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to read block at %d: %v", currentBlockAddress, err)
		}

		currentBlock := DeserializeRecordListBlock(blockBinary)

		for i := 0; i < int(currentBlock.Count); i++ {
			allLocations = append(allLocations, currentBlock.Locations[i])
		}

		if currentBlock.NextBlock == 0 {
			break
		}

		currentBlockAddress = currentBlock.NextBlock
	}

	return allLocations, nil
}
