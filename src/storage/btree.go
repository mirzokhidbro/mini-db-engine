package storage

import (
	"errors"
	"fmt"
)

type NodeType int8

const (
	NodeTypeRootWithChild NodeType = iota
	NodeTypeRootNoChild
	NodeTypeExternal
	NodeTypeLeaf
)

const MaxKeys = 5

type NodeHeader struct {
	RootPointer      uint64
	FreeSpacePointer uint64
	NodeCount        uint64
	Height           uint64
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
	Value             int64
	RecordListPointer int64
}

func (tm *TableManager) CreateIndex(tableName string, columnName string) error {
	indexFileName := tableName + "_" + columnName + ".index"
	if tm.FileManager.FileExists(indexFileName) {
		return errors.New("index already exists")
	}

	_, err := tm.FileManager.CreateFile(indexFileName)

	node_header := NodeHeader{
		RootPointer:      32,
		FreeSpacePointer: 32 + 8192,
		NodeCount:        1,
		Height:           1,
	}

	node_header_binary := SerializeIndexHeader(node_header)

	root_binary := createEmptyNode()

	binary_data := append(node_header_binary, root_binary...)

	tm.FileManager.Write(indexFileName, 0, binary_data)

	if err != nil {
		return err
	}

	return nil
}

func (tm *TableManager) InsetValueToIndex(indexName string, valueEntry ValueEntry) error {
	header, err := tm.GetIndexHeader(indexName)
	if err != nil {
		return fmt.Errorf("failed to get index header: %v", err)
	}

	root, err := tm.GetNode(indexName, int64(header.RootPointer))
	if err != nil {
		return fmt.Errorf("failed to get root node: %v", err)
	}

	if root.KeyCount == MaxKeys {
		if root.NodeType == NodeTypeRootNoChild {
			return tm.InsertNodeToWithoutChildRoot(root, valueEntry, header, indexName)
		} else {
			if err := tm.splitRoot(&root, &header, indexName); err != nil {
				return fmt.Errorf("failed to split root: %v", err)
			}
			root, err = tm.GetNode(indexName, int64(header.RootPointer))
			if err != nil {
				return fmt.Errorf("failed to get new root: %v", err)
			}
		}
	}

	if root.NodeType == NodeTypeRootNoChild {
		return tm.InsertNodeToWithoutChildRoot(root, valueEntry, header, indexName)
	} else {
		return tm.insertValueToNode(root, valueEntry, header, indexName)
	}
}

func (tm *TableManager) InsertNodeToWithoutChildRoot(node Node, valueEntry ValueEntry, header NodeHeader, indexName string) error {
	idx := 0
	for i, v := range node.Values {
		if v.Value > valueEntry.Value {
			idx = i
			break
		} else if v.Value == valueEntry.Value {
			fmt.Println("there is the same value with new value")
			return nil
		}
		idx = i + 1
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
		if err := tm.FileManager.Write(indexName, int64(header.FreeSpacePointer)+8192, nodeBinaries[1]); err != nil {
			return fmt.Errorf("failed to write right leaf: %w", err)
		}
		if err := tm.FileManager.Write(indexName, int64(header.RootPointer), nodeBinaries[2]); err != nil {
			return fmt.Errorf("failed to write new root: %w", err)
		}

		header.NodeCount += 2
		header.Height++
		header.FreeSpacePointer += 8192 * 2

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

func (tm *TableManager) insertValueToNode(currentNode Node, valueEntry ValueEntry, header NodeHeader, indexName string) error {
	if currentNode.NodeType == NodeTypeLeaf {
		return tm.insertToLeafNode(currentNode, valueEntry, header, indexName)
	}

	childIdx := tm.findChildIndex(currentNode, valueEntry.Value)

	childNode, err := tm.GetNode(indexName, int64(currentNode.ChildPointers[childIdx]))
	if err != nil {
		return fmt.Errorf("failed to get child node: %v", err)
	}

	childNode.ParentAddress = currentNode.Address

	if childNode.KeyCount == MaxKeys {
		if err := tm.splitNodeProactively(&currentNode, &childNode, childIdx, &header, indexName); err != nil {
			return fmt.Errorf("failed to split child proactively: %v", err)
		}

		childIdx = tm.findChildIndex(currentNode, valueEntry.Value)

		childNode, err = tm.GetNode(indexName, int64(currentNode.ChildPointers[childIdx]))
		if err != nil {
			return fmt.Errorf("failed to get child node after split: %v", err)
		}
		childNode.ParentAddress = currentNode.Address
	}

	return tm.insertValueToNode(childNode, valueEntry, header, indexName)
}

func (tm *TableManager) SplitRootNode(splittingNode Node, free_space_pointer int64) [][]byte {
	if splittingNode.NodeType == NodeTypeRootNoChild || splittingNode.NodeType == NodeTypeRootWithChild {
		newRoot := Node{
			NodeType:      NodeTypeRootWithChild,
			KeyCount:      1,
			ChildPointers: []uint64{uint64(free_space_pointer), uint64(free_space_pointer) + 8192},
			Values: []ValueEntry{
				{
					Value:             splittingNode.Values[2].Value,
					RecordListPointer: splittingNode.Values[2].RecordListPointer,
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
			padding := make([]byte, 8192-len(bin))
			binaries = append(binaries, append(bin, padding...))
		}

		return binaries
	}

	return nil
}

func (tm *TableManager) SplitLeafNode(fullNode Node, parentNode *Node, header NodeHeader, indexName string) error {
	if fullNode.NodeType != NodeTypeLeaf {
		return errors.New("can only split leaf nodes")
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
		return fmt.Errorf("failed to write left node: %v", err)
	}

	rightNodeBinary := SerializeIndexNode(rightNode)
	if err := tm.FileManager.Write(indexName, rightNode.Address, padTo8KB(rightNodeBinary)); err != nil {
		return fmt.Errorf("failed to write right node: %v", err)
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

	parentBinary := SerializeIndexNode(*parentNode)
	if err := tm.FileManager.Write(indexName, parentNode.Address, padTo8KB(parentBinary)); err != nil {
		return fmt.Errorf("failed to update parent node: %v", err)
	}

	header.NodeCount++
	header.FreeSpacePointer += 8192

	headerBinary := SerializeIndexHeader(header)
	if err := tm.FileManager.Write(indexName, 0, headerBinary); err != nil {
		return fmt.Errorf("failed to update header: %v", err)
	}

	return nil
}

func (tm *TableManager) GetIndexHeader(fileName string) (NodeHeader, error) {
	node_header_binary, err := tm.FileManager.Read(fileName, 0, 32)
	if err != nil {
		return NodeHeader{}, err
	}

	node_header := DeserializeIndexHeader(node_header_binary)

	return node_header, nil
}

func (tm *TableManager) GetNodeById(fileName string, node_id int64) (Node, error) {
	node_binary, err := tm.FileManager.Read(fileName, 32+(node_id-1)*8192, 8192)
	if err != nil {
		return Node{}, err
	}

	node := DeserializeIndexNode(node_binary)
	node.Address = node_id * 8192

	return node, nil
}

func (tm *TableManager) GetNode(fileName string, node_pointer int64) (Node, error) {
	node_binary, err := tm.FileManager.Read(fileName, node_pointer, 8192)
	if err != nil {
		return Node{}, err

	}

	node := DeserializeIndexNode(node_binary)
	node.Address = node_pointer

	return node, nil
}

func createEmptyNode() []byte {
	node := Node{
		KeyCount: 0,
		NodeType: NodeTypeRootNoChild,
	}
	data := SerializeIndexNode(node)

	padding := make([]byte, 8192-len(data))
	data = append(data, padding...)

	return data
}

func padTo8KB(bin []byte) []byte {
	padding := make([]byte, 8192-len(bin))
	return append(bin, padding...)
}

func (tm *TableManager) SplitExternalNode(fullNode Node, parentNode *Node, header *NodeHeader, indexName string) error {
	if fullNode.NodeType == NodeTypeLeaf {
		return errors.New("use SplitLeafNode for leaf nodes")
	}

	middle := len(fullNode.Values) / 2

	middleValue := fullNode.Values[middle]

	rightNode := Node{
		NodeType:      NodeTypeExternal,
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
		return fmt.Errorf("failed to write left node: %v", err)
	}

	rightNodeBinary := SerializeIndexNode(rightNode)
	if err := tm.FileManager.Write(indexName, rightNode.Address, padTo8KB(rightNodeBinary)); err != nil {
		return fmt.Errorf("failed to write right node: %v", err)
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

	parentBinary := SerializeIndexNode(*parentNode)
	if err := tm.FileManager.Write(indexName, parentNode.Address, padTo8KB(parentBinary)); err != nil {
		return fmt.Errorf("failed to update parent node: %v", err)
	}

	header.NodeCount++
	header.FreeSpacePointer += 8192

	headerBinary := SerializeIndexHeader(*header)
	if err := tm.FileManager.Write(indexName, 0, headerBinary); err != nil {
		return fmt.Errorf("failed to update header: %v", err)
	}

	return nil
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

func (tm *TableManager) insertToLeafNode(leafNode Node, valueEntry ValueEntry, header NodeHeader, indexName string) error {
	idx := 0
	for i, v := range leafNode.Values {
		if v.Value > valueEntry.Value {
			idx = i
			break
		} else if v.Value == valueEntry.Value {
			return nil
		}
		idx = i + 1
	}

	leafNode.Values = append(
		leafNode.Values[:idx],
		append([]ValueEntry{valueEntry}, leafNode.Values[idx:]...)...,
	)
	leafNode.KeyCount++

	// important section
	nodeBinary := SerializeIndexNode(leafNode)
	return tm.FileManager.Write(indexName, leafNode.Address, padTo8KB(nodeBinary))
}

func (tm *TableManager) splitNodeProactively(parentNode *Node, fullChildNode *Node, childIdx int, header *NodeHeader, indexName string) error {
	if fullChildNode.NodeType == NodeTypeLeaf {
		return tm.SplitLeafNode(*fullChildNode, parentNode, *header, indexName)
	} else {
		return tm.SplitExternalNode(*fullChildNode, parentNode, header, indexName)
	}
}

func (tm *TableManager) splitRoot(root *Node, header *NodeHeader, indexName string) error {
	middle := len(root.Values) / 2
	middleValue := root.Values[middle]

	leftChild := Node{
		NodeType:      NodeTypeExternal,
		KeyCount:      int8(middle),
		Values:        root.Values[:middle],
		ChildPointers: root.ChildPointers[:middle+1],
		Address:       int64(header.FreeSpacePointer),
	}

	rightChild := Node{
		NodeType:      NodeTypeExternal,
		KeyCount:      int8(len(root.Values) - middle - 1),
		Values:        root.Values[middle+1:],
		ChildPointers: root.ChildPointers[middle+1:],
		Address:       int64(header.FreeSpacePointer) + 8192,
	}

	newRoot := Node{
		NodeType:      NodeTypeRootWithChild,
		KeyCount:      1,
		Values:        []ValueEntry{middleValue},
		ChildPointers: []uint64{uint64(leftChild.Address), uint64(rightChild.Address)},
		Address:       root.Address,
	}

	leftBinary := SerializeIndexNode(leftChild)
	if err := tm.FileManager.Write(indexName, leftChild.Address, padTo8KB(leftBinary)); err != nil {
		return err
	}

	rightBinary := SerializeIndexNode(rightChild)
	if err := tm.FileManager.Write(indexName, rightChild.Address, padTo8KB(rightBinary)); err != nil {
		return err
	}

	rootBinary := SerializeIndexNode(newRoot)
	if err := tm.FileManager.Write(indexName, newRoot.Address, padTo8KB(rootBinary)); err != nil {
		return err
	}

	header.NodeCount += 2
	header.FreeSpacePointer += 8192 * 2
	header.Height++

	headerBinary := SerializeIndexHeader(*header)
	if err := tm.FileManager.Write(indexName, 0, headerBinary); err != nil {
		return err
	}

	*root = newRoot
	return nil
}
