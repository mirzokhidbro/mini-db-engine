package index

import (
	"rdbms/src/sort"
)

// BTree constants
const (
	NodeSize = 8192 // 8KB per node
	MaxKeys  = 128  // Maximum keys per node
	MinKeys  = 64   // Minimum keys per node (MaxKeys/2)
)

// NodeType represents the type of B-tree node
type NodeType uint8

const (
	LeafNode NodeType = iota
	InternalNode
)

// BTreeNode represents a single node in the B-tree
type BTreeNode struct {
	NodeType NodeType
	KeyCount uint16
	Keys     []interface{} // Actual values (int64 or float64)
	Values   []RowPointer  // For leaf nodes: points to table records
	Children []uint64      // For internal nodes: points to child nodes
	NextLeaf uint64        // For leaf nodes: pointer to next leaf (for range queries)
}

// RowPointer points to a record in the table
type RowPointer struct {
	PageNumber uint64
	RecordSlot uint16
}

// BTree represents the entire B-tree index
type BTree struct {
	RootPosition uint64
	NodeCount    uint64
	IsInt        bool
	Nodes        []*BTreeNode // In-memory representation
}

// BuildBTree builds a B-tree from sorted entries
func BuildBTree(entries []sort.IndexEntry, isInt bool) *BTree {
	if len(entries) == 0 {
		return &BTree{
			RootPosition: 0,
			NodeCount:    0,
			IsInt:        isInt,
			Nodes:        []*BTreeNode{},
		}
	}

	// Build leaf nodes from sorted entries
	leafNodes := buildLeafNodes(entries)

	// If only one leaf node, it's the root
	if len(leafNodes) == 1 {
		return &BTree{
			RootPosition: 0,
			NodeCount:    1,
			IsInt:        isInt,
			Nodes:        leafNodes,
		}
	}

	// Build internal nodes bottom-up
	currentLevel := leafNodes
	allNodes := make([]*BTreeNode, 0)
	allNodes = append(allNodes, leafNodes...)

	for len(currentLevel) > 1 {
		nextLevel := buildInternalLevel(currentLevel, len(allNodes))
		allNodes = append(allNodes, nextLevel...)
		currentLevel = nextLevel
	}

	// The last node in currentLevel is the root
	rootPosition := uint64(len(allNodes) - 1)

	return &BTree{
		RootPosition: rootPosition,
		NodeCount:    uint64(len(allNodes)),
		IsInt:        isInt,
		Nodes:        allNodes,
	}
}

// buildLeafNodes creates leaf nodes from sorted entries
func buildLeafNodes(entries []sort.IndexEntry) []*BTreeNode {
	leafNodes := make([]*BTreeNode, 0)
	i := 0

	for i < len(entries) {
		// Determine how many entries to put in this leaf
		count := MaxKeys
		if i+count > len(entries) {
			count = len(entries) - i
		}

		// Create leaf node
		node := &BTreeNode{
			NodeType: LeafNode,
			KeyCount: uint16(count),
			Keys:     make([]interface{}, count),
			Values:   make([]RowPointer, count),
			Children: nil,
			NextLeaf: 0, // Will be set later
		}

		// Fill the node
		for j := 0; j < count; j++ {
			node.Keys[j] = entries[i+j].Value
			node.Values[j] = RowPointer{
				PageNumber: entries[i+j].PageNumber,
				RecordSlot: entries[i+j].RecordSlot,
			}
		}

		leafNodes = append(leafNodes, node)
		i += count
	}

	// Link leaf nodes together
	for i := 0; i < len(leafNodes)-1; i++ {
		leafNodes[i].NextLeaf = uint64(i + 1)
	}

	return leafNodes
}

// buildInternalLevel creates one level of internal nodes
func buildInternalLevel(childNodes []*BTreeNode, baseOffset int) []*BTreeNode {
	internalNodes := make([]*BTreeNode, 0)
	i := 0

	for i < len(childNodes) {
		// Determine how many children for this internal node
		count := MaxKeys + 1 // Internal nodes can have MaxKeys+1 children
		if i+count > len(childNodes) {
			count = len(childNodes) - i
		}

		// Create internal node
		// Keys are the first key of each child (except the first child)
		keyCount := count - 1
		node := &BTreeNode{
			NodeType: InternalNode,
			KeyCount: uint16(keyCount),
			Keys:     make([]interface{}, keyCount),
			Values:   nil,
			Children: make([]uint64, count),
			NextLeaf: 0,
		}

		// Fill children pointers
		for j := 0; j < count; j++ {
			node.Children[j] = uint64(baseOffset + i + j)
		}

		// Fill keys (first key of each child except first)
		for j := 0; j < keyCount; j++ {
			node.Keys[j] = childNodes[i+j+1].Keys[0]
		}

		internalNodes = append(internalNodes, node)
		i += count
	}

	return internalNodes
}
