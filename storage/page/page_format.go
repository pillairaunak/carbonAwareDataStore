package page

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

const (
	MaxPageSize = 4096

	LeafNodeType     byte = 1
	InternalNodeType byte = 2

	NodeTypeOffset = 0
	NumKeysOffset  = 2
	HeaderSize     = 8

	NextLeafOffset = HeaderSize
	LeafHeaderSize = HeaderSize + 8

	MaxLeafKeys     = (MaxPageSize - LeafHeaderSize) / 16 // 255
	MaxInternalKeys = (MaxPageSize - HeaderSize) / 16     // 255

	MinLeafKeys     = MaxLeafKeys / 2     // 255 / 2 = 127
	MinInternalKeys = MaxInternalKeys / 2 // 255 / 2 = 127
)

type NodeHeader struct {
	Type      byte
	_padding  byte
	NumKeys   uint16
	_reserved uint32
}

type LeafNode struct {
	Header   NodeHeader
	NextLeaf uint64
	Keys     [MaxLeafKeys]uint64
	Values   [MaxLeafKeys]uint64
}

type InternalNode struct {
	Header   NodeHeader
	Keys     [MaxInternalKeys]uint64
	Children [MaxInternalKeys + 1]uint64
}

func GetNodeType(page []byte) byte {
	if len(page) < 1 {
		panic("Page buffer too small to determine node type")
	}
	return page[NodeTypeOffset]
}

func GetNumKeys(page []byte) uint16 {
	if len(page) < NumKeysOffset+2 {
		panic("Page buffer too small to read number of keys")
	}
	return binary.LittleEndian.Uint16(page[NumKeysOffset : NumKeysOffset+2])
}

func SetNumKeys(page []byte, count uint16) {
	if len(page) < NumKeysOffset+2 {
		panic("Page buffer too small to write number of keys")
	}
	binary.LittleEndian.PutUint16(page[NumKeysOffset:NumKeysOffset+2], count)
}

func PageToLeafNode(page []byte) *LeafNode {
	if len(page) < MaxPageSize {
		panic(fmt.Sprintf("Page buffer too small for leaf node: %d < %d", len(page), MaxPageSize))
	}
	return (*LeafNode)(unsafe.Pointer(&page[0]))
}

func PageToInternalNode(page []byte) *InternalNode {
	if len(page) < MaxPageSize {
		panic(fmt.Sprintf("Page buffer too small for internal node: %d < %d", len(page), MaxPageSize))
	}
	return (*InternalNode)(unsafe.Pointer(&page[0]))
}

func InitializeLeafNode(page []byte) error {
	if len(page) < MaxPageSize {
		return fmt.Errorf("page buffer too small: %d < %d", len(page), MaxPageSize)
	}
	for i := range page {
		page[i] = 0
	}
	page[NodeTypeOffset] = LeafNodeType
	SetNumKeys(page, 0)
	return nil
}

func InitializeInternalNode(page []byte) error {
	if len(page) < MaxPageSize {
		return fmt.Errorf("page buffer too small: %d < %d", len(page), MaxPageSize)
	}
	for i := range page {
		page[i] = 0
	}
	page[NodeTypeOffset] = InternalNodeType
	SetNumKeys(page, 0)
	return nil
}

func LeafSearch(node *LeafNode, key uint64) (index int, found bool) {
	numKeys := int(node.Header.NumKeys)
	low, high := 0, numKeys-1
	for low <= high {
		mid := low + (high-low)/2
		midKey := node.Keys[mid]
		if midKey < key {
			low = mid + 1
		} else if midKey > key {
			high = mid - 1
		} else {
			return mid, true
		}
	}
	return low, false
}

func InternalSearch(node *InternalNode, key uint64) int {
	numKeys := int(node.Header.NumKeys)
	low, high := 0, numKeys-1
	resultIndex := numKeys
	for low <= high {
		mid := low + (high-low)/2
		if node.Keys[mid] > key {
			resultIndex = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return resultIndex
}

func GetNextLeafPageID(node *LeafNode) uint64         { return node.NextLeaf }
func SetNextLeafPageID(node *LeafNode, pageID uint64) { node.NextLeaf = pageID }
