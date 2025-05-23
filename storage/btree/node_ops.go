package btree

import (
	"fmt"
	"minibtreestore/storage/buffer"
	"minibtreestore/storage/page"
)

type splitInfo struct {
	key     uint64
	rightID uint64
}

func (t *BTreeImpl) createNewRoot(promotedKey uint64, leftChildID, rightChildID uint64) error {
	newRootPageID, err := t.bm.AllocatePage(t.btreeID)
	if err != nil {
		return fmt.Errorf("alloc new root: %w", err)
	}
	newRootID := uint64(newRootPageID)
	rootPg, err := t.bm.PinPage(t.btreeID, newRootPageID)
	if err != nil {
		_ = t.bm.FreePage(t.btreeID, newRootPageID)
		return fmt.Errorf("pin new root %d: %w", newRootID, err)
	}
	rootPageData := rootPg.Data
	if initErr := page.InitializeInternalNode(rootPageData); initErr != nil {
		_ = t.bm.UnpinPage(t.btreeID, newRootPageID, false)
		_ = t.bm.FreePage(t.btreeID, newRootPageID)
		return fmt.Errorf("init new root: %w", initErr)
	}
	rootNode := page.PageToInternalNode(rootPageData)
	rootNode.Header.NumKeys = 1
	page.SetNumKeys(rootPageData, 1)
	rootNode.Keys[0] = promotedKey
	rootNode.Children[0] = leftChildID
	rootNode.Children[1] = rightChildID
	unpinErr := t.bm.UnpinPage(t.btreeID, newRootPageID, true)
	if unpinErr != nil {
		return fmt.Errorf("unpin new root: %w", unpinErr)
	}
	t.rootID = newRootID
	return nil
}

func (t *BTreeImpl) splitLeafNode(leftNodeID uint64, leftPg buffer.Page, insertKey, insertValue uint64) (*splitInfo, error) {
	leftNode := page.PageToLeafNode(leftPg.Data)
	rightNodePageID, err := t.bm.AllocatePage(t.btreeID)
	if err != nil {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), false)
		return nil, fmt.Errorf("splitLeaf alloc: %w", err)
	}
	rightNodeID := uint64(rightNodePageID)
	rightPg, err := t.bm.PinPage(t.btreeID, rightNodePageID)
	if err != nil {
		_ = t.bm.FreePage(t.btreeID, rightNodePageID)
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), false)
		return nil, fmt.Errorf("splitLeaf pin right %d: %w", rightNodeID, err)
	}
	rightPageData := rightPg.Data
	if initErr := page.InitializeLeafNode(rightPageData); initErr != nil {
		_ = t.bm.UnpinPage(t.btreeID, rightNodePageID, false)
		_ = t.bm.FreePage(t.btreeID, rightNodePageID)
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), false)
		return nil, fmt.Errorf("splitLeaf init right %d: %w", rightNodeID, initErr)
	}
	rightNode := page.PageToLeafNode(rightPageData)
	numKeys := int(leftNode.Header.NumKeys)
	totalEntries := numKeys + 1
	tempKeys := make([]uint64, totalEntries)
	tempValues := make([]uint64, totalEntries)
	insertPos := 0
	for insertPos < numKeys && leftNode.Keys[insertPos] < insertKey {
		insertPos++
	}
	copy(tempKeys[:insertPos], leftNode.Keys[:insertPos])
	copy(tempValues[:insertPos], leftNode.Values[:insertPos])
	tempKeys[insertPos] = insertKey
	tempValues[insertPos] = insertValue
	copy(tempKeys[insertPos+1:], leftNode.Keys[insertPos:])
	copy(tempValues[insertPos+1:], leftNode.Values[insertPos:])
	splitPoint := (totalEntries + 1) / 2
	leftNode.Header.NumKeys = uint16(splitPoint)
	page.SetNumKeys(leftPg.Data, leftNode.Header.NumKeys)
	copy(leftNode.Keys[:splitPoint], tempKeys[:splitPoint])
	copy(leftNode.Values[:splitPoint], tempValues[:splitPoint])
	for i := splitPoint; i < page.MaxLeafKeys; i++ {
		leftNode.Keys[i] = 0
		leftNode.Values[i] = 0
	}
	rightNode.Header.NumKeys = uint16(totalEntries - splitPoint)
	page.SetNumKeys(rightPageData, rightNode.Header.NumKeys)
	copy(rightNode.Keys[:rightNode.Header.NumKeys], tempKeys[splitPoint:])
	copy(rightNode.Values[:rightNode.Header.NumKeys], tempValues[splitPoint:])
	rightNode.NextLeaf = leftNode.NextLeaf
	leftNode.NextLeaf = rightNodeID
	page.SetNextLeafPageID(leftNode, rightNodeID)
	page.SetNextLeafPageID(rightNode, rightNode.NextLeaf)
	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), true)
	_ = t.bm.UnpinPage(t.btreeID, rightNodePageID, true)
	return &splitInfo{key: rightNode.Keys[0], rightID: rightNodeID}, nil
}

func (t *BTreeImpl) splitInternalNode(leftNodeID uint64, leftPg buffer.Page, insertKey uint64, insertChildID uint64) (*splitInfo, error) {
	leftNode := page.PageToInternalNode(leftPg.Data)
	rightNodePageID, err := t.bm.AllocatePage(t.btreeID)
	if err != nil {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), false)
		return nil, fmt.Errorf("splitInternal alloc: %w", err)
	}
	rightNodeID := uint64(rightNodePageID)
	rightPg, err := t.bm.PinPage(t.btreeID, rightNodePageID)
	if err != nil {
		_ = t.bm.FreePage(t.btreeID, rightNodePageID)
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), false)
		return nil, fmt.Errorf("splitInternal pin right %d: %w", rightNodeID, err)
	}
	rightPageData := rightPg.Data
	if initErr := page.InitializeInternalNode(rightPageData); initErr != nil {
		_ = t.bm.UnpinPage(t.btreeID, rightNodePageID, false)
		_ = t.bm.FreePage(t.btreeID, rightNodePageID)
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), false)
		return nil, fmt.Errorf("splitInternal init right %d: %w", rightNodeID, initErr)
	}
	rightNode := page.PageToInternalNode(rightPageData)
	numKeys := int(leftNode.Header.NumKeys)
	totalKeys := numKeys + 1
	tempKeys := make([]uint64, totalKeys)
	tempChildren := make([]uint64, totalKeys+1)
	insertPos := 0
	for insertPos < numKeys && leftNode.Keys[insertPos] < insertKey {
		insertPos++
	}
	copy(tempKeys[:insertPos], leftNode.Keys[:insertPos])
	copy(tempChildren[:insertPos+1], leftNode.Children[:insertPos+1])
	tempKeys[insertPos] = insertKey
	tempChildren[insertPos+1] = insertChildID
	copy(tempKeys[insertPos+1:], leftNode.Keys[insertPos:])
	copy(tempChildren[insertPos+2:], leftNode.Children[insertPos+1:])
	splitPointIndex := totalKeys / 2
	keyToPromote := tempKeys[splitPointIndex]
	leftNode.Header.NumKeys = uint16(splitPointIndex)
	page.SetNumKeys(leftPg.Data, leftNode.Header.NumKeys)
	copy(leftNode.Keys[:splitPointIndex], tempKeys[:splitPointIndex])
	copy(leftNode.Children[:splitPointIndex+1], tempChildren[:splitPointIndex+1])
	for i := splitPointIndex; i < page.MaxInternalKeys; i++ {
		leftNode.Keys[i] = 0
	}
	for i := splitPointIndex + 1; i <= page.MaxInternalKeys; i++ {
		leftNode.Children[i] = 0
	}
	rightNode.Header.NumKeys = uint16(totalKeys - 1 - splitPointIndex)
	page.SetNumKeys(rightPageData, rightNode.Header.NumKeys)
	copy(rightNode.Keys[:rightNode.Header.NumKeys], tempKeys[splitPointIndex+1:])
	copy(rightNode.Children[:rightNode.Header.NumKeys+1], tempChildren[splitPointIndex+1:])
	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), true)
	_ = t.bm.UnpinPage(t.btreeID, rightNodePageID, true)
	return &splitInfo{key: keyToPromote, rightID: rightNodeID}, nil
}

func (t *BTreeImpl) findLeafNode(key uint64) (uint64, error) {
	currentPageID := t.rootID
	if currentPageID == 0 {
		return 0, ErrTreeNotInit
	}
	for {
		pg, err := t.bm.PinPage(t.btreeID, buffer.PageID(currentPageID))
		if err != nil {
			return 0, fmt.Errorf("findLeaf pin %d: %w", currentPageID, err)
		}
		pageData := pg.Data
		nodeType := page.GetNodeType(pageData)
		switch nodeType {
		case page.LeafNodeType:
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentPageID), false)
			return currentPageID, nil
		case page.InternalNodeType:
			internalNode := page.PageToInternalNode(pageData)
			idx := page.InternalSearch(internalNode, key)
			childPageID := internalNode.Children[idx]
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentPageID), false)
			if childPageID == 0 {
				return 0, fmt.Errorf("findLeaf null child %d idx %d key %d", currentPageID, idx, key)
			}
			currentPageID = childPageID
		default:
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentPageID), false)
			return 0, fmt.Errorf("findLeaf invalid type %d page %d", nodeType, currentPageID)
		}
	}
}

func getNodeSiblings(parent *page.InternalNode, indexInParent int) (uint64, uint64) {
	var leftSiblingID, rightSiblingID uint64
	if indexInParent > 0 {
		leftSiblingID = parent.Children[indexInParent-1]
	}
	if indexInParent < int(parent.Header.NumKeys) {
		rightSiblingID = parent.Children[indexInParent+1]
	}
	return leftSiblingID, rightSiblingID
}
