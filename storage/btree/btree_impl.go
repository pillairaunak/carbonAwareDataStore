package btree

import (
	"errors"
	"fmt"
	"minibtreestore/storage/buffer"
	"minibtreestore/storage/page"
)

type BTreeImpl struct {
	bm      buffer.BufferManager
	btreeID string
	rootID  uint64
}

func NewBTreePersistent(bm buffer.BufferManager, btreeID string) (BTree, error) {
	if bm == nil {
		return nil, errors.New("buffer manager cannot be nil")
	}
	if btreeID == "" {
		return nil, errors.New("btreeID cannot be empty")
	}

	tree := &BTreeImpl{bm: bm, btreeID: btreeID}

	pg, err := bm.PinPage(btreeID, buffer.PageID(RootPageID))
	if err != nil {
		return nil, fmt.Errorf("failed pin root %d: %w", RootPageID, err)
	}

	pageData := pg.Data
	nodeType := page.GetNodeType(pageData)
	isInitialized := (nodeType == page.LeafNodeType || nodeType == page.InternalNodeType)

	if !isInitialized {
		initErr := page.InitializeLeafNode(pageData)
		unpinErr := bm.UnpinPage(btreeID, buffer.PageID(RootPageID), initErr == nil)
		if initErr != nil {
			return nil, fmt.Errorf("failed format root %d: %w (unpin: %v)", RootPageID, initErr, unpinErr)
		}
		if unpinErr != nil {
			return nil, fmt.Errorf("failed unpin init root %d: %w", RootPageID, unpinErr)
		}
		tree.rootID = RootPageID
	} else {
		unpinErr := bm.UnpinPage(btreeID, buffer.PageID(RootPageID), false)
		if unpinErr != nil {
			return nil, fmt.Errorf("failed unpin root %d: %w", RootPageID, unpinErr)
		}
		tree.rootID = RootPageID
	}

	return tree, nil
}

func (t *BTreeImpl) Lookup(key uint64) (value uint64, found bool) {
	currentPageID := t.rootID
	if currentPageID == 0 {
		return 0, false
	}

	for {
		pg, err := t.bm.PinPage(t.btreeID, buffer.PageID(currentPageID))
		if err != nil {
			return 0, false
		}

		pageData := pg.Data
		nodeType := page.GetNodeType(pageData)

		switch nodeType {
		case page.LeafNodeType:
			leafNode := page.PageToLeafNode(pageData)
			idx, keyFound := page.LeafSearch(leafNode, key)
			if keyFound {
				value = leafNode.Values[idx]
				found = true
			}
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentPageID), false)
			return value, found

		case page.InternalNodeType:
			internalNode := page.PageToInternalNode(pageData)
			idx := page.InternalSearch(internalNode, key)
			childPageID := internalNode.Children[idx]
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentPageID), false)
			if childPageID == 0 {
				return 0, false
			}
			currentPageID = childPageID

		default:
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentPageID), false)
			return 0, false
		}
	}
}

func (t *BTreeImpl) Insert(key uint64, value uint64) error {
	if t.rootID == 0 {
		return errors.New("insert failed, root is not initialized")
	}

	split, err := t.insertRecursive(t.rootID, key, value)
	if err != nil {
		return fmt.Errorf("insertion failed key %d: %w", key, err)
	}

	if split != nil {
		if err = t.createNewRoot(split.key, t.rootID, split.rightID); err != nil {
			return fmt.Errorf("failed create new root: %w", err)
		}
	}

	return nil
}

func (t *BTreeImpl) insertRecursive(nodeID uint64, key, value uint64) (*splitInfo, error) {
	pg, err := t.bm.PinPage(t.btreeID, buffer.PageID(nodeID))
	if err != nil {
		return nil, fmt.Errorf("pin %d failed insertRec: %w", nodeID, err)
	}

	pageData := pg.Data
	nodeType := page.GetNodeType(pageData)
	var resultedSplit *splitInfo

	switch nodeType {
	case page.LeafNodeType:
		leafNode := page.PageToLeafNode(pageData)
		numKeys := int(leafNode.Header.NumKeys)
		idx, found := page.LeafSearch(leafNode, key)

		if found {
			leafNode.Values[idx] = value
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), true)
			return nil, nil
		}

		if numKeys >= page.MaxLeafKeys {
			resultedSplit, err = t.splitLeafNode(nodeID, pg, key, value)
			if err != nil {
				return nil, fmt.Errorf("split leaf %d: %w", nodeID, err)
			}
			return resultedSplit, nil
		} else {
			copy(leafNode.Keys[idx+1:], leafNode.Keys[idx:numKeys])
			copy(leafNode.Values[idx+1:], leafNode.Values[idx:numKeys])
			leafNode.Keys[idx] = key
			leafNode.Values[idx] = value
			leafNode.Header.NumKeys++
			page.SetNumKeys(pageData, leafNode.Header.NumKeys)
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), true)
			return nil, nil
		}

	case page.InternalNodeType:
		internalNode := page.PageToInternalNode(pageData)
		idx := page.InternalSearch(internalNode, key)
		childPageID := internalNode.Children[idx]
		unpinErr := t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), false)
		if unpinErr != nil {
			return nil, fmt.Errorf("unpin internal %d before insertRec: %w", nodeID, unpinErr)
		}

		if childPageID == 0 {
			return nil, fmt.Errorf("internal error null child %d key %d", nodeID, key)
		}

		childSplitInfo, recursiveErr := t.insertRecursive(childPageID, key, value)
		if recursiveErr != nil {
			return nil, recursiveErr
		}

		if childSplitInfo == nil {
			return nil, nil
		}

		pg, err = t.bm.PinPage(t.btreeID, buffer.PageID(nodeID))
		if err != nil {
			return nil, fmt.Errorf("re-pin internal %d after split: %w", nodeID, err)
		}

		pageData = pg.Data
		internalNode = page.PageToInternalNode(pageData)
		numKeys := int(internalNode.Header.NumKeys)

		if numKeys >= page.MaxInternalKeys {
			resultedSplit, err = t.splitInternalNode(nodeID, pg, childSplitInfo.key, childSplitInfo.rightID)
			if err != nil {
				return nil, fmt.Errorf("split internal %d: %w", nodeID, err)
			}
			return resultedSplit, nil
		} else {
			insertPos := 0
			for insertPos < numKeys && internalNode.Keys[insertPos] < childSplitInfo.key {
				insertPos++
			}
			copy(internalNode.Keys[insertPos+1:], internalNode.Keys[insertPos:numKeys])
			copy(internalNode.Children[insertPos+2:], internalNode.Children[insertPos+1:numKeys+1])
			internalNode.Keys[insertPos] = childSplitInfo.key
			internalNode.Children[insertPos+1] = childSplitInfo.rightID
			internalNode.Header.NumKeys++
			page.SetNumKeys(pageData, internalNode.Header.NumKeys)
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), true)
			return nil, nil
		}

	default:
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), false)
		return nil, fmt.Errorf("invalid node type %d page %d insert", nodeType, nodeID)
	}
}

func (t *BTreeImpl) Scan(minKey uint64, maxKey uint64) ([]uint64, error) {
	results := make([]uint64, 0)
	startLeafID, err := t.findLeafNode(minKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrTreeNotInit) {
			return results, nil
		}
		return nil, fmt.Errorf("scan find leaf %d: %w", minKey, err)
	}

	if startLeafID == 0 {
		return results, nil
	}

	currentLeafID := startLeafID
	for currentLeafID != 0 {
		pg, err := t.bm.PinPage(t.btreeID, buffer.PageID(currentLeafID))
		if err != nil {
			return results, fmt.Errorf("scan pin leaf %d: %w", currentLeafID, err)
		}

		pageData := pg.Data
		if page.GetNodeType(pageData) != page.LeafNodeType {
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentLeafID), false)
			return results, fmt.Errorf("scan expected leaf %d", currentLeafID)
		}

		leafNode := page.PageToLeafNode(pageData)
		numKeys := int(leafNode.Header.NumKeys)
		stopScan := false
		startPos := 0

		for startPos < numKeys && leafNode.Keys[startPos] < minKey {
			startPos++
		}

		for i := startPos; i < numKeys; i++ {
			key := leafNode.Keys[i]
			if key > maxKey {
				stopScan = true
				break
			}
			results = append(results, leafNode.Values[i])
		}

		nextLeafID := page.GetNextLeafPageID(leafNode)
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(currentLeafID), false)

		if stopScan {
			break
		}
		currentLeafID = nextLeafID
	}

	return results, nil
}
