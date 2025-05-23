package btree

import (
	"errors"
	"fmt"
	"minibtreestore/storage/buffer"
	"minibtreestore/storage/page"
	"os"
)

func (t *BTreeImpl) Delete(key uint64) error {
	if t.rootID == 0 {
		return ErrKeyNotFound
	}
	_, err := t.deleteRecursive(t.rootID, key, 0, 0)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		return err
	}

	return t.checkAndUpdateRoot()
}

func (t *BTreeImpl) checkAndUpdateRoot() error {
	rootPg, pinErr := t.bm.PinPage(t.btreeID, buffer.PageID(t.rootID))
	if pinErr != nil {
		return fmt.Errorf("critical: failed pin root %d post-delete: %w", t.rootID, pinErr)
	}

	rootPageData := rootPg.Data
	rootNumKeys := page.GetNumKeys(rootPageData)
	rootNodeType := page.GetNodeType(rootPageData)
	oldRootID := t.rootID
	rootNeedsUnpin := true

	if rootNodeType == page.InternalNodeType && rootNumKeys == 0 {
		rootNode := page.PageToInternalNode(rootPageData)
		newRootID := rootNode.Children[0]

		unpinOldRootErr := t.bm.UnpinPage(t.btreeID, buffer.PageID(oldRootID), false)
		if unpinOldRootErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed unpin old root %d before free: %v\n", oldRootID, unpinOldRootErr)
		}

		if freeErr := t.bm.FreePage(t.btreeID, buffer.PageID(oldRootID)); freeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed free old root %d: %v\n", oldRootID, freeErr)
		}

		t.rootID = newRootID
		rootNeedsUnpin = false
	}

	if rootNeedsUnpin {
		unpinErr := t.bm.UnpinPage(t.btreeID, buffer.PageID(oldRootID), false)
		if unpinErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed unpin root %d post-delete check: %v\n", oldRootID, unpinErr)
		}
	}

	return nil
}

func (t *BTreeImpl) deleteRecursive(nodeID uint64, key uint64, parentID uint64, indexInParent int) (childModified bool, err error) {
	pg, pinErr := t.bm.PinPage(t.btreeID, buffer.PageID(nodeID))
	if pinErr != nil {
		return false, fmt.Errorf("pin %d fail delRec: %w", nodeID, pinErr)
	}
	pageData := pg.Data
	nodeType := page.GetNodeType(pageData)
	nodeModified := false
	needsRebalance := false
	var recursiveErr error

	switch nodeType {
	case page.LeafNodeType:
		nodeModified, needsRebalance, err = t.deleteFromLeafNode(pg, nodeID, key)
		if err != nil {
			return false, err
		}

		if needsRebalance && nodeID != t.rootID {
			err = t.handleLeafUnderflow(parentID, nodeID, indexInParent, pg)
			return true, err
		} else {
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), nodeModified)
			return nodeModified, nil
		}

	case page.InternalNodeType:
		childModified, recursiveErr = t.deleteFromInternalNode(pg, nodeID, key, parentID, indexInParent)
		if recursiveErr != nil {
			return false, recursiveErr
		}

		return childModified, nil

	default:
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), false)
		return false, fmt.Errorf("invalid node type %d page %d delete", nodeType, nodeID)
	}
}

func (t *BTreeImpl) deleteFromLeafNode(pg buffer.Page, nodeID uint64, key uint64) (nodeModified bool, needsRebalance bool, err error) {
	leafNode := page.PageToLeafNode(pg.Data)
	numKeys := int(leafNode.Header.NumKeys)
	idx, found := page.LeafSearch(leafNode, key)

	if !found {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), false)
		return false, false, ErrKeyNotFound
	}

	// Remove key and value by shifting remaining entries
	copy(leafNode.Keys[idx:], leafNode.Keys[idx+1:numKeys])
	copy(leafNode.Values[idx:], leafNode.Values[idx+1:numKeys])
	leafNode.Keys[numKeys-1] = 0
	leafNode.Values[numKeys-1] = 0
	leafNode.Header.NumKeys--
	page.SetNumKeys(pg.Data, leafNode.Header.NumKeys)

	// Check if node needs rebalancing
	needsRebalance = leafNode.Header.NumKeys < page.MinLeafKeys && nodeID != t.rootID

	return true, needsRebalance, nil
}

func (t *BTreeImpl) deleteFromInternalNode(pg buffer.Page, nodeID uint64, key uint64, parentID uint64, indexInParent int) (bool, error) {
	internalNode := page.PageToInternalNode(pg.Data)
	childIndex := page.InternalSearch(internalNode, key)
	childPageID := internalNode.Children[childIndex]

	if childPageID == 0 {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), false)
		return false, fmt.Errorf("internal error null child %d idx %d key %d", nodeID, childIndex, key)
	}

	unpinErr := t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), false)
	if unpinErr != nil {
		return false, fmt.Errorf("unpin internal %d before delRec: %w", nodeID, unpinErr)
	}

	childModified, recursiveErr := t.deleteRecursive(childPageID, key, nodeID, childIndex)

	pg, pinErr := t.bm.PinPage(t.btreeID, buffer.PageID(nodeID))
	if pinErr != nil {
		return false, fmt.Errorf("re-pin internal %d after delRec: %w", nodeID, pinErr)
	}

	pageData := pg.Data
	internalNode = page.PageToInternalNode(pageData)

	if recursiveErr != nil {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), false)
		if errors.Is(recursiveErr, ErrKeyNotFound) {
			return false, ErrKeyNotFound
		}
		return false, recursiveErr
	}

	nodeModified := childModified
	needsRebalance := internalNode.Header.NumKeys < page.MinInternalKeys && nodeID != t.rootID

	if needsRebalance {
		err := t.handleInternalUnderflow(parentID, nodeID, indexInParent, pg)
		return true, err
	} else {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), nodeModified)
		return nodeModified, nil
	}
}

func (t *BTreeImpl) handleLeafUnderflow(parentID uint64, nodeID uint64, indexInParent int, nodePg buffer.Page) (err error) {
	nodeDirty := true
	operationPerformed := false

	defer func() {
		if !operationPerformed && nodePg.Data != nil {
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), nodeDirty)
		}
	}()

	parentPg, pinErr := t.bm.PinPage(t.btreeID, buffer.PageID(parentID))
	if pinErr != nil {
		return fmt.Errorf("hlu pin parent %d: %w", parentID, pinErr)
	}
	parent := page.PageToInternalNode(parentPg.Data)
	parentDirty := false
	defer func() { _ = t.bm.UnpinPage(t.btreeID, buffer.PageID(parentID), parentDirty) }()

	leftSiblingID, rightSiblingID := getNodeSiblings(parent, indexInParent)

	// Try borrow from left sibling
	if success, err := t.tryBorrowFromLeftLeaf(parent, indexInParent, leftSiblingID, nodeID, nodePg); err != nil {
		return err
	} else if success {
		operationPerformed = true
		parentDirty = true
		return nil
	}

	// Try borrow from right sibling
	if success, err := t.tryBorrowFromRightLeaf(parent, indexInParent, rightSiblingID, nodeID, nodePg); err != nil {
		return err
	} else if success {
		operationPerformed = true
		parentDirty = true
		return nil
	}

	// Merge
	if indexInParent > 0 { // Merge with Left (Node is Right)
		err = t.mergeLeafNodes(parentPg, indexInParent-1, leftSiblingID, nodeID)
		if err == nil {
			operationPerformed = true
			nodePg = buffer.Page{}
		}
		parentDirty = true
	} else if indexInParent < int(parent.Header.NumKeys) { // Merge with Right (Node is Left)
		err = t.mergeLeafNodes(parentPg, indexInParent, nodeID, rightSiblingID)
		if err == nil {
			operationPerformed = true
			nodeDirty = true
		}
		parentDirty = true
	} else {
		err = fmt.Errorf("hlu: node %d no valid sibling to merge with", nodeID)
		operationPerformed = true
	}
	return err
}

func (t *BTreeImpl) tryBorrowFromLeftLeaf(parent *page.InternalNode, indexInParent int, leftSiblingID, nodeID uint64, nodePg buffer.Page) (bool, error) {
	if indexInParent <= 0 {
		return false, nil
	}

	leftSiblingPg, pinLErr := t.bm.PinPage(t.btreeID, buffer.PageID(leftSiblingID))
	if pinLErr != nil {
		return false, nil
	}

	leftSibling := page.PageToLeafNode(leftSiblingPg.Data)
	if leftSibling.Header.NumKeys <= page.MinLeafKeys {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftSiblingID), false)
		return false, nil
	}

	node := page.PageToLeafNode(nodePg.Data)
	numNodeKeys := int(node.Header.NumKeys)
	numLeftKeys := int(leftSibling.Header.NumKeys)

	borrowedKey := leftSibling.Keys[numLeftKeys-1]
	borrowedValue := leftSibling.Values[numLeftKeys-1]

	leftSibling.Keys[numLeftKeys-1] = 0
	leftSibling.Values[numLeftKeys-1] = 0
	leftSibling.Header.NumKeys--
	page.SetNumKeys(leftSiblingPg.Data, leftSibling.Header.NumKeys)

	copy(node.Keys[1:], node.Keys[:numNodeKeys])
	copy(node.Values[1:], node.Values[:numNodeKeys])
	node.Keys[0] = borrowedKey
	node.Values[0] = borrowedValue
	node.Header.NumKeys++
	page.SetNumKeys(nodePg.Data, node.Header.NumKeys)

	parent.Keys[indexInParent-1] = borrowedKey

	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftSiblingID), true)
	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), true)

	return true, nil
}

func (t *BTreeImpl) tryBorrowFromRightLeaf(parent *page.InternalNode, indexInParent int, rightSiblingID, nodeID uint64, nodePg buffer.Page) (bool, error) {
	if indexInParent >= int(parent.Header.NumKeys) {
		return false, nil
	}

	rightSiblingPg, pinRErr := t.bm.PinPage(t.btreeID, buffer.PageID(rightSiblingID))
	if pinRErr != nil {
		return false, nil
	}

	rightSibling := page.PageToLeafNode(rightSiblingPg.Data)
	if rightSibling.Header.NumKeys <= page.MinLeafKeys {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(rightSiblingID), false)
		return false, nil
	}

	node := page.PageToLeafNode(nodePg.Data)
	numNodeKeys := int(node.Header.NumKeys)
	numRightKeys := int(rightSibling.Header.NumKeys)

	borrowedKey := rightSibling.Keys[0]
	borrowedValue := rightSibling.Values[0]

	copy(rightSibling.Keys[:], rightSibling.Keys[1:numRightKeys])
	copy(rightSibling.Values[:], rightSibling.Values[1:numRightKeys])
	rightSibling.Keys[numRightKeys-1] = 0
	rightSibling.Values[numRightKeys-1] = 0
	rightSibling.Header.NumKeys--
	page.SetNumKeys(rightSiblingPg.Data, rightSibling.Header.NumKeys)

	node.Keys[numNodeKeys] = borrowedKey
	node.Values[numNodeKeys] = borrowedValue
	node.Header.NumKeys++
	page.SetNumKeys(nodePg.Data, node.Header.NumKeys)

	parent.Keys[indexInParent] = rightSibling.Keys[0]

	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(rightSiblingID), true)
	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), true)

	return true, nil
}

func (t *BTreeImpl) mergeLeafNodes(parentPg buffer.Page, parentKeyIndex int, leftNodeID, rightNodeID uint64) error {
	parent := page.PageToInternalNode(parentPg.Data)
	leftPg, pinLErr := t.bm.PinPage(t.btreeID, buffer.PageID(leftNodeID))
	if pinLErr != nil {
		return fmt.Errorf("mln pin left %d: %w", leftNodeID, pinLErr)
	}
	defer func() { _ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), true) }()

	rightPg, pinRErr := t.bm.PinPage(t.btreeID, buffer.PageID(rightNodeID))
	if pinRErr != nil {
		return fmt.Errorf("mln pin right %d: %w", rightNodeID, pinRErr)
	}

	leftNode := page.PageToLeafNode(leftPg.Data)
	rightNode := page.PageToLeafNode(rightPg.Data)
	leftNumKeys := int(leftNode.Header.NumKeys)
	rightNumKeys := int(rightNode.Header.NumKeys)

	if leftNumKeys+rightNumKeys > page.MaxLeafKeys {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(rightNodeID), false)
		return fmt.Errorf("mln combined keys %d > max %d", leftNumKeys+rightNumKeys, page.MaxLeafKeys)
	}

	// Copy keys and values from right to left
	copy(leftNode.Keys[leftNumKeys:], rightNode.Keys[:rightNumKeys])
	copy(leftNode.Values[leftNumKeys:], rightNode.Values[:rightNumKeys])
	leftNode.Header.NumKeys += uint16(rightNumKeys)
	page.SetNumKeys(leftPg.Data, leftNode.Header.NumKeys)

	// Update leaf node chain
	leftNode.NextLeaf = rightNode.NextLeaf
	page.SetNextLeafPageID(leftNode, leftNode.NextLeaf)

	// Remove from parent
	parentNumKeys := int(parent.Header.NumKeys)
	copy(parent.Keys[parentKeyIndex:], parent.Keys[parentKeyIndex+1:parentNumKeys])
	copy(parent.Children[parentKeyIndex+1:], parent.Children[parentKeyIndex+2:parentNumKeys+1])
	parent.Keys[parentNumKeys-1] = 0
	parent.Children[parentNumKeys] = 0
	parent.Header.NumKeys--
	page.SetNumKeys(parentPg.Data, parent.Header.NumKeys)

	// Unpin right before freeing
	unpinRErr := t.bm.UnpinPage(t.btreeID, buffer.PageID(rightNodeID), false)
	if unpinRErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed unpin right %d before free: %v\n", rightNodeID, unpinRErr)
	}

	if freeErr := t.bm.FreePage(t.btreeID, buffer.PageID(rightNodeID)); freeErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed free page %d after merge: %v\n", rightNodeID, freeErr)
	}

	return nil
}

func (t *BTreeImpl) handleInternalUnderflow(parentID uint64, nodeID uint64, indexInParent int, nodePg buffer.Page) (err error) {
	nodeDirty := true
	operationPerformed := false
	defer func() {
		if !operationPerformed && nodePg.Data != nil {
			_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), nodeDirty)
		}
	}()

	parentPg, pinErr := t.bm.PinPage(t.btreeID, buffer.PageID(parentID))
	if pinErr != nil {
		return fmt.Errorf("hiu pin parent %d: %w", parentID, pinErr)
	}
	parent := page.PageToInternalNode(parentPg.Data)
	parentDirty := false
	defer func() { _ = t.bm.UnpinPage(t.btreeID, buffer.PageID(parentID), parentDirty) }()

	leftSiblingID, rightSiblingID := getNodeSiblings(parent, indexInParent)

	// Try borrow from left sibling
	if success, err := t.tryBorrowFromLeftInternal(parent, indexInParent, leftSiblingID, nodeID, nodePg); err != nil {
		return err
	} else if success {
		operationPerformed = true
		parentDirty = true
		return nil
	}

	// Try borrow from right sibling
	if success, err := t.tryBorrowFromRightInternal(parent, indexInParent, rightSiblingID, nodeID, nodePg); err != nil {
		return err
	} else if success {
		operationPerformed = true
		parentDirty = true
		return nil
	}

	// Merge
	if indexInParent > 0 {
		err = t.mergeInternalNodes(parentPg, indexInParent-1, leftSiblingID, nodeID)
		if err == nil {
			operationPerformed = true
			nodePg = buffer.Page{}
		}
		parentDirty = true
	} else if indexInParent < int(parent.Header.NumKeys) {
		err = t.mergeInternalNodes(parentPg, indexInParent, nodeID, rightSiblingID)
		if err == nil {
			operationPerformed = true
			nodeDirty = true
		}
		parentDirty = true
	} else {
		err = fmt.Errorf("hiu: node %d no valid sibling to merge", nodeID)
		operationPerformed = true
	}
	return err
}

func (t *BTreeImpl) tryBorrowFromLeftInternal(parent *page.InternalNode, indexInParent int, leftSiblingID, nodeID uint64, nodePg buffer.Page) (bool, error) {
	if indexInParent <= 0 {
		return false, nil
	}

	leftSiblingPg, pinLErr := t.bm.PinPage(t.btreeID, buffer.PageID(leftSiblingID))
	if pinLErr != nil {
		return false, nil
	}

	leftSibling := page.PageToInternalNode(leftSiblingPg.Data)
	if leftSibling.Header.NumKeys <= page.MinInternalKeys {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftSiblingID), false)
		return false, nil
	}

	node := page.PageToInternalNode(nodePg.Data)
	numNodeKeys := int(node.Header.NumKeys)
	numLeftKeys := int(leftSibling.Header.NumKeys)

	separatorKey := parent.Keys[indexInParent-1]

	// Make space and insert at beginning of deficient node
	copy(node.Keys[1:], node.Keys[:numNodeKeys])
	copy(node.Children[1:], node.Children[:numNodeKeys+1])

	node.Keys[0] = separatorKey
	node.Children[0] = leftSibling.Children[numLeftKeys]
	node.Header.NumKeys++
	page.SetNumKeys(nodePg.Data, node.Header.NumKeys)

	// Move up the last key from left sibling to parent
	parent.Keys[indexInParent-1] = leftSibling.Keys[numLeftKeys-1]

	// Clean up left sibling
	leftSibling.Keys[numLeftKeys-1] = 0
	leftSibling.Children[numLeftKeys] = 0
	leftSibling.Header.NumKeys--
	page.SetNumKeys(leftSiblingPg.Data, leftSibling.Header.NumKeys)

	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftSiblingID), true)
	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), true)

	return true, nil
}

func (t *BTreeImpl) tryBorrowFromRightInternal(parent *page.InternalNode, indexInParent int, rightSiblingID, nodeID uint64, nodePg buffer.Page) (bool, error) {
	if indexInParent >= int(parent.Header.NumKeys) {
		return false, nil
	}

	rightSiblingPg, pinRErr := t.bm.PinPage(t.btreeID, buffer.PageID(rightSiblingID))
	if pinRErr != nil {
		return false, nil
	}

	rightSibling := page.PageToInternalNode(rightSiblingPg.Data)
	if rightSibling.Header.NumKeys <= page.MinInternalKeys {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(rightSiblingID), false)
		return false, nil
	}

	node := page.PageToInternalNode(nodePg.Data)
	numNodeKeys := int(node.Header.NumKeys)
	numRightKeys := int(rightSibling.Header.NumKeys)

	// Get separator key from parent and add to end of deficient node
	separatorKey := parent.Keys[indexInParent]
	node.Keys[numNodeKeys] = separatorKey
	node.Children[numNodeKeys+1] = rightSibling.Children[0]
	node.Header.NumKeys++
	page.SetNumKeys(nodePg.Data, node.Header.NumKeys)

	// Move up first key from right sibling to parent
	parent.Keys[indexInParent] = rightSibling.Keys[0]

	// Clean up right sibling
	copy(rightSibling.Keys[:], rightSibling.Keys[1:numRightKeys])
	copy(rightSibling.Children[:], rightSibling.Children[1:numRightKeys+1])
	rightSibling.Keys[numRightKeys-1] = 0
	rightSibling.Children[numRightKeys] = 0
	rightSibling.Header.NumKeys--
	page.SetNumKeys(rightSiblingPg.Data, rightSibling.Header.NumKeys)

	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(rightSiblingID), true)
	_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(nodeID), true)

	return true, nil
}

func (t *BTreeImpl) mergeInternalNodes(parentPg buffer.Page, parentKeyIndex int, leftNodeID, rightNodeID uint64) error {
	parent := page.PageToInternalNode(parentPg.Data)
	leftPg, pinLErr := t.bm.PinPage(t.btreeID, buffer.PageID(leftNodeID))
	if pinLErr != nil {
		return fmt.Errorf("min pin left %d: %w", leftNodeID, pinLErr)
	}
	defer func() { _ = t.bm.UnpinPage(t.btreeID, buffer.PageID(leftNodeID), true) }()

	rightPg, pinRErr := t.bm.PinPage(t.btreeID, buffer.PageID(rightNodeID))
	if pinRErr != nil {
		return fmt.Errorf("min pin right %d: %w", rightNodeID, pinRErr)
	}

	leftNode := page.PageToInternalNode(leftPg.Data)
	rightNode := page.PageToInternalNode(rightPg.Data)
	leftNumKeys := int(leftNode.Header.NumKeys)
	rightNumKeys := int(rightNode.Header.NumKeys)

	if leftNumKeys+rightNumKeys+1 > page.MaxInternalKeys {
		_ = t.bm.UnpinPage(t.btreeID, buffer.PageID(rightNodeID), false)
		return fmt.Errorf("min combined keys %d > max %d", leftNumKeys+rightNumKeys+1, page.MaxInternalKeys)
	}

	// Get separator key from parent
	separatorKey := parent.Keys[parentKeyIndex]

	// Move separator key and all right node content to left node
	leftNode.Keys[leftNumKeys] = separatorKey
	copy(leftNode.Keys[leftNumKeys+1:], rightNode.Keys[:rightNumKeys])
	copy(leftNode.Children[leftNumKeys+1:], rightNode.Children[:rightNumKeys+1])
	leftNode.Header.NumKeys = uint16(leftNumKeys + 1 + rightNumKeys)
	page.SetNumKeys(leftPg.Data, leftNode.Header.NumKeys)

	// Remove separator key from parent
	parentNumKeys := int(parent.Header.NumKeys)
	copy(parent.Keys[parentKeyIndex:], parent.Keys[parentKeyIndex+1:parentNumKeys])
	copy(parent.Children[parentKeyIndex+1:], parent.Children[parentKeyIndex+2:parentNumKeys+1])
	parent.Keys[parentNumKeys-1] = 0
	parent.Children[parentNumKeys] = 0
	parent.Header.NumKeys--
	page.SetNumKeys(parentPg.Data, parent.Header.NumKeys)

	// Free right node
	unpinRErr := t.bm.UnpinPage(t.btreeID, buffer.PageID(rightNodeID), false)
	if unpinRErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed unpin right %d before free: %v\n", rightNodeID, unpinRErr)
	}

	if freeErr := t.bm.FreePage(t.btreeID, buffer.PageID(rightNodeID)); freeErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed free page %d after internal merge: %v\n", rightNodeID, freeErr)
	}

	return nil
}
