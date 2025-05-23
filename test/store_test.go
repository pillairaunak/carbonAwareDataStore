package test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"minibtreestore/storage/btree"
	"minibtreestore/storage/buffer"
	"minibtreestore/storage/page"
	"minibtreestore/util/loader"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// --- Helper Functions ---
func createTempDir(t *testing.T) string { return t.TempDir() }
func createCSVFile(t *testing.T, dir, filename, content string) string {
	filePath := filepath.Join(dir, filename)
	err := ioutil.WriteFile(filePath, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}
	return filePath
}

// --- Existing Tests (Cleaned up style slightly) ---

func TestBasicBTreeOperations(t *testing.T) {
	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	} // Increased buffer slightly
	btreeName := "test_btree"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() {
		if err := bm.CloseBTree(btreeName); err != nil {
			t.Logf("Cleanup Warn: %v", err)
		}
	})
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}
	t.Run("Insert and Lookup", func(t *testing.T) {
		testData := map[uint64]uint64{1: 100, 5: 500, 10: 1000, 15: 1500, 20: 2000}
		for k, v := range testData {
			if err := tree.Insert(k, v); err != nil {
				t.Errorf("Insert(%d) failed: %v", k, err)
			}
		}
		for k, expectedV := range testData {
			v, found := tree.Lookup(k)
			if !found {
				t.Errorf("Lookup(%d) failed", k)
			} else if v != expectedV {
				t.Errorf("Lookup(%d) wrong: got %d, want %d", k, v, expectedV)
			}
		}
		_, found := tree.Lookup(999)
		if found {
			t.Errorf("Lookup(999) found non-existent")
		}
	})
	t.Run("Scan", func(t *testing.T) {
		values, err := tree.Scan(5, 15)
		if err != nil {
			t.Errorf("Scan(5, 15) fail: %v", err)
		}
		expected := []uint64{500, 1000, 1500}
		if !reflect.DeepEqual(values, expected) {
			t.Errorf("Scan(5, 15) wrong: got %v, want %v", values, expected)
		}
		values, err = tree.Scan(100, 200)
		if err != nil {
			t.Errorf("Scan(100, 200) fail: %v", err)
		}
		if len(values) != 0 {
			t.Errorf("Scan(100, 200) not empty: %v", values)
		}
	})
}

func TestBTreeSplitting(t *testing.T) {
	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "split_test_btree"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() {
		if err := bm.CloseBTree(btreeName); err != nil {
			t.Logf("Cleanup Warn: %v", err)
		}
	})
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}
	numElements := 256
	for i := 1; i <= numElements; i++ {
		if err := tree.Insert(uint64(i), uint64(i*100)); err != nil {
			t.Logf("Insert(%d) fail split stress: %v", i, err)
		}
	}
	for i := 1; i <= numElements; i++ {
		v, found := tree.Lookup(uint64(i))
		if !found {
			t.Errorf("Lookup(%d) fail split: not found", i)
		} else if v != uint64(i*100) {
			t.Errorf("Lookup(%d) wrong val split: got %d, want %d", i, v, uint64(i*100))
		}
	}
	t.Run("Scan_After_Split", func(t *testing.T) {
		sMin, sMax := 50, 55
		values, err := tree.Scan(uint64(sMin), uint64(sMax))
		if err != nil {
			t.Errorf("Scan(%d, %d) fail: %v", sMin, sMax, err)
		}
		expected := []uint64{}
		for i := sMin; i <= sMax; i++ {
			expected = append(expected, uint64(i*100))
		}
		if !reflect.DeepEqual(values, expected) {
			t.Errorf("Scan(%d, %d) wrong: got %v, want %v", sMin, sMax, values, expected)
		}
		fMin, fMax := 1, uint64(numElements)
		values, err = tree.Scan(uint64(fMin), fMax)
		if err != nil {
			t.Errorf("Scan(%d, %d) fail: %v", fMin, fMax, err)
		}
		if len(values) != numElements {
			t.Errorf("Scan(%d, %d) wrong count: got %d, want %d", fMin, fMax, len(values), numElements)
		}
	})
}

func TestCSVLoader(t *testing.T) {
	tempDir := createTempDir(t)
	csvContent := "1,100\n2,200\n3,300\n#C\n5,500\n10,1000\n"
	csvPath := createCSVFile(t, tempDir, "test.csv", csvContent)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "csv_test_btree"
	t.Cleanup(func() {
		if bm != nil {
			if err := bm.CloseBTree(btreeName); err != nil && !errors.Is(err, buffer.ErrBTreeNotFound) {
				t.Logf("Cleanup Warn: %v", err)
			}
		}
	})
	result, err := loader.LoadCSV(bm, btreeName, csvPath)
	if err != nil {
		t.Fatalf("LoadCSV fail: %v", err)
	}
	if len(result.Errors) > 0 {
		t.Errorf("LoadCSV errors: %v", result.Errors)
	}
	eP, eI := 6, 5
	if result.EntriesProcessed != eP {
		t.Errorf("Wrong processed: got %d, want %d", result.EntriesProcessed, eP)
	}
	if result.EntriesInserted != eI {
		t.Errorf("Wrong inserted: got %d, want %d", result.EntriesInserted, eI)
	}
	if err = bm.OpenBTree(btreeName); err != nil {
		t.Fatalf("Re-open fail: %v", err)
	}
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("NewBTree fail: %v", err)
	}
	expected := map[uint64]uint64{1: 100, 2: 200, 3: 300, 5: 500, 10: 1000}
	for k, eV := range expected {
		v, f := tree.Lookup(k)
		if !f {
			t.Errorf("Lookup(%d) fail: not found", k)
		} else if v != eV {
			t.Errorf("Lookup(%d) wrong: got %d, want %d", k, v, eV)
		}
	}
	_, f := tree.Lookup(999)
	if f {
		t.Errorf("Lookup(999) found non-existent")
	}
}

func TestPersistence(t *testing.T) {
	tempDir := createTempDir(t)
	btreeName := "persistence_test"
	runPhase := func(phase int, action func(*testing.T, btree.BTree)) {
		bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
		if err != nil {
			t.Fatalf("P%d BM fail: %v", phase, err)
		}
		defer func() {
			if err := bm.CloseBTree(btreeName); err != nil {
				t.Logf("P%d Defer close fail: %v", phase, err)
			}
		}()
		if phase == 1 {
			if err = bm.CreateBTree(btreeName); err != nil {
				t.Fatalf("P%d CreateBTree fail: %v", phase, err)
			}
		} else {
			if err = bm.OpenBTree(btreeName); err != nil {
				t.Fatalf("P%d OpenBTree fail: %v", phase, err)
			}
		}
		tree, err := btree.NewBTreePersistent(bm, btreeName)
		if err != nil {
			t.Fatalf("P%d NewBTree fail: %v", phase, err)
		}
		action(t, tree)
	}
	runPhase(1, func(t *testing.T, tree btree.BTree) {
		for i := 1; i <= 10; i++ {
			if err := tree.Insert(uint64(i), uint64(i*100)); err != nil {
				t.Errorf("P1 Insert(%d) fail: %v", i, err)
			}
		}
	})
	runPhase(2, func(t *testing.T, tree btree.BTree) {
		for i := 1; i <= 10; i++ {
			v, f := tree.Lookup(uint64(i))
			if !f {
				t.Errorf("P2 Lookup(%d) fail", i)
			} else if v != uint64(i*100) {
				t.Errorf("P2 Lookup(%d) wrong: %d", i, v)
			}
		}
		for i := 11; i <= 20; i++ {
			if err := tree.Insert(uint64(i), uint64(i*100)); err != nil {
				t.Errorf("P2 Insert(%d) fail: %v", i, err)
			}
		}
	})
	runPhase(3, func(t *testing.T, tree btree.BTree) {
		for i := 1; i <= 20; i++ {
			v, f := tree.Lookup(uint64(i))
			if !f {
				t.Errorf("P3 Lookup(%d) fail", i)
			} else if v != uint64(i*100) {
				t.Errorf("P3 Lookup(%d) wrong: %d", i, v)
			}
		}
		values, err := tree.Scan(5, 15)
		if err != nil {
			t.Errorf("P3 Scan(5, 15) fail: %v", err)
		}
		expected := []uint64{}
		for i := 5; i <= 15; i++ {
			expected = append(expected, uint64(i*100))
		}
		if len(values) != 11 {
			t.Errorf("P3 Scan count: %d!=11", len(values))
		}
		if !reflect.DeepEqual(values, expected) {
			t.Errorf("P3 Scan wrong values")
		}
	})
}

func TestDeleteBasic(t *testing.T) {
	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "delete_basic_test"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() { bm.CloseBTree(btreeName) })
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}
	keys := []uint64{10, 5, 15, 3, 8, 12, 18, 20, 1, 25}
	values := make(map[uint64]uint64)
	for _, k := range keys {
		values[k] = k * 10
		if err := tree.Insert(k, values[k]); err != nil {
			t.Fatalf("Insert(%d) fail: %v", k, err)
		}
	}
	del1 := uint64(8)
	delete(values, del1)
	err = tree.Delete(del1)
	if err != nil {
		t.Errorf("Delete(%d) fail: %v", del1, err)
	}
	_, f := tree.Lookup(del1)
	if f {
		t.Errorf("Lookup(%d) found post-del", del1)
	}
	del2 := uint64(18)
	delete(values, del2)
	err = tree.Delete(del2)
	if err != nil {
		t.Errorf("Delete(%d) fail: %v", del2, err)
	}
	_, f = tree.Lookup(del2)
	if f {
		t.Errorf("Lookup(%d) found post-del", del2)
	}
	for k, vExp := range values {
		vAct, fL := tree.Lookup(k)
		if !fL {
			t.Errorf("Lookup(%d) fail post-del", k)
		} else if vAct != vExp {
			t.Errorf("Lookup(%d) wrong val %d post-del, want %d", k, vAct, vExp)
		}
	}
	scanExp := []uint64{}
	for _, v := range values {
		scanExp = append(scanExp, v)
	}
	sort.Slice(scanExp, func(i, j int) bool { return scanExp[i] < scanExp[j] })
	scanAct, sErr := tree.Scan(0, 1000)
	if sErr != nil {
		t.Fatalf("Scan fail post-del: %v", sErr)
	}
	sort.Slice(scanAct, func(i, j int) bool { return scanAct[i] < scanAct[j] })
	if !reflect.DeepEqual(scanAct, scanExp) {
		t.Errorf("Scan mismatch post-del:\nGot: %v\nWant: %v", scanAct, scanExp)
	}
}

func TestDeleteNotFound(t *testing.T) {
	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "delete_notfound_test"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() { bm.CloseBTree(btreeName) })
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}
	if err := tree.Insert(10, 100); err != nil {
		t.Fatalf("Insert(10) fail: %v", err)
	}
	if err := tree.Insert(20, 200); err != nil {
		t.Fatalf("Insert(20) fail: %v", err)
	}
	keyDel := uint64(15)
	err = tree.Delete(keyDel)
	if !errors.Is(err, btree.ErrKeyNotFound) {
		t.Errorf("Delete non-exist %d: expect ErrKeyNotFound, got %v", keyDel, err)
	}
	v, f := tree.Lookup(10)
	if !f || v != 100 {
		t.Errorf("Lookup(10) fail/wrong post non-exist del")
	}
	v, f = tree.Lookup(20)
	if !f || v != 200 {
		t.Errorf("Lookup(20) fail/wrong post non-exist del")
	}
}

func TestDeleteWithMerge(t *testing.T) {
	if page.MaxLeafKeys != 255 || page.MinLeafKeys != 128 {
		t.Skipf("Skip merge: req MaxL=255, MinL=128 (got %d, %d)", page.MaxLeafKeys, page.MinLeafKeys)
	}
	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "delete_merge_test"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() { bm.CloseBTree(btreeName) })
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}
	numSplit := page.MaxLeafKeys + 1
	values := make(map[uint64]uint64)
	for i := 1; i <= numSplit; i++ {
		k := uint64(i)
		v := k * 10
		values[k] = v
		if err := tree.Insert(k, v); err != nil {
			t.Fatalf("Insert(%d) setup fail: %v", k, err)
		}
	}
	keyDel := uint64(1)
	delete(values, keyDel)
	err = tree.Delete(keyDel)
	if err != nil {
		t.Fatalf("Delete(%d) fail, expect merge: %v", keyDel, err)
	}
	_, found := tree.Lookup(keyDel)
	if found {
		t.Errorf("Lookup(%d) found post-merge", keyDel)
	}
	sepKey := uint64(page.MinLeafKeys + 1)
	vExp := sepKey * 10
	vAct, found := tree.Lookup(sepKey)
	if !found {
		t.Errorf("Lookup(%d) separator key miss post-merge", sepKey)
	} else if vAct != vExp {
		t.Errorf("Lookup(%d) separator wrong val: got %d, want %d", sepKey, vAct, vExp)
	}
	scanExp := []uint64{}
	for i := 2; i <= numSplit; i++ {
		scanExp = append(scanExp, uint64(i*10))
	}
	scanAct, sErr := tree.Scan(0, uint64(numSplit+100))
	if sErr != nil {
		t.Fatalf("Scan fail post-merge: %v", sErr)
	}
	sort.Slice(scanAct, func(i, j int) bool { return scanAct[i] < scanAct[j] })
	if len(scanAct) != len(scanExp) {
		t.Errorf("Scan post-merge wrong count: got %d, want %d", len(scanAct), len(scanExp))
	} else if !reflect.DeepEqual(scanAct, scanExp) {
		t.Errorf("Scan post-merge mismatch:\nGot: %v\nWant: %v", scanAct, scanExp)
	}
}

func TestDeleteWithRedistribute(t *testing.T) {
	if page.MaxLeafKeys != 255 || page.MinLeafKeys != 128 {
		t.Skipf("Skip redist: req MaxL=255, MinL=128 (got %d, %d)", page.MaxLeafKeys, page.MinLeafKeys)
	}
	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(50))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "delete_redist_test"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() { bm.CloseBTree(btreeName) })
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}
	numInsert := page.MaxLeafKeys + 2
	values := make(map[uint64]uint64)
	for i := 1; i <= numInsert; i++ {
		k := uint64(i)
		v := k * 10
		values[k] = v
		if err := tree.Insert(k, v); err != nil {
			t.Fatalf("Insert(%d) setup fail: %v", k, err)
		}
	}
	keyDel := uint64(1)
	delete(values, keyDel)
	err = tree.Delete(keyDel)
	if err != nil {
		t.Fatalf("Delete(%d) fail, expect redist: %v", keyDel, err)
	}
	_, found := tree.Lookup(keyDel)
	if found {
		t.Errorf("Lookup(%d) found post-redist", keyDel)
	}
	borrowKey := uint64(page.MinLeafKeys + 1)
	vExp := borrowKey * 10
	vAct, found := tree.Lookup(borrowKey)
	if !found {
		t.Errorf("Lookup(%d) borrow key miss post-redist", borrowKey)
	} else if vAct != vExp {
		t.Errorf("Lookup(%d) borrow key wrong val: got %d, want %d", borrowKey, vAct, vExp)
	}
	scanExp := []uint64{}
	for i := 2; i <= numInsert; i++ {
		scanExp = append(scanExp, uint64(i*10))
	}
	scanAct, sErr := tree.Scan(0, uint64(numInsert+100))
	if sErr != nil {
		t.Fatalf("Scan fail post-redist: %v", sErr)
	}
	sort.Slice(scanAct, func(i, j int) bool { return scanAct[i] < scanAct[j] })
	if len(scanAct) != len(scanExp) {
		t.Errorf("Scan post-redist wrong count: got %d, want %d", len(scanAct), len(scanExp))
	} else if !reflect.DeepEqual(scanAct, scanExp) {
		t.Errorf("Scan post-redist mismatch:\nGot: %v\nWant: %v", scanAct, scanExp)
	}
}

func TestDeleteInternalNodeMerge(t *testing.T) {

	numKeysForInternalTest := (page.MinInternalKeys + 1 + page.MinInternalKeys + 1) * page.MinLeafKeys // (127+1 + 127+1) * 127 = 256 * 127 = 32512
	bufferSizeForLargeTest := 2000

	if page.MaxLeafKeys != 255 || page.MinLeafKeys != 127 || page.MaxInternalKeys != 255 || page.MinInternalKeys != 127 {
		t.Skip("Skip internal merge: test assumes default keys (MaxL=255, MinL=127, MaxI=255, MinI=127)")
	}

	t.Logf("Setting up for internal merge test with %d keys...", numKeysForInternalTest)

	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(bufferSizeForLargeTest))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "delete_int_merge_test"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() { bm.CloseBTree(btreeName) })
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}

	valueMap := make(map[uint64]uint64)
	for i := 1; i <= numKeysForInternalTest; i++ {
		k := uint64(i)
		v := k * 10
		valueMap[k] = v
		if err := tree.Insert(k, v); err != nil {
			t.Fatalf("Insert(%d) setup fail: %v", k, err)
		}
		if i%5000 == 0 {
			t.Logf("Inserted %d keys...", i)
		}
	}
	t.Logf("Setup complete. Performing delete.")

	keyToDelete := uint64(1)
	delete(valueMap, keyToDelete)
	err = tree.Delete(keyToDelete)
	if err != nil {
		t.Fatalf("Delete(%d) failed, expected internal merge/rebalance: %v", keyToDelete, err)
	}
	t.Logf("Delete complete. Verifying...")

	_, found := tree.Lookup(keyToDelete)
	if found {
		t.Errorf("Lookup(%d) found key after internal merge scenario", keyToDelete)
	}

	expectedScanValues := []uint64{}
	for _, v := range valueMap {
		expectedScanValues = append(expectedScanValues, v)
	}
	sort.Slice(expectedScanValues, func(i, j int) bool { return expectedScanValues[i] < expectedScanValues[j] })
	actualScanValues, scanErr := tree.Scan(0, uint64(numKeysForInternalTest+100))
	if scanErr != nil {
		t.Fatalf("Scan failed after internal merge scenario: %v", scanErr)
	}
	sort.Slice(actualScanValues, func(i, j int) bool { return actualScanValues[i] < actualScanValues[j] })

	if len(actualScanValues) != len(expectedScanValues) {
		t.Errorf("Scan post-internal merge wrong count: got %d, want %d", len(actualScanValues), len(expectedScanValues))
	} else if !reflect.DeepEqual(actualScanValues, expectedScanValues) {
		t.Errorf("Scan post-internal merge mismatch (counts match but content differs)")
	}
	t.Logf("Verification complete.")
}

func TestDeleteInternalNodeRedistribute(t *testing.T) {

	numKeysForInternalTest := (page.MinInternalKeys + 1 + page.MinInternalKeys + 2) * page.MinLeafKeys // (127+1 + 127+2) * 127 = 257 * 127 = 32639
	bufferSizeForLargeTest := 2000

	if page.MaxLeafKeys != 255 || page.MinLeafKeys != 127 || page.MaxInternalKeys != 255 || page.MinInternalKeys != 127 {
		t.Skip("Skip internal redist: test assumes default keys (MaxL=255, MinL=127, MaxI=255, MinI=127)")
	}

	t.Logf("Setting up for internal redistribute test with %d keys...", numKeysForInternalTest)

	tempDir := createTempDir(t)
	bm, err := buffer.NewBufferManager(buffer.WithDirectory(tempDir), buffer.WithBufferSize(bufferSizeForLargeTest))
	if err != nil {
		t.Fatalf("Failed BM init: %v", err)
	}
	btreeName := "delete_int_redist_test"
	if err := bm.CreateBTree(btreeName); err != nil {
		t.Fatalf("Failed CreateBTree: %v", err)
	}
	t.Cleanup(func() { bm.CloseBTree(btreeName) })
	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		t.Fatalf("Failed NewBTree: %v", err)
	}

	valueMap := make(map[uint64]uint64)
	for i := 1; i <= numKeysForInternalTest; i++ {
		k := uint64(i)
		v := k * 10
		valueMap[k] = v
		if err := tree.Insert(k, v); err != nil {
			t.Fatalf("Insert(%d) setup fail: %v", k, err)
		}
		if i%5000 == 0 {
			t.Logf("Inserted %d keys...", i)
		}
	}
	t.Logf("Setup complete. Performing delete.")

	keyToDelete := uint64(1)
	delete(valueMap, keyToDelete)
	err = tree.Delete(keyToDelete)
	if err != nil {
		t.Fatalf("Delete(%d) failed, expected internal redistribute: %v", keyToDelete, err)
	}
	t.Logf("Delete complete. Verifying...")

	_, found := tree.Lookup(keyToDelete)
	if found {
		t.Errorf("Lookup(%d) found key after internal redistribute scenario", keyToDelete)
	}

	expectedScanValues := []uint64{}
	for _, v := range valueMap {
		expectedScanValues = append(expectedScanValues, v)
	}
	sort.Slice(expectedScanValues, func(i, j int) bool { return expectedScanValues[i] < expectedScanValues[j] })
	actualScanValues, scanErr := tree.Scan(0, uint64(numKeysForInternalTest+100))
	if scanErr != nil {
		t.Fatalf("Scan failed after internal redistribute scenario: %v", scanErr)
	}
	sort.Slice(actualScanValues, func(i, j int) bool { return actualScanValues[i] < actualScanValues[j] })

	if len(actualScanValues) != len(expectedScanValues) {
		t.Errorf("Scan post-internal redistribute wrong count: got %d, want %d", len(actualScanValues), len(expectedScanValues))
	} else if !reflect.DeepEqual(actualScanValues, expectedScanValues) {
		t.Errorf("Scan post-internal redistribute mismatch (counts match but content differs)")
	}
	t.Logf("Verification complete.")
}

func TestMain(m *testing.M) {
	fmt.Println("Running simplified MiniB+Tree Store tests...")
	exitCode := m.Run()
	fmt.Println("Tests completed.")
	os.Exit(exitCode)
}
