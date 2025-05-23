package loader

import (
	"bufio"
	"errors"
	"fmt"
	"minibtreestore/storage/btree"
	"minibtreestore/storage/buffer"
	"os"
	"strconv"
	"strings"
)

type Result struct {
	EntriesProcessed int
	EntriesInserted  int
	Errors           []string
}

func LoadCSV(bm buffer.BufferManager, btreeName string, csvFilePath string) (*Result, error) {
	if bm == nil {
		return nil, fmt.Errorf("buffer manager cannot be nil")
	}
	if btreeName == "" {
		return nil, fmt.Errorf("btree name cannot be empty")
	}

	result := &Result{Errors: []string{}}

	err := bm.OpenBTree(btreeName)
	if err != nil {
		var errBTreeNotFound error
		if errors.As(err, &errBTreeNotFound) || strings.Contains(err.Error(), "not found") {
			fmt.Printf("Loader: BTree '%s' not found, attempting to create.\n", btreeName)
			createErr := bm.CreateBTree(btreeName)
			if createErr != nil {
				return result, fmt.Errorf("failed to create btree '%s': %w", btreeName, createErr)
			}

			openErr := bm.OpenBTree(btreeName)
			if openErr != nil {
				return result, fmt.Errorf("failed to open newly created btree '%s': %w", btreeName, openErr)
			}
		} else {
			return result, fmt.Errorf("failed to open btree '%s': %w", btreeName, err)
		}
	}

	tree, err := btree.NewBTreePersistent(bm, btreeName)
	if err != nil {
		return result, fmt.Errorf("failed to instantiate persistent btree '%s': %w", btreeName, err)
	}

	defer func() {
		fmt.Printf("Loader: Attempting to close BTree '%s' via Buffer Manager.\n", btreeName)
		closeErr := bm.CloseBTree(btreeName)
		if closeErr != nil {
			errMsg := fmt.Sprintf("Error closing BTree '%s' after loading: %v", btreeName, closeErr)
			fmt.Println(errMsg)
			result.Errors = append(result.Errors, errMsg)
			if err == nil {
				err = closeErr
			}
		} else {
			fmt.Printf("Loader: Successfully closed BTree '%s'.\n", btreeName)
		}
	}()

	file, err := os.Open(csvFilePath)
	if err != nil {
		return result, fmt.Errorf("failed to open CSV file '%s': %w", csvFilePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	fmt.Printf("Loader: Starting processing of CSV file '%s' for BTree '%s'.\n", csvFilePath, btreeName)

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		result.EntriesProcessed++

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			errMsg := fmt.Sprintf("line %d: invalid format (expected key,value), line: '%s'", lineNum, line)
			result.Errors = append(result.Errors, errMsg)
			continue
		}

		keyStr := strings.TrimSpace(parts[0])
		key, err := strconv.ParseUint(keyStr, 10, 64)
		if err != nil {
			errMsg := fmt.Sprintf("line %d: invalid key '%s': %v", lineNum, keyStr, err)
			result.Errors = append(result.Errors, errMsg)
			continue
		}

		valueStr := strings.TrimSpace(parts[1])
		value, err := strconv.ParseUint(valueStr, 10, 64)
		if err != nil {
			errMsg := fmt.Sprintf("line %d: invalid value '%s': %v", lineNum, valueStr, err)
			result.Errors = append(result.Errors, errMsg)
			continue
		}

		if insertErr := tree.Insert(key, value); insertErr != nil {
			errMsg := fmt.Sprintf("line %d: insert failed for key %d: %v", lineNum, key, insertErr)
			result.Errors = append(result.Errors, errMsg)
		} else {
			result.EntriesInserted++
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		errMsg := fmt.Sprintf("error reading CSV file '%s': %v", csvFilePath, scanErr)
		result.Errors = append(result.Errors, errMsg)
		if err == nil {
			err = scanErr
		}
	}

	fmt.Printf("Loader: Finished processing CSV. Processed: %d, Inserted: %d, Errors: %d\n",
		result.EntriesProcessed, result.EntriesInserted, len(result.Errors))

	return result, err
}
