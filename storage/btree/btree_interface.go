package btree

import (
	"errors"
)

var (
	ErrNodeFull    = errors.New("node is full")
	ErrKeyNotFound = errors.New("key not found in node")
	ErrInvalidPage = errors.New("invalid page data or buffer size")
	ErrTreeNotInit = errors.New("btree root is not initialized")
)

const (
	RootPageID uint64 = 1
)

type BTree interface {
	Lookup(key uint64) (value uint64, found bool)
	Insert(key uint64, value uint64) error
	Scan(minKey uint64, maxKey uint64) ([]uint64, error)
	Delete(key uint64) error
}
