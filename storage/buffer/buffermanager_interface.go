package buffer

import (
	"errors"
	"time" // Added for DeferredFlushInterval
)

type Page struct {
	Data []byte
}

type PageID uint64

var (
	ErrBTreeNotFound = errors.New("btree not found or not open")
	ErrPageNotFound  = errors.New("page not found on disk or in buffer")
	ErrBufferFull    = errors.New("buffer pool is full and no page could be evicted")
	ErrPagePinned    = errors.New("page is pinned and cannot be evicted or freed")
)

type BufferManager interface {
	CreateBTree(name string) error
	OpenBTree(name string) error
	CloseBTree(name string) error
	DeleteBTree(name string) error

	PinPage(btreeID string, pageID PageID) (Page, error)
	UnpinPage(btreeID string, pageID PageID, isDirty bool) error
	AllocatePage(btreeID string) (PageID, error)
	FreePage(btreeID string, pageID PageID) error

	// GetStats returns current operational statistics, useful for the visualizer.
	// This is a new addition for Phase 2 to expose stats.
	// The concrete type of stats will be defined alongside the visualizer implementation.
	// For now, it's a placeholder for the concept.
	// GetStats() map[string]interface{} // Or a specific struct
}

type Option func(*BufferManagerConfig)

type BufferManagerConfig struct {
	Directory             string
	BufferSize            int
	CarbonAware           bool          // New: Enable/disable carbon-aware features
	CarbonRegion          string        // New: Region for carbon intensity data
	DeferredFlushInterval time.Duration // New: How often to check deferred queue
	VisualizerPort        string        // New: Port for the web visualizer backend
}

func WithDirectory(dir string) Option {
	return func(config *BufferManagerConfig) {
		config.Directory = dir
	}
}

func WithBufferSize(numPages int) Option {
	return func(config *BufferManagerConfig) {
		config.BufferSize = numPages
	}
}

// New functional options for carbon awareness and visualizer

func WithCarbonAware(enabled bool) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonAware = enabled
	}
}

func WithCarbonRegion(region string) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonRegion = region
	}
}

func WithDeferredFlushInterval(interval time.Duration) Option {
	return func(config *BufferManagerConfig) {
		config.DeferredFlushInterval = interval
	}
}

func WithVisualizerPort(port string) Option {
	return func(config *BufferManagerConfig) {
		config.VisualizerPort = port
	}
}
