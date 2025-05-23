package buffer

import (
	"errors"
	"time"
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
}

type Option func(*BufferManagerConfig)

type BufferManagerConfig struct {
	Directory              string
	BufferSize             int
	CarbonAware            bool
	CarbonRegion           string
	CarbonAPIEndpoint      string        // URL for the carbon intensity API
	CarbonAPIClientTimeout time.Duration // Timeout for the carbon API client
	DeferredFlushInterval  time.Duration
	VisualizerPort         string
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

// New option for Carbon API Endpoint
func WithCarbonAPIEndpoint(url string) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonAPIEndpoint = url
	}
}

// New option for Carbon API Client Timeout
func WithCarbonAPIClientTimeout(timeout time.Duration) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonAPIClientTimeout = timeout
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
