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
	// GetConfig() BufferManagerConfig // Added in previous steps, if needed by external components directly
	// GetIntensityProvider() carbonaware.IntensityProvider // Added in previous steps
	// Other stat accessors like GetDeferredPagesCount(), etc.
}
type Option func(*BufferManagerConfig)
type BufferManagerConfig struct {
	Directory             string
	BufferSize            int
	CarbonAware           bool          // Retained from Phase 2: Enable/disable carbon-aware features globally
	CarbonRegion          string        // Retained from Phase 2: Region for carbon intensity data
	DeferredFlushInterval time.Duration // Retained from Phase 2: How often to check deferred queue
	VisualizerPort        string        // Retained from Phase 2: Port for the web visualizer backend
	// New fields for selecting and configuring the Carbon Intensity Provider (Phase 5 integration)
	CarbonProviderType          string        `json:"carbonProviderType"`          // e.g., "mock" or "api"
	CarbonIntensityApiUrl       string        `json:"carbonIntensityApiUrl"`       // URL for the API provider
	CarbonIntensityApiThreshold float64       `json:"carbonIntensityApiThreshold"` // Threshold for the API provider to determine IsLow
	CarbonIntensityApiTimeout   time.Duration `json:"carbonIntensityApiTimeout"`   // Timeout for API requests
}

// Existing functional options
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

// New functional options for API-based carbon intensity provider
func WithCarbonProviderType(providerType string) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonProviderType = providerType
	}
}
func WithCarbonIntensityApiUrl(apiUrl string) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonIntensityApiUrl = apiUrl
	}
}
func WithCarbonIntensityApiThreshold(threshold float64) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonIntensityApiThreshold = threshold
	}
}
func WithCarbonIntensityApiTimeout(timeout time.Duration) Option {
	return func(config *BufferManagerConfig) {
		config.CarbonIntensityApiTimeout = timeout
	}
}
