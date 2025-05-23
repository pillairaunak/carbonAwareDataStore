package visualizer

import (
	"encoding/json"
	"log"
	"minibtreestore/carbonaware"
	"minibtreestore/storage/buffer" // Assuming BufferManager interface and concrete type are accessible
	"net/http"
	"time"
	// For reading atomic counters from BufferManagerImpl if not exposed via interface
)

// VisualizerStatus defines the structure of the data sent to the frontend.
type VisualizerStatus struct {
	CarbonSignal       carbonaware.IntensitySignal `json:"carbonSignal"`
	DeferredQueueSize  int32                       `json:"deferredQueueSize"`
	FlushedNormal      int32                       `json:"flushedNormal"`
	FlushedDeferred    int32                       `json:"flushedDeferred"`
	FlushedForced      int32                       `json:"flushedForced"`
	TotalPagesInBuffer int                         `json:"totalPagesInBuffer"` // Example: if BufferManager can provide this
	DirtyPagesInBuffer int                         `json:"dirtyPagesInBuffer"` // Example: if BufferManager can provide this
	CurrentBufferSize  int                         `json:"currentBufferSize"`
	ConfigCarbonAware  bool                        `json:"configCarbonAware"`
	ConfigCarbonRegion string                      `json:"configCarbonRegion"`
}

// statusHandler holds a reference to the BufferManager to fetch stats.
type statusHandler struct {
	// We need the concrete type to access the specific stat methods we added.
	// If BufferManager interface exposed these, we could use the interface.
	bm *buffer.BufferManagerImpl
}

// ServeHTTP handles requests to the /status endpoint.
func (sh *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if sh.bm == nil {
		http.Error(w, "BufferManager not initialized for visualizer", http.StatusInternalServerError)
		return
	}

	signal, sigOk := sh.bm.GetLastKnownIntensitySignal()
	if !sigOk {
		// Handle case where signal might not be ready (e.g. if provider hasn't run yet)
		// For now, send a default or potentially an error/specific status
		signal = carbonaware.IntensitySignal{Region: sh.bm.GetConfig().CarbonRegion, IsLow: true, Value: -1, Timestamp: time.Now()}
	}

	cfg := sh.bm.GetConfig() // Assuming a GetConfig() method on BufferManagerImpl

	payload := VisualizerStatus{
		CarbonSignal:       signal,
		DeferredQueueSize:  sh.bm.GetDeferredPagesCount(),
		FlushedNormal:      sh.bm.GetFlushedNormalCount(),
		FlushedDeferred:    sh.bm.GetFlushedDeferredCount(),
		FlushedForced:      sh.bm.GetFlushedForcedCount(),
		TotalPagesInBuffer: sh.bm.GetCurrentPageMapSize(),     // Assuming method GetCurrentPageMapSize() exists
		DirtyPagesInBuffer: sh.bm.GetCurrentDirtyPagesCount(), // Assuming method GetCurrentDirtyPagesCount() exists
		CurrentBufferSize:  cfg.BufferSize,
		ConfigCarbonAware:  cfg.CarbonAware,
		ConfigCarbonRegion: cfg.CarbonRegion,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") // For easy local development with frontend
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("Visualizer: Error encoding status to JSON: %v", err)
		http.Error(w, "Error encoding status", http.StatusInternalServerError)
	}
}

// StartVisualizerServer sets up the HTTP routes and starts the server in a new goroutine.
// It expects the concrete *buffer.BufferManagerImpl type to access specific stat methods.
func StartVisualizerServer(bm *buffer.BufferManagerImpl, port string) {
	if bm == nil {
		log.Println("Visualizer: BufferManager instance is nil, cannot start server.")
		return
	}
	if port == "" {
		log.Println("Visualizer: Port not specified, visualizer server will not start.")
		return
	}

	handler := &statusHandler{bm: bm}
	http.Handle("/status", handler)

	log.Printf("Visualizer: Starting server on port %s (e.g., http://localhost%s/status)", port, port)
	go func() {
		if err := http.ListenAndServe(port, nil); err != nil {
			log.Printf("Visualizer: Server error: %v", err)
		}
	}()
}

// Note: For the VisualizerStatus to get TotalPagesInBuffer and DirtyPagesInBuffer,
// you would need to add corresponding methods to BufferManagerImpl:
// e.g., GetCurrentPageMapSize() and GetCurrentDirtyPagesCount()
//
// func (bm *BufferManagerImpl) GetCurrentPageMapSize() int {
//     bm.poolMutex.RLock()
//     defer bm.poolMutex.RUnlock()
//     return len(bm.pageMap)
// }
//
// func (bm *BufferManagerImpl) GetCurrentDirtyPagesCount() int {
//     bm.poolMutex.RLock()
//     defer bm.poolMutex.RUnlock()
//     count := 0
//     for _, frameIdx := range bm.pageMap { // This iterates values (frame indices)
//         // To check IsDirty, we need the frame itself
//         // This approach to get dirty count might be slow or need rework
//         // A simpler way is to iterate bm.frames and check if in pageMap and dirty
//         // Or maintain an atomic counter for dirty pages if high accuracy and performance are needed here
//     }
//      // A better way for DirtyPagesInBuffer might be to iterate frames:
//      // count := 0
//      // for _, frame := range bm.frames {
//      //    if frame.PageID != 0 && frame.IsDirty() { // PageID != 0 means it's in use
//      //        count++
//      //    }
//      // }
//     return count
// }
//
// func (bm *BufferManagerImpl) GetConfig() buffer.BufferManagerConfig {
//    return bm.config // Expose config
// }
// These accessor methods should be added to your buffermanager_impl.go
