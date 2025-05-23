package buffer

import (
	"fmt"
	"io"
	"log"
	"minibtreestore/carbonaware" // Already added in Phase 2
	"minibtreestore/storage/page"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic" // Already added in Phase 2
	"time"
)

const defaultPageSize = page.MaxPageSize

type BufferPoolEntry struct {
	BTreeID string
	PageID  PageID
}

type PageFrame struct {
	Data       []byte
	PageID     PageID
	BTreeID    string
	PinCount   int
	Dirty      bool
	LastAccess time.Time
	sync.RWMutex
}

type PageReplacementPolicy interface {
	Initialize(bufferSize int)
	RecordAccess(frameIndex int) // Called when a page in frame is accessed
	FindVictim(frames []*PageFrame) int
}

type LRUReplacementPolicy struct{}

func NewPageFrame(pageSize int) *PageFrame {
	return &PageFrame{
		Data:       make([]byte, pageSize),
		PageID:     0,
		BTreeID:    "",
		PinCount:   0,
		Dirty:      false,
		LastAccess: time.Time{},
	}
}

func (pf *PageFrame) Pin() {
	pf.Lock()
	defer pf.Unlock()
	pf.PinCount++
	pf.LastAccess = time.Now()
}

func (pf *PageFrame) Unpin(dirty bool) bool {
	pf.Lock()
	defer pf.Unlock()
	if pf.PinCount <= 0 {
		return false
	}
	pf.PinCount--
	if dirty {
		pf.Dirty = true
	}
	pf.LastAccess = time.Now()
	return true
}

func (pf *PageFrame) IsPinned() bool {
	pf.RLock()
	defer pf.RUnlock()
	return pf.PinCount > 0
}

func (pf *PageFrame) IsDirty() bool {
	pf.RLock()
	defer pf.RUnlock()
	return pf.Dirty
}

func (pf *PageFrame) ClearDirty() {
	pf.Lock()
	defer pf.Unlock()
	pf.Dirty = false
}

func (pf *PageFrame) GetLastAccess() time.Time {
	pf.RLock()
	defer pf.RUnlock()
	return pf.LastAccess
}

func (lru *LRUReplacementPolicy) Initialize(bufferSize int) {}

func (lru *LRUReplacementPolicy) RecordAccess(frameIndex int) {} // Frame was accessed

func (lru *LRUReplacementPolicy) FindVictim(frames []*PageFrame) int {
	victimIndex := -1
	var oldestTime time.Time
	firstUnpinnedFound := false

	for i, frame := range frames {
		if frame.IsPinned() { // Cannot evict a pinned page
			continue
		}
		if frame.PageID == 0 {
			continue
		}

		frameAccessTime := frame.GetLastAccess()
		if !firstUnpinnedFound {
			oldestTime = frameAccessTime
			victimIndex = i
			firstUnpinnedFound = true
		} else if frameAccessTime.Before(oldestTime) {
			oldestTime = frameAccessTime
			victimIndex = i
		}
	}
	return victimIndex
}

type BufferManagerImpl struct {
	config            BufferManagerConfig
	frames            []*PageFrame
	pageMap           map[BufferPoolEntry]int
	replacementPolicy PageReplacementPolicy
	btreeFiles        map[string]*os.File
	nextPageIDs       map[string]PageID
	freePages         map[string][]PageID
	metaMutex         sync.RWMutex
	poolMutex         sync.RWMutex

	intensityProvider       carbonaware.IntensityProvider
	deferredFlushQueue      []BufferPoolEntry
	deferredFlushLock       sync.Mutex
	lastKnownIntensityValue atomic.Value
	deferredPagesCount      int32
	flushedNormalCount      int32
	flushedDeferredCount    int32
	flushedForcedCount      int32
	shutdownChan            chan struct{}
}

func NewBufferManager(options ...Option) (*BufferManagerImpl, error) {
	config := BufferManagerConfig{
		Directory:              ".",
		BufferSize:             1024,
		CarbonAware:            false,
		CarbonRegion:           "default-region",
		CarbonAPIEndpoint:      "",               // Default: no API endpoint
		CarbonAPIClientTimeout: 10 * time.Second, // Default API client timeout
		DeferredFlushInterval:  1 * time.Minute,
		VisualizerPort:         "",
	}
	for _, option := range options {
		option(&config)
	}

	if config.BufferSize <= 0 {
		return nil, fmt.Errorf("buffer size must be positive")
	}
	if config.Directory == "" {
		return nil, fmt.Errorf("storage directory cannot be empty")
	}

	bm := &BufferManagerImpl{
		config:             config,
		frames:             make([]*PageFrame, config.BufferSize),
		pageMap:            make(map[BufferPoolEntry]int),
		btreeFiles:         make(map[string]*os.File),
		nextPageIDs:        make(map[string]PageID),
		freePages:          make(map[string][]PageID),
		replacementPolicy:  &LRUReplacementPolicy{},
		deferredFlushQueue: make([]BufferPoolEntry, 0),
		shutdownChan:       make(chan struct{}),
	}

	bm.lastKnownIntensityValue.Store(carbonaware.IntensitySignal{IsLow: true, Value: 0, Timestamp: time.Now()})
	atomic.StoreInt32(&bm.deferredPagesCount, 0)
	atomic.StoreInt32(&bm.flushedNormalCount, 0)
	atomic.StoreInt32(&bm.flushedDeferredCount, 0)
	atomic.StoreInt32(&bm.flushedForcedCount, 0)

	if config.CarbonAware {
		log.Printf("BufferManager: Carbon-aware mode ENABLED for region '%s'. Interval: %v\n",
			config.CarbonRegion, config.DeferredFlushInterval)

		if config.CarbonAPIEndpoint != "" {
			log.Printf("BufferManager: Attempting to use APIIntensityProvider with endpoint: %s", config.CarbonAPIEndpoint)
			provider, err := carbonaware.NewAPIIntensityProvider(config.CarbonAPIEndpoint, config.CarbonRegion, config.CarbonAPIClientTimeout)
			if err != nil {
				log.Printf("BufferManager: WARN - Failed to create APIIntensityProvider (endpoint: %s): %v. Falling back to MockIntensityProvider.", config.CarbonAPIEndpoint, err)
				bm.intensityProvider = carbonaware.NewMockIntensityProvider(config.CarbonRegion)
			} else {
				log.Printf("BufferManager: Successfully initialized APIIntensityProvider for region '%s'. Endpoint: %s", config.CarbonRegion, config.CarbonAPIEndpoint)
				bm.intensityProvider = provider
			}
		} else {
			log.Println("BufferManager: CarbonAPIEndpoint not configured. Using MockIntensityProvider.")
			bm.intensityProvider = carbonaware.NewMockIntensityProvider(config.CarbonRegion)
		}
		go bm.processDeferredFlushes()
	} else {
		log.Println("BufferManager: Carbon-aware mode DISABLED")
	}

	for i := 0; i < config.BufferSize; i++ {
		bm.frames[i] = NewPageFrame(defaultPageSize)
	}
	bm.replacementPolicy.Initialize(config.BufferSize)

	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory '%s': %w", config.Directory, err)
	}
	return bm, nil
}

// Helper for logging based on current intensity (Phase 3)
func (bm *BufferManagerImpl) getCurrentIntensityEmoji() string {
	if !bm.config.CarbonAware || bm.intensityProvider == nil {
		return "N/A ⚫"
	}
	var currentSignalToUse carbonaware.IntensitySignal
	storedSignal, ok := bm.lastKnownIntensityValue.Load().(carbonaware.IntensitySignal)

	// If stored signal is very old (e.g., older than half the deferred flush interval, or a fixed threshold like 30s),
	// consider it stale and try to fetch a fresh one.
	isStale := !ok || storedSignal.Timestamp.IsZero() || time.Since(storedSignal.Timestamp) > (bm.config.DeferredFlushInterval/2)
	if !ok && bm.config.DeferredFlushInterval == 0 { // if watchdog is disabled, use a fixed staleness threshold
		isStale = !ok || storedSignal.Timestamp.IsZero() || time.Since(storedSignal.Timestamp) > 30*time.Second
	}

	if isStale {
		freshSignal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
		if err == nil {
			currentSignalToUse = freshSignal
			bm.lastKnownIntensityValue.Store(freshSignal) // Keep it updated
		} else {
			log.Printf("BufferManager: Error fetching fresh intensity for emoji: %v. Using stored/default.", err)
			if ok && !storedSignal.Timestamp.IsZero() { // Use stale stored if fresh fetch failed and stored is somewhat valid
				currentSignalToUse = storedSignal
			} else { // Very default if everything fails
				return "ERR ❓"
			}
		}
	} else {
		currentSignalToUse = storedSignal
	}

	if currentSignalToUse.Value == 0 && !currentSignalToUse.Timestamp.IsZero() && !isStale { // Check if it's a valid zero or uninitialized
		// If value is 0 but timestamp is recent, it might be a valid reading.
		// The IsLow flag should be authoritative.
	}

	if currentSignalToUse.IsLow {
		return fmt.Sprintf("LOW 🟢 (%.0f)", currentSignalToUse.Value)
	}
	return fmt.Sprintf("HIGH 🔴 (%.0f)", currentSignalToUse.Value)
}

// GetConfig returns the current buffer manager configuration.
func (bm *BufferManagerImpl) GetConfig() BufferManagerConfig {
	return bm.config
}

// GetIntensityProvider returns the configured intensity provider.
// Useful for testing or external control of a mock provider.
func (bm *BufferManagerImpl) GetIntensityProvider() carbonaware.IntensityProvider {
	return bm.intensityProvider
}

// Public accessors for stats (Phase 2)
func (bm *BufferManagerImpl) GetLastKnownIntensitySignal() (carbonaware.IntensitySignal, bool) {
	signal, ok := bm.lastKnownIntensityValue.Load().(carbonaware.IntensitySignal)
	return signal, ok
}
func (bm *BufferManagerImpl) GetDeferredPagesCount() int32 {
	return atomic.LoadInt32(&bm.deferredPagesCount)
}
func (bm *BufferManagerImpl) GetFlushedNormalCount() int32 {
	return atomic.LoadInt32(&bm.flushedNormalCount)
}
func (bm *BufferManagerImpl) GetFlushedDeferredCount() int32 {
	return atomic.LoadInt32(&bm.flushedDeferredCount)
}
func (bm *BufferManagerImpl) GetFlushedForcedCount() int32 {
	return atomic.LoadInt32(&bm.flushedForcedCount)
}

// GetCurrentPageMapSize returns the number of pages currently in the buffer pool's page map.
func (bm *BufferManagerImpl) GetCurrentPageMapSize() int {
	bm.poolMutex.RLock()
	defer bm.poolMutex.RUnlock()
	return len(bm.pageMap)
}

// GetCurrentDirtyPagesCount returns the number of dirty pages currently in the buffer pool.
func (bm *BufferManagerImpl) GetCurrentDirtyPagesCount() int {
	bm.poolMutex.RLock()
	defer bm.poolMutex.RUnlock()
	count := 0
	// Iterate through the frames, as pageMap only gives indices.
	// A page is considered if it's active (PageID != 0) and dirty.
	for _, frame := range bm.frames {
		// A frame is in use by a page if its PageID is not the zero value.
		// The IsDirty method on the frame tells its state.
		if frame.PageID != 0 && frame.IsDirty() {
			count++
		}
	}
	return count
}

// New method for Phase 4: processDeferredFlushes (background goroutine)
// In storage/buffer/buffermanager_impl.go

func (bm *BufferManagerImpl) processDeferredFlushes() {
	if bm.config.DeferredFlushInterval <= 0 {
		log.Println("[CarbonWatchdog] Deferred flush interval is not positive, watchdog will not run.")
		return
	}
	ticker := time.NewTicker(bm.config.DeferredFlushInterval)
	defer ticker.Stop()

	log.Println("[CarbonWatchdog] Started: Watching deferred flush queue and updating intensity.")

	for {
		select {
		case <-ticker.C:
			if bm.intensityProvider == nil || !bm.config.CarbonAware {
				continue
			}

			// --- MODIFICATION START ---
			// Always fetch current intensity to update lastKnownIntensityValue and for dashboard freshness
			// This will ensure APIIntensityProvider's log is triggered.
			currentSignal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
			if err != nil {
				log.Printf("[CarbonWatchdog] Error getting carbon intensity: %v. Will retry next tick.", err)
				// Optionally, decide if you want to proceed with an old signal or just wait
				continue // Skip further processing this tick if intensity fetch fails
			}
			bm.lastKnownIntensityValue.Store(currentSignal)
			log.Printf("[CarbonWatchdog] Intensity updated. Provider: %T, IsLow: %v, Value: %.2f. Ref Intensity for logs: %s",
				bm.intensityProvider, currentSignal.IsLow, currentSignal.Value, bm.getCurrentIntensityEmoji())
			// --- MODIFICATION END ---

			currentDeferredCount := atomic.LoadInt32(&bm.deferredPagesCount)
			if currentDeferredCount == 0 {
				// log.Println("[CarbonWatchdog] Deferred queue is empty. No pages to flush.")
				continue // No pages in the queue to flush
			}

			// Now, currentSignal holds the freshly fetched (or very recently fetched) intensity.
			// The bm.getCurrentIntensityEmoji() used below will also reflect this recent fetch.
			log.Printf("[CarbonWatchdog] Checking deferred queue (Size: %d). Current Intensity (from update): %s\n",
				currentDeferredCount, bm.getCurrentIntensityEmoji()) // Emoji now uses the fresh value

			if currentSignal.IsLow {
				log.Printf("[CarbonWatchdog] Intensity is LOW 🟢 (Value: %.2f). Attempting to flush deferred queue.\n", currentSignal.Value)
				bm.flushDeferredQueue(false) // forceFlush = false
			} else {
				log.Printf("[CarbonWatchdog] Intensity is HIGH 🔴 (Value: %.2f). Deferring flush of queue.\n", currentSignal.Value)
			}

		case <-bm.shutdownChan:
			log.Println("[CarbonWatchdog] Shutting down.")
			return
		}
	}
}

func (bm *BufferManagerImpl) flushDeferredQueue(forceFlush bool, specificBTreeID ...string) {
	bm.deferredFlushLock.Lock() // Lock at the beginning

	if len(bm.deferredFlushQueue) == 0 {
		bm.deferredFlushLock.Unlock() // Unlock if returning early
		return
	}

	targetBTreeID := ""
	if len(specificBTreeID) > 0 && specificBTreeID[0] != "" {
		targetBTreeID = specificBTreeID[0]
		log.Printf("BufferManager: Flushing deferred queue specifically for BTree '%s' (Forced: %v, Queue size before: %d)\n", targetBTreeID, forceFlush, len(bm.deferredFlushQueue))
	} else {
		log.Printf("BufferManager: Flushing entire deferred queue (Forced: %v, Queue size before: %d)\n", forceFlush, len(bm.deferredFlushQueue))
	}

	remainingQueue := make([]BufferPoolEntry, 0, len(bm.deferredFlushQueue))
	actuallyFlushedCount := 0
	reDeferredCount := 0

	for _, entry := range bm.deferredFlushQueue {
		if targetBTreeID != "" && entry.BTreeID != targetBTreeID {
			remainingQueue = append(remainingQueue, entry) // Keep if not for the target BTree
			continue
		}

		bm.poolMutex.RLock() // RLock for checking pageMap and frame state
		frameIdx, pageExistsInPool := bm.pageMap[entry]
		var frame *PageFrame
		if pageExistsInPool {
			frame = bm.frames[frameIdx]
		}
		bm.poolMutex.RUnlock()

		if pageExistsInPool && frame.IsDirty() && frame.BTreeID == entry.BTreeID && frame.PageID == entry.PageID {
			// flushPage_nolock will handle carbon check if not forceFlush.
			// It returns nil if successfully flushed OR successfully re-deferred.
			// It returns an error only on actual I/O error.
			// It takes the frameIdx as an argument.
			err := bm.flushPage_nolock(entry.BTreeID, entry.PageID, frameIdx, forceFlush)
			if err != nil {
				log.Printf(" carboneffectERROR ❌ Error flushing deferred page %s:%d: %v. Will retry later.", entry.BTreeID, entry.PageID, err)
				remainingQueue = append(remainingQueue, entry) // Keep in queue to retry
			} else {
				// If flushPage_nolock returned nil, it either flushed or re-deferred.
				// We need to check if it's still dirty to know if it was *actually* flushed.
				bm.poolMutex.RLock() // Re-check dirty status under lock
				isStillDirtyAfterAttempt := bm.frames[frameIdx].IsDirty()
				bm.poolMutex.RUnlock()

				if !isStillDirtyAfterAttempt { // Page was actually flushed
					actuallyFlushedCount++
					atomic.AddInt32(&bm.flushedDeferredCount, 1)
					atomic.AddInt32(&bm.deferredPagesCount, -1) // Item removed from effective deferred state
					log.Printf(" carboneffectDEFERRED_FLUSH ➡️💾 Page %s:%d flushed from deferred queue (Forced: %v). Deferred Count Now: %d",
						entry.BTreeID, entry.PageID, forceFlush, atomic.LoadInt32(&bm.deferredPagesCount))
				} else { // Page was re-deferred by flushPage_nolock (carbon still high, not forced)
					reDeferredCount++
					remainingQueue = append(remainingQueue, entry) // Keep in queue as it's still effectively deferred
					// deferredPagesCount is not decremented as it's still in deferred state.
					log.Printf(" carboneffectRE_DEFERRED 🔄 Page %s:%d re-deferred (Carbon still high). Deferred Count: %d",
						entry.BTreeID, entry.PageID, atomic.LoadInt32(&bm.deferredPagesCount))
				}
			}
		} else { // Page no longer in buffer, or not dirty, or mismatched
			atomic.AddInt32(&bm.deferredPagesCount, -1) // Decrement if it's being removed from consideration
			log.Printf("BufferManager: Page %s:%d from deferred queue no longer needs active flushing (not found/dirty in pool). Deferred Count Now: %d",
				entry.BTreeID, entry.PageID, atomic.LoadInt32(&bm.deferredPagesCount))
		}
	}

	bm.deferredFlushQueue = remainingQueue
	bm.deferredFlushLock.Unlock() // Unlock after modifying shared queue

	if actuallyFlushedCount > 0 {
		log.Printf("BufferManager: Actually flushed %d pages from deferred queue processing.\n", actuallyFlushedCount)
	}
	if reDeferredCount > 0 {
		log.Printf("BufferManager: Re-deferred %d pages during deferred queue processing.\n", reDeferredCount)
	}

	// Final consistency check for the counter
	bm.deferredFlushLock.Lock()
	currentQueueLen := len(bm.deferredFlushQueue)
	bm.deferredFlushLock.Unlock()
	if int32(currentQueueLen) != atomic.LoadInt32(&bm.deferredPagesCount) {
		log.Printf("WARN: Deferred queue length (%d) and counter (%d) mismatch after processing! Correcting counter.", currentQueueLen, atomic.LoadInt32(&bm.deferredPagesCount))
		atomic.StoreInt32(&bm.deferredPagesCount, int32(currentQueueLen))
	}
}

// flushPage_nolock: Modified in Phase 3 for carbon awareness.
// Takes frameIdx as an argument.
func (bm *BufferManagerImpl) flushPage_nolock(btreeID string, pageID PageID, frameIdx int, forceFlush bool) error {
	// poolMutex should NOT be held here. This function assumes frameIdx is valid.
	// The caller (PinPage, flushAllDirtyPages, flushDeferredQueue) handles poolMutex.
	// This function might acquire metaMutex (RLock) and deferredFlushLock.

	frame := bm.frames[frameIdx] // Assumes frameIdx is valid

	// Lock the frame for reading its dirty status and potentially writing to it (clearing dirty)
	// However, IsDirty() and ClearDirty() have their own locks.
	// We need to ensure that the decision to flush and the act of flushing/clearing dirty are somewhat atomic
	// with respect to the dirty flag itself. The frame's internal lock handles this.

	if !frame.IsDirty() {
		return nil // Not dirty, nothing to do
	}

	if bm.config.CarbonAware && bm.intensityProvider != nil && !forceFlush {
		signal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
		if err != nil {
			// Log error but proceed with flush as a fallback (conservative approach).
			// Or, could choose to defer if intensity is unknown. For now, log and proceed.
			log.Printf("BufferManager: Error getting carbon intensity for %s:%d (Frame %d): %v. Proceeding with flush.", btreeID, pageID, frameIdx, err)
			// Update lastKnownIntensityValue with an error state or keep it as is?
			// For now, we don't update lastKnownIntensityValue on error here, processDeferredFlushes does.
		} else {
			bm.lastKnownIntensityValue.Store(signal) // Update on successful fetch
			if !signal.IsLow {
				// Carbon intensity is high, defer the flush.
				bm.deferredFlushLock.Lock() // Lock before accessing deferredFlushQueue
				// Check if already in queue to avoid duplicates
				inQueue := false
				for _, entry := range bm.deferredFlushQueue {
					if entry.BTreeID == btreeID && entry.PageID == pageID {
						inQueue = true
						break
					}
				}
				if !inQueue {
					bm.deferredFlushQueue = append(bm.deferredFlushQueue, BufferPoolEntry{BTreeID: btreeID, PageID: pageID})
					atomic.AddInt32(&bm.deferredPagesCount, 1) // Increment count of pages in deferred state
				}
				bm.deferredFlushLock.Unlock()
				log.Printf(" carboneffectDEFERRED 🟡 Page %s:%d (Frame: %d, Deferred Count: %d) - Carbon: %s",
					btreeID, pageID, frameIdx, atomic.LoadInt32(&bm.deferredPagesCount), bm.getCurrentIntensityEmoji())
				return nil // Successfully deferred, frame remains dirty
			}
			// Intensity is low, proceed to flush
			log.Printf(" carboneffectLOW_CARBON_FLUSH_ATTEMPT 🟢 Page %s:%d (Frame: %d) - Carbon: %s. Attempting flush.",
				btreeID, pageID, frameIdx, bm.getCurrentIntensityEmoji())
		}
	}

	// --- Perform the actual disk write ---
	bm.metaMutex.RLock() // Lock for reading btreeFiles map
	file, exists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock() // Unlock after map access

	if !exists {
		return fmt.Errorf("flush page internal error: BTree file for '%s' not found (Page: %d, Frame: %d)", btreeID, pageID, frameIdx)
	}

	// Frame data is accessed here. Frame's own RWMutex protects its Data field if needed,
	// but WriteAt reads it. PinCount should be 0 if we're evicting, but not necessarily if called from flushAll.
	// The critical part is that Data isn't modified while being written.
	// If PinCount > 0, another goroutine might be using it.
	// However, flushPage_nolock is usually called on unpinned victims or during controlled flushes (CloseBTree, Shutdown)
	// where concurrent modification of this specific page's data by its "owner" is less likely.
	// For a page being evicted (PinCount=0), its Data is safe to read.
	// For a page being flushed while potentially pinned (e.g. from flushAllDirtyPages not forced by eviction),
	// the data should represent a consistent state *before* this flush call.
	// The Dirty flag is cleared *after* successful write.

	offset := int64(pageID-1) * defaultPageSize
	// Read frame.Data under frame's RLock if there's concern about concurrent modification
	// For now, assume direct access is okay in contexts this is called.
	n, err := file.WriteAt(frame.Data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %s:%d (Frame: %d) to disk: %w", btreeID, pageID, frameIdx, err)
	}
	if n != defaultPageSize {
		return fmt.Errorf("incomplete write for page %s:%d (Frame: %d): wrote %d bytes, expected %d", btreeID, pageID, frameIdx, n, defaultPageSize)
	}

	frame.ClearDirty() // Clears the dirty flag *after* successful write. Frame handles its own lock for this.

	// Logging and stats update
	if forceFlush {
		atomic.AddInt32(&bm.flushedForcedCount, 1)
		log.Printf(" carboneffectFLUSHING ❗ Page %s:%d (Frame: %d, Forced) - Carbon: %s",
			btreeID, pageID, frameIdx, bm.getCurrentIntensityEmoji())
	} else if bm.config.CarbonAware && bm.intensityProvider != nil {
		// This path implies carbon was low, or provider errored and we defaulted to flush
		atomic.AddInt32(&bm.flushedNormalCount, 1)
		log.Printf(" carboneffectFLUSHING 🟢 Page %s:%d (Frame: %d, Normal/Low Carbon) - Carbon: %s",
			btreeID, pageID, frameIdx, bm.getCurrentIntensityEmoji())
	} else { // Carbon-aware disabled
		atomic.AddInt32(&bm.flushedNormalCount, 1)
		log.Printf(" carboneffectFLUSHING 💾 Page %s:%d (Frame: %d, Carbon-Aware Disabled)",
			btreeID, pageID, frameIdx)
	}
	return nil
}

func (bm *BufferManagerImpl) PinPage(btreeID string, pageID PageID) (Page, error) {
	bm.metaMutex.RLock()
	_, btreeExists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()
	if !btreeExists {
		return Page{}, fmt.Errorf("pin failed: unknown btree '%s'", btreeID)
	}
	if pageID == 0 {
		return Page{}, fmt.Errorf("pin failed: invalid page ID 0 for BTree '%s'", btreeID)
	}

	entry := BufferPoolEntry{BTreeID: btreeID, PageID: pageID}

	bm.poolMutex.Lock() // Single lock for the whole PinPage operation

	// 1. Check if page is already in buffer
	if frameIdx, exists := bm.pageMap[entry]; exists {
		frame := bm.frames[frameIdx]
		frame.Pin()
		bm.replacementPolicy.RecordAccess(frameIdx)
		bm.poolMutex.Unlock()
		return Page{Data: frame.Data}, nil
	}

	// 2. Page not in buffer, try to find a frame.
	// Loop to handle deferred victims.
	maxAttempts := bm.config.BufferSize + 1 // Allow one attempt per frame plus one
	for attempts := 0; attempts < maxAttempts; attempts++ {
		targetFrameIdx := bm.findFreeFrame_nolock()

		if targetFrameIdx != -1 { // Found a completely free (unused) frame
			frame := bm.frames[targetFrameIdx]
			// frame is reset (newly created or from a prior non-dirty victim)
			// No need to clear dirty or BTreeID/PageID as it's a fresh assignment.

			if err := bm.readPage_nolock(btreeID, pageID, frame.Data); err != nil {
				// bm.frames[targetFrameIdx] = NewPageFrame(defaultPageSize) // Reset on error - already clean
				bm.poolMutex.Unlock()
				return Page{}, fmt.Errorf("PinPage: read new page %s:%d into free frame %d failed: %w", btreeID, pageID, targetFrameIdx, err)
			}
			frame.BTreeID = btreeID
			frame.PageID = pageID
			frame.PinCount = 1 // Set pin count to 1
			frame.Dirty = false
			frame.LastAccess = time.Now()
			bm.pageMap[entry] = targetFrameIdx
			bm.replacementPolicy.RecordAccess(targetFrameIdx)
			bm.poolMutex.Unlock()
			return Page{Data: frame.Data}, nil
		}

		// No free frames, try to evict a victim
		victimFrameIdx := bm.replacementPolicy.FindVictim(bm.frames)
		if victimFrameIdx == -1 { // All pages are pinned, no victim found
			bm.poolMutex.Unlock()
			return Page{}, ErrBufferFull
		}

		victimFrame := bm.frames[victimFrameIdx]
		// Find victim's BufferPoolEntry using victimFrameIdx
		var victimEntry BufferPoolEntry
		foundVictimInMap := false
		for e, idx := range bm.pageMap {
			if idx == victimFrameIdx {
				victimEntry = e
				foundVictimInMap = true
				break
			}
		}
		if !foundVictimInMap { // Should not happen with correct logic
			bm.poolMutex.Unlock()
			return Page{}, fmt.Errorf("internal error: PinPage victim frame %d not found in pageMap", victimFrameIdx)
		}

		// At this point, victimFrame is unpinned (guaranteed by FindVictim)
		// Now, process the victim frame.
		if victimFrame.IsDirty() {
			// Try to flush the victim. Pass forceFlush = false.
			// flushPage_nolock needs poolMutex *unlocked* if it's to try and get other locks.
			// But flushPage_nolock is designed to be called with poolMutex *not* held by itself.
			// Here, we hold poolMutex. This is tricky.
			// For now, let's assume flushPage_nolock can be called.
			// If flushPage_nolock defers, the victimFrame remains Dirty.
			errFlush := bm.flushPage_nolock(victimEntry.BTreeID, victimEntry.PageID, victimFrameIdx, false) // false = not forced

			if errFlush != nil { // Actual I/O error during flush attempt
				// Releasing poolMutex here might be complex if we want to retry.
				// For now, propagate error. The buffer pool might be in a transiently tricky state.
				bm.poolMutex.Unlock()
				return Page{}, fmt.Errorf("PinPage: failed to flush victim page %s:%d for eviction: %w", victimEntry.BTreeID, victimEntry.PageID, errFlush)
			}

			// After calling flushPage_nolock, check if it's still dirty (meaning it was deferred)
			if victimFrame.IsDirty() {
				log.Printf("BufferManager: PinPage: Victim %s:%d (Frame %d) flush deferred. Frame remains in use. Attempt: %d. Seeking another victim.",
					victimEntry.BTreeID, victimEntry.PageID, victimFrameIdx, attempts+1)
				// The frame is still "occupied" by the deferred page.
				// Mark it as accessed so LRU might pick something else next time if possible.
				bm.replacementPolicy.RecordAccess(victimFrameIdx) // Make it less likely to be picked immediately again
				// Continue the loop to find another victim or a free frame.
				if attempts == maxAttempts-1 { // Last attempt, about to fail
					log.Printf("BufferManager: PinPage: Max attempts reached to find frame for %s:%d due to deferrals/pins.", btreeID, pageID)
				}
				continue // Try to find another frame in the next iteration
			}
		}

		// If we reach here, the victimFrame was either clean or successfully flushed (not deferred).
		// It's now available for reuse.
		delete(bm.pageMap, victimEntry) // Remove old page from map
		// The victimFrame itself is reused (bm.frames[victimFrameIdx])

		targetFrameIdx = victimFrameIdx // This is the frame we'll use

		// Load the requested page into this now-available frame
		frameToLoad := bm.frames[targetFrameIdx] // This is victimFrame
		if err := bm.readPage_nolock(btreeID, pageID, frameToLoad.Data); err != nil {
			// If read fails, what to do? The victim was evicted.
			// Reset the frame to a clean state.
			bm.frames[targetFrameIdx] = NewPageFrame(defaultPageSize)
			bm.poolMutex.Unlock()
			return Page{}, fmt.Errorf("PinPage: read new page %s:%d into victim frame %d failed: %w", btreeID, pageID, targetFrameIdx, err)
		}
		frameToLoad.BTreeID = btreeID
		frameToLoad.PageID = pageID
		frameToLoad.PinCount = 1
		frameToLoad.Dirty = false
		frameToLoad.LastAccess = time.Now()
		bm.pageMap[entry] = targetFrameIdx                // Map new entry to this frame index
		bm.replacementPolicy.RecordAccess(targetFrameIdx) // Record access for the new page
		bm.poolMutex.Unlock()
		return Page{Data: frameToLoad.Data}, nil
	}

	// If loop finishes, all attempts to find a frame failed.
	bm.poolMutex.Unlock()
	log.Printf("BufferManager: PinPage for %s:%d failed after %d attempts. Buffer likely full of pinned/deferred pages.", btreeID, pageID, maxAttempts)
	return Page{}, ErrBufferFull // Buffer is full of pinned or unflushable (deferred) pages
}

func (bm *BufferManagerImpl) flushAllDirtyPages(btreeID string, forceFlush bool) error {
	log.Printf("BufferManager: Flushing all dirty pages for BTree '%s' (Forced: %v)\n", btreeID, forceFlush)

	// Collect candidates under RLock to minimize write lock duration on poolMutex
	bm.poolMutex.RLock()
	pagesToFlushInfo := []struct {
		entry    BufferPoolEntry
		frameIdx int
	}{}

	for entry, frameIdx := range bm.pageMap {
		// Check IsDirty() under RLock for safety, though IsDirty() has its own internal lock.
		// This ensures we're iterating a consistent view of pageMap.
		if entry.BTreeID == btreeID && bm.frames[frameIdx].IsDirty() {
			pagesToFlushInfo = append(pagesToFlushInfo, struct {
				entry    BufferPoolEntry
				frameIdx int
			}{entry, frameIdx})
		}
	}
	bm.poolMutex.RUnlock()

	if len(pagesToFlushInfo) == 0 {
		log.Printf("BufferManager: No dirty pages found for BTree '%s' to flush (Forced: %v)\n", btreeID, forceFlush)
		return nil
	}

	var lastErr error
	flushedCount := 0
	deferredCount := 0

	for _, info := range pagesToFlushInfo {
		// Call flushPage_nolock. This function handles its own locking for frame data (dirty flag)
		// and for deferred queue access if needed. It does not take poolMutex.
		err := bm.flushPage_nolock(info.entry.BTreeID, info.entry.PageID, info.frameIdx, forceFlush)

		if err != nil { // This means an actual I/O error occurred
			log.Printf("Error flushing page %s:%d (Frame %d) during flushAll (Forced: %v): %v\n", info.entry.BTreeID, info.entry.PageID, info.frameIdx, forceFlush, err)
			lastErr = err // Keep track of the last error
		} else {
			// If err is nil, it was either flushed or (if !forceFlush) potentially re-deferred.
			// We need to check IsDirty again to see what happened.
			// This check needs to be safe w.r.t concurrent changes to the frame's dirty status.
			// Frame's IsDirty() method is internally synchronized.
			frameIsStillDirty := bm.frames[info.frameIdx].IsDirty() // Re-check under frame's lock

			if !frameIsStillDirty {
				flushedCount++
			} else if !forceFlush { // If still dirty AND not forced, it was likely re-deferred
				deferredCount++
			}
			// If forceFlush was true and it's still dirty, something went wrong, but flushPage_nolock should have errored.
		}
	}

	logMsg := fmt.Sprintf("BufferManager: flushAllDirtyPages for BTree '%s' (Forced: %v) summary: %d actually flushed", btreeID, forceFlush, flushedCount)
	if deferredCount > 0 {
		logMsg += fmt.Sprintf(", %d re-deferred", deferredCount)
	}
	if lastErr != nil {
		logMsg += fmt.Sprintf(". Last error: %v", lastErr)
	}
	log.Println(logMsg)

	return lastErr
}

func (bm *BufferManagerImpl) CloseBTree(name string) error {
	log.Printf("BufferManager: Attempting to close BTree '%s'\n", name)
	bm.metaMutex.RLock()
	_, btreeFileExists := bm.btreeFiles[name]
	bm.metaMutex.RUnlock()

	if !btreeFileExists {
		log.Printf("BufferManager: BTree '%s' not found or already closed (initial check).\n", name)
		return ErrBTreeNotFound
	}

	// Phase 3: Force flush all pages in buffer pool for this BTree.
	// Pass forceFlush = true to ensure they are written out, overriding carbon deferral.
	if err := bm.flushAllDirtyPages(name, true); err != nil {
		// Log warning but proceed with close if flushing had I/O errors.
		// Pages might remain dirty in buffer if not for this btree, or if error was specific.
		log.Printf("Warning: force flushAllDirtyPages failed during close of '%s': %v. Proceeding with close.\n", name, err)
	}

	// Phase 4: Force flush any pages for this BTree from the deferred queue.
	if bm.config.CarbonAware { // Only if carbon aware features are active
		log.Printf("BufferManager: Force flushing deferred queue for BTree '%s' during close.\n", name)
		bm.flushDeferredQueue(true, name) // forceFlush = true, specific BTree
	}

	// Now, remove pages of this BTree from the buffer pool if they are not pinned.
	bm.poolMutex.Lock() // Lock for modifying pageMap and frames
	var pinnedPages []PageID
	pagesToRemoveFromMap := []BufferPoolEntry{} // Collect entries to delete from pageMap

	for entry, frameIdx := range bm.pageMap {
		if entry.BTreeID == name {
			if bm.frames[frameIdx].IsPinned() {
				pinnedPages = append(pinnedPages, entry.PageID)
			} else {
				// Page is not pinned, mark for removal from buffer.
				// It should have been flushed by flushAllDirtyPages(name, true) if dirty.
				pagesToRemoveFromMap = append(pagesToRemoveFromMap, entry)
			}
		}
	}

	if len(pinnedPages) > 0 {
		bm.poolMutex.Unlock() // Unlock before returning error
		return fmt.Errorf("cannot close BTree '%s': pages %v are still pinned", name, pinnedPages)
	}

	// Remove unpinned pages from pageMap and reset their frames
	for _, entry := range pagesToRemoveFromMap {
		frameIdx := bm.pageMap[entry] // Get index again, map could change if not careful (but we hold lock)
		delete(bm.pageMap, entry)
		bm.frames[frameIdx] = NewPageFrame(defaultPageSize) // Reset the frame
	}
	bm.poolMutex.Unlock() // Unlock after modifying pageMap and frames

	// Finally, close the file and remove BTree metadata under metaMutex
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()

	file, stillExistsInMeta := bm.btreeFiles[name]
	if !stillExistsInMeta {
		// This might happen if another goroutine closed/deleted it between the initial check and now.
		log.Printf("BufferManager: BTree '%s' was removed from meta map by another operation before final lock during close.\n", name)
		return ErrBTreeNotFound // Or nil, if this state is acceptable.
	}

	err := file.Close()
	// Clean up metadata regardless of file close error (though if file.Close() fails, OS might still hold it)
	delete(bm.btreeFiles, name)
	delete(bm.nextPageIDs, name)
	delete(bm.freePages, name)

	if err != nil {
		return fmt.Errorf("failed to close file for BTree '%s': %w", name, err)
	}
	log.Printf("BufferManager: Closed BTree '%s'\n", name)
	return nil
}

// Shutdown gracefully stops background processes (New for Phase 4)
func (bm *BufferManagerImpl) Shutdown() {
	log.Println("BufferManager: Shutdown initiated.")
	if bm.config.CarbonAware && bm.shutdownChan != nil {
		// Check if channel is already closed to prevent panic
		// This requires a select or a more complex mechanism if Shutdown can be called multiple times.
		// For simplicity, assume it's called once.
		close(bm.shutdownChan)
	}

	// Iterate all open btrees and try to close them.
	// Need to collect names first to avoid issues with map iteration while modifying (CloseBTree modifies btreeFiles)
	bm.metaMutex.RLock()
	openBTrees := make([]string, 0, len(bm.btreeFiles))
	for name := range bm.btreeFiles {
		openBTrees = append(openBTrees, name)
	}
	bm.metaMutex.RUnlock()

	log.Printf("BufferManager: %d BTrees to close during shutdown: %v", len(openBTrees), openBTrees)
	for _, name := range openBTrees {
		log.Printf("BufferManager: Closing BTree '%s' during shutdown.\n", name)
		if err := bm.CloseBTree(name); err != nil {
			// Log error but continue shutting down other B-trees.
			log.Printf("BufferManager: Error closing BTree '%s' during shutdown: %v\n", name, err)
		}
	}
	log.Println("BufferManager: Shutdown complete.")
}

// --- Unchanged methods from original provided code for Phase 3/4 scope ---
// CreateBTree, OpenBTree, DeleteBTree, UnpinPage, AllocatePage, FreePage,
// findFreeFrame_nolock, readPage_nolock

func (bm *BufferManagerImpl) CreateBTree(name string) error {
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()

	if _, exists := bm.btreeFiles[name]; exists {
		return fmt.Errorf("btree '%s' already open in buffer manager", name)
	}
	if name == "" {
		return fmt.Errorf("btree name cannot be empty")
	}

	filePath := filepath.Join(bm.config.Directory, name)
	if _, err := os.Stat(filePath); err == nil {
		return fmt.Errorf("file '%s' already exists on disk", filePath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking status of '%s': %w", filePath, err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file '%s': %w", filePath, err)
	}

	if err := file.Truncate(defaultPageSize); err != nil {
		file.Close()
		os.Remove(filePath)
		return fmt.Errorf("failed to truncate new btree file '%s' to initial size: %w", name, err)
	}

	bm.btreeFiles[name] = file
	bm.nextPageIDs[name] = 2 // Page 1 is root, so next allocatable is 2
	bm.freePages[name] = []PageID{}
	log.Printf("BufferManager: Created BTree file '%s' and truncated to initial size.\n", name)
	return nil
}

func (bm *BufferManagerImpl) OpenBTree(name string) error {
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()

	if _, exists := bm.btreeFiles[name]; exists {
		return nil // Already open
	}
	if name == "" {
		return fmt.Errorf("btree name cannot be empty")
	}

	filePath := filepath.Join(bm.config.Directory, name)
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrBTreeNotFound
		}
		return fmt.Errorf("open '%s': %w", name, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("stat '%s': %w", name, err)
	}

	bm.btreeFiles[name] = file

	if fileInfo.Size() == 0 {
		// If file is empty, it implies it might be a new BTree or corrupted.
		// The root page (PageID 1) should exist.
		// For now, assume nextPageID starts from 1 if empty, btree logic will initialize root.
		bm.nextPageIDs[name] = 1
	} else {
		bm.nextPageIDs[name] = PageID(fileInfo.Size()/defaultPageSize) + 1
	}

	if fileInfo.Size() > 0 && fileInfo.Size()%defaultPageSize != 0 {
		log.Printf("Warning: File '%s' size %d not a perfect multiple of page size %d\n", name, fileInfo.Size(), defaultPageSize)
		// Consider how to handle this. Potentially truncate to last full page or error.
	}
	bm.freePages[name] = []PageID{} // Initialize free pages list for this BTree
	log.Printf("BufferManager: Opened BTree '%s', next page ID currently estimated as: %d\n", name, bm.nextPageIDs[name])
	return nil
}

func (bm *BufferManagerImpl) DeleteBTree(name string) error {
	log.Printf("BufferManager: Attempting to delete BTree '%s'\n", name)
	// CloseBTree handles flushing and removing from buffer pool.
	// It's important CloseBTree is robust.
	if err := bm.CloseBTree(name); err != nil && err != ErrBTreeNotFound {
		// If CloseBTree fails for reasons other than "not found" (e.g. pages pinned),
		// we should not proceed with deletion to avoid data loss or inconsistent state.
		return fmt.Errorf("delete BTree '%s' failed during pre-close: %w", name, err)
	}

	// MetaMutex for filePath construction and os.Remove, and for cleaning maps
	// if CloseBTree didn't (e.g., if it was ErrBTreeNotFound path).
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()

	filePath := filepath.Join(bm.config.Directory, name)

	// Ensure metadata is cleaned up even if CloseBTree was called and handled some of it.
	// This also handles the case where the BTree was not open (ErrBTreeNotFound from CloseBTree).
	delete(bm.btreeFiles, name)
	delete(bm.nextPageIDs, name)
	delete(bm.freePages, name)

	log.Printf("BufferManager: Removing file '%s'\n", filePath)
	errRemove := os.Remove(filePath)
	if errRemove != nil && !os.IsNotExist(errRemove) {
		// Log error but metadata is already cleaned up.
		// The BTree is "deleted" from the BufferManager's perspective.
		log.Printf("BufferManager: Failed to delete file '%s': %v. Metadata cleaned.", name, errRemove)
		return fmt.Errorf("failed to delete file '%s': %w", name, errRemove)
	}
	log.Printf("BufferManager: Deleted BTree '%s'\n", name)
	return nil
}

func (bm *BufferManagerImpl) UnpinPage(btreeID string, pageID PageID, isDirty bool) error {
	if pageID == 0 {
		return fmt.Errorf("unpin failed: invalid page ID 0")
	}
	entry := BufferPoolEntry{BTreeID: btreeID, PageID: pageID}

	bm.poolMutex.Lock()
	defer bm.poolMutex.Unlock()

	frameIdx, exists := bm.pageMap[entry]
	if !exists {
		return fmt.Errorf("unpin failed: page %s:%d not found in buffer pool", btreeID, pageID)
	}

	frame := bm.frames[frameIdx]
	if !frame.Unpin(isDirty) {
		// This typically means PinCount was already 0.
		return fmt.Errorf("unpin failed for page %s:%d: either not pinned or pin count is zero", btreeID, pageID)
	}
	// bm.replacementPolicy.RecordAccess(frameIdx) // Unpinning is also an access, keeps it "fresh" if immediately repinned.
	// Or, maybe not for LRU - unpinning means it's a candidate for eviction sooner if not repinned.
	// Current LRU logic in FindVictim uses frame.GetLastAccess(), which Unpin updates. So it's fine.
	return nil
}

func (bm *BufferManagerImpl) AllocatePage(btreeID string) (PageID, error) {
	bm.metaMutex.Lock() // Lock for nextPageIDs, freePages, and file operations
	defer bm.metaMutex.Unlock()

	file, exists := bm.btreeFiles[btreeID]
	if !exists {
		return 0, fmt.Errorf("allocate page failed: BTree '%s' not found or not open", btreeID)
	}

	var pageID PageID
	allocatedFromFreeList := false

	freeList := bm.freePages[btreeID]
	if len(freeList) > 0 {
		pageID = freeList[len(freeList)-1]                 // Get last page from free list
		bm.freePages[btreeID] = freeList[:len(freeList)-1] // Remove it
		allocatedFromFreeList = true
		log.Printf("BufferManager: Allocated page %d for BTree '%s' from free list.", pageID, btreeID)
	} else {
		pageID = bm.nextPageIDs[btreeID]
		if pageID == 0 { // Should ideally be initialized to 1 or more
			pageID = 1
		}
		bm.nextPageIDs[btreeID] = pageID + 1
		log.Printf("BufferManager: Allocated new page %d for BTree '%s'. Next will be %d.", pageID, btreeID, bm.nextPageIDs[btreeID])
	}

	// Ensure the file is large enough for this new page, especially if not from free list
	// or if free list pages were at the end and file was somehow truncated.
	if !allocatedFromFreeList {
		requiredSize := int64(pageID) * defaultPageSize // PageID is 1-based
		// Stat file to check current size
		fileInfo, err := file.Stat()
		if err != nil {
			// Rollback nextPageID if allocation fails
			if bm.nextPageIDs[btreeID] > pageID && pageID > 0 { // if it was incremented
				bm.nextPageIDs[btreeID]--
			} else if pageID > 0 { // if it was set from 0 to 1
				bm.nextPageIDs[btreeID] = pageID
			}
			return 0, fmt.Errorf("failed to stat file for BTree '%s' during page allocation: %w", btreeID, err)
		}

		if fileInfo.Size() < requiredSize {
			if err := file.Truncate(requiredSize); err != nil {
				// Rollback nextPageID
				if bm.nextPageIDs[btreeID] > pageID && pageID > 0 {
					bm.nextPageIDs[btreeID]--
				} else if pageID > 0 {
					bm.nextPageIDs[btreeID] = pageID
				}
				return 0, fmt.Errorf("failed to truncate file for BTree '%s' to size %d for new page %d: %w", btreeID, requiredSize, pageID, err)
			}
			log.Printf("BufferManager: Truncated file '%s' to %d bytes for page %d.", btreeID, requiredSize, pageID)
		}
	}
	// For pages from free list, assume file was already correct size for them.
	// If a page from free list is used, its physical space is zeroed out by caller typically by writing new data.
	return pageID, nil
}

func (bm *BufferManagerImpl) FreePage(btreeID string, pageID PageID) error {
	if pageID == 0 {
		return fmt.Errorf("free page failed: invalid page ID 0 for BTree '%s'", btreeID)
	}

	bm.metaMutex.RLock() // Check if BTree is known
	_, btreeExists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()
	if !btreeExists {
		return fmt.Errorf("free page failed: BTree '%s' not found or not open", btreeID)
	}

	entry := BufferPoolEntry{BTreeID: btreeID, PageID: pageID}

	// Evict from buffer pool if present
	bm.poolMutex.Lock() // Lock for pageMap and frames modification
	if frameIdx, exists := bm.pageMap[entry]; exists {
		frame := bm.frames[frameIdx]
		if frame.IsPinned() {
			bm.poolMutex.Unlock()
			return ErrPagePinned // Cannot free a pinned page from buffer
		}
		// Page is in buffer and not pinned. Remove it.
		delete(bm.pageMap, entry)
		bm.frames[frameIdx] = NewPageFrame(defaultPageSize) // Reset the frame
		log.Printf("BufferManager: Page %s:%d evicted from buffer pool during FreePage.", btreeID, pageID)
	}
	bm.poolMutex.Unlock()

	// Add to BTree's free list (under metaMutex for freePages map)
	bm.metaMutex.Lock()
	// Re-check btree still exists in meta in case of concurrent delete
	if _, stillExists := bm.btreeFiles[btreeID]; stillExists {
		// Add to free list if not already there (to prevent duplicates)
		isAlreadyFree := false
		for _, freeID := range bm.freePages[btreeID] {
			if freeID == pageID {
				isAlreadyFree = true
				break
			}
		}
		if !isAlreadyFree {
			bm.freePages[btreeID] = append(bm.freePages[btreeID], pageID)
			log.Printf("BufferManager: Page %d added to free list for BTree '%s'.", pageID, btreeID)
		} else {
			log.Printf("BufferManager: Page %d was already in free list for BTree '%s'.", pageID, btreeID)
		}
	} else {
		log.Printf("BufferManager: BTree '%s' no longer exists in meta during FreePage %d. Cannot add to free list.", btreeID, pageID)
	}
	bm.metaMutex.Unlock()

	// Note: Actual disk space is not reclaimed here, just marked as free for reuse.
	return nil
}

// findFreeFrame_nolock finds an unused frame in the buffer pool.
// It assumes poolMutex is held by the caller.
func (bm *BufferManagerImpl) findFreeFrame_nolock() int {
	for i, frame := range bm.frames {
		// A frame is free if its PageID is 0 (or some other sentinel for "unused")
		// AND it's not pinned (PinCount should be 0 if PageID is 0, but check anyway).
		if frame.PageID == 0 && !frame.IsPinned() { // PageID == 0 indicates frame is not holding any page
			return i
		}
	}
	return -1 // No free frame found
}

// readPage_nolock reads a page from disk into the provided buffer.
// It assumes metaMutex (RLock) is held by the caller if btreeFiles map is accessed.
// Here, it's designed to be called after metaMutex is RLocked.
func (bm *BufferManagerImpl) readPage_nolock(btreeID string, pageID PageID, buffer []byte) error {
	if len(buffer) != defaultPageSize {
		return fmt.Errorf("internal read error: provided buffer size %d is incorrect, expected %d", len(buffer), defaultPageSize)
	}

	// Caller should hold metaMutex.RLock()
	file, exists := bm.btreeFiles[btreeID] // Assumes caller holds necessary lock for bm.btreeFiles
	if !exists {
		// This implies a logic error if metaMutex was not held or BTree was closed concurrently.
		return fmt.Errorf("read page internal error: BTree file for '%s' not found (metaMutex RLock should be held by caller)", btreeID)
	}

	offset := int64(pageID-1) * defaultPageSize // PageID is 1-based
	// Check file size before reading.
	// file.Stat() might be slow to call repeatedly.
	// However, ReadAt will return EOF or short read if offset is beyond file size.
	// Let's rely on ReadAt's error handling for out-of-bounds reads.

	n, err := file.ReadAt(buffer, offset)
	if err != nil {
		// If err is EOF and n < defaultPageSize, it's like a short read / page not fully there.
		// If err is EOF and n == 0, offset was beyond file end.
		// ReadAt returns io.EOF if n < len(buffer) but > 0.
		if err == io.EOF && n < defaultPageSize && n > 0 { // common case for reading a partially written or non-existent page at end
			return fmt.Errorf("short read for page %s:%d: read %d bytes, expected %d, potential EOF: %w", btreeID, pageID, n, defaultPageSize, ErrPageNotFound)
		} else if err == io.EOF && n == 0 { // reading beyond the end of the file
			return fmt.Errorf("page %s:%d not found on disk (read beyond EOF): %w", btreeID, pageID, ErrPageNotFound)
		}
		return fmt.Errorf("failed to read page %s:%d from disk (read %d bytes): %w", btreeID, pageID, n, err)
	}
	// ReadAt guarantees that if err is nil, n == len(buffer).
	// So, no need to check n == defaultPageSize if err is nil.

	return nil
}
