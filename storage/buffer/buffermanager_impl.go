/*package buffer

import (
	"fmt"
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
	RecordAccess(frameIndex int)
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

func (lru *LRUReplacementPolicy) RecordAccess(frameIndex int) {}

func (lru *LRUReplacementPolicy) FindVictim(frames []*PageFrame) int {
	victimIndex := -1
	var oldestTime time.Time
	firstUnpinnedFound := false

	for i, frame := range frames {
		if frame.IsPinned() {
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

	// New for Phase 4: Channel to signal shutdown for background goroutines
	shutdownChan chan struct{}
}

func NewBufferManager(options ...Option) (*BufferManagerImpl, error) {
	config := BufferManagerConfig{
		Directory:             ".",
		BufferSize:            1024,
		CarbonAware:           false,
		CarbonRegion:          "default-region",
		DeferredFlushInterval: 1 * time.Minute,
		VisualizerPort:        "",
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
		shutdownChan:       make(chan struct{}), // New for Phase 4
	}

	bm.lastKnownIntensityValue.Store(carbonaware.IntensitySignal{IsLow: true, Value: 0})
	atomic.StoreInt32(&bm.deferredPagesCount, 0)
	atomic.StoreInt32(&bm.flushedNormalCount, 0)
	atomic.StoreInt32(&bm.flushedDeferredCount, 0)
	atomic.StoreInt32(&bm.flushedForcedCount, 0)

	if config.CarbonAware {
		bm.intensityProvider = carbonaware.NewMockIntensityProvider(config.CarbonRegion)
		log.Printf("BufferManager: Carbon-aware mode ENABLED for region '%s'. Interval: %v\n",
			config.CarbonRegion, config.DeferredFlushInterval)
		// Launch background processor for deferred flushes (Phase 4)
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
	signal, ok := bm.lastKnownIntensityValue.Load().(carbonaware.IntensitySignal)
	if !ok {
		return "N/A ⚫"
	}
	if signal.IsLow {
		return "LOW 🟢"
	}
	return "HIGH 🔴"
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

// New method for Phase 4: processDeferredFlushes (background goroutine)
func (bm *BufferManagerImpl) processDeferredFlushes() {
	if bm.config.DeferredFlushInterval <= 0 {
		log.Println("[CarbonWatchdog] Deferred flush interval is not positive, watchdog will not run.")
		return
	}
	ticker := time.NewTicker(bm.config.DeferredFlushInterval)
	defer ticker.Stop()

	log.Println("[CarbonWatchdog] Started: Watching deferred flush queue.")

	for {
		select {
		case <-ticker.C:
			if bm.intensityProvider == nil || !bm.config.CarbonAware {
				// log.Println("[CarbonWatchdog] Carbon provider not available or awareness disabled. Skipping check.")
				continue
			}
			currentDeferredCount := atomic.LoadInt32(&bm.deferredPagesCount)
			if currentDeferredCount == 0 {
				// log.Println("[CarbonWatchdog] Deferred queue is empty. Skipping check.")
				continue
			}

			log.Printf("[CarbonWatchdog] Checking deferred queue (Size: %d). Current Intensity: %s\n",
				currentDeferredCount, bm.getCurrentIntensityEmoji())

			signal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
			if err != nil {
				log.Printf("[CarbonWatchdog] Error getting carbon intensity: %v. Will retry later.", err)
				continue // Don't try to flush if we can't get intensity
			}
			bm.lastKnownIntensityValue.Store(signal) // Update last known intensity

			if signal.IsLow {
				log.Printf("[CarbonWatchdog] Intensity is LOW 🟢. Attempting to flush deferred queue.\n")
				bm.flushDeferredQueue(false) // forceFlush = false
			} else {
				log.Printf("[CarbonWatchdog] Intensity is HIGH 🔴. Deferring flush of queue.\n")
			}
		case <-bm.shutdownChan: // New for Phase 4: Handle shutdown
			log.Println("[CarbonWatchdog] Shutting down.")
			return
		}
	}
}

// New method for Phase 4: flushDeferredQueue
// forceFlush overrides carbon intensity check.
// specificBTreeID (optional) to only flush for a particular BTree (e.g., during CloseBTree).
func (bm *BufferManagerImpl) flushDeferredQueue(forceFlush bool, specificBTreeID ...string) {
	bm.deferredFlushLock.Lock()
	defer bm.deferredFlushLock.Unlock()

	if len(bm.deferredFlushQueue) == 0 {
		// log.Printf("BufferManager: Deferred queue is empty. Nothing to flush (Forced: %v)\n", forceFlush)
		return
	}

	targetBTreeID := ""
	if len(specificBTreeID) > 0 && specificBTreeID[0] != "" {
		targetBTreeID = specificBTreeID[0]
		log.Printf("BufferManager: Flushing deferred queue specifically for BTree '%s' (Forced: %v)\n", targetBTreeID, forceFlush)
	} else {
		log.Printf("BufferManager: Flushing entire deferred queue (Forced: %v)\n", forceFlush)
	}

	remainingQueue := make([]BufferPoolEntry, 0, len(bm.deferredFlushQueue))
	actuallyFlushedCount := 0
	reDeferredCount := 0

	for _, entry := range bm.deferredFlushQueue {
		if targetBTreeID != "" && entry.BTreeID != targetBTreeID {
			remainingQueue = append(remainingQueue, entry) // Keep if not for the target BTree
			continue
		}

		// Check if page is still in buffer and dirty
		bm.poolMutex.RLock() // RLock for checking pageMap and frame state
		frameIdx, pageExistsInPool := bm.pageMap[entry]
		var frame *PageFrame
		if pageExistsInPool {
			frame = bm.frames[frameIdx]
		}
		bm.poolMutex.RUnlock()

		if pageExistsInPool && frame.IsDirty() && frame.BTreeID == entry.BTreeID && frame.PageID == entry.PageID {
			// Attempt to flush this page. flushPage_nolock will handle carbon check if not forceFlush.
			// It returns nil if successfully flushed OR successfully re-deferred.
			// It returns an error only on actual I/O error.
			err := bm.flushPage_nolock(entry.BTreeID, entry.PageID, frameIdx, forceFlush)
			if err != nil {
				log.Printf(" carboneffectERROR ❌ Error flushing deferred page %s:%d: %v. Will retry later.", entry.BTreeID, entry.PageID, err)
				remainingQueue = append(remainingQueue, entry) // Keep in queue to retry
			} else {
				// If flushPage_nolock returned nil, it either flushed or re-deferred.
				// We need to check if it's still dirty to know if it was *actually* flushed.
				bm.poolMutex.RLock()
				isStillDirtyAfterAttempt := bm.frames[frameIdx].IsDirty()
				bm.poolMutex.RUnlock()

				if !isStillDirtyAfterAttempt { // Page was actually flushed
					actuallyFlushedCount++
					atomic.AddInt32(&bm.flushedDeferredCount, 1) // Counted as a deferred flush
					// The deferredPagesCount was decremented by flushPage_nolock if it added to queue and now it's flushed
					// No, deferredPagesCount is for items *currently* in the queue.
					// It gets decremented when an item leaves the queue *because it was flushed*.
					atomic.AddInt32(&bm.deferredPagesCount, -1)
					log.Printf(" carboneffectDEFERRED_FLUSH ➡️💾 Page %s:%d flushed from deferred queue (Forced: %v). Queue: %d",
						entry.BTreeID, entry.PageID, forceFlush, atomic.LoadInt32(&bm.deferredPagesCount))
				} else { // Page was re-deferred by flushPage_nolock (carbon still high, not forced)
					reDeferredCount++
					remainingQueue = append(remainingQueue, entry)
					// No change to deferredPagesCount as it's still deferred.
					log.Printf(" carboneffectRE_DEFERRED 🔄 Page %s:%d re-deferred (Carbon still high). Queue: %d",
						entry.BTreeID, entry.PageID, atomic.LoadInt32(&bm.deferredPagesCount))
				}
			}
		} else { // Page no longer in buffer, or not dirty, or mismatched (should not happen for BTreeID/PageID)
			// This page effectively doesn't need to be in the deferred queue anymore.
			atomic.AddInt32(&bm.deferredPagesCount, -1) // Decrement if it's being removed
			log.Printf("BufferManager: Page %s:%d from deferred queue no longer needs flushing (not found/dirty). Queue: %d",
				entry.BTreeID, entry.PageID, atomic.LoadInt32(&bm.deferredPagesCount))
		}
	}

	bm.deferredFlushQueue = remainingQueue
	if actuallyFlushedCount > 0 {
		log.Printf("BufferManager: Actually flushed %d pages from deferred queue.\n", actuallyFlushedCount)
	}
	if reDeferredCount > 0 {
		log.Printf("BufferManager: Re-deferred %d pages due to continued high carbon intensity.\n", reDeferredCount)
	}
	if len(bm.deferredFlushQueue) != int(atomic.LoadInt32(&bm.deferredPagesCount)) {
		log.Printf("WARN: Deferred queue length (%d) and counter (%d) mismatch after processing!", len(bm.deferredFlushQueue), atomic.LoadInt32(&bm.deferredPagesCount))
		atomic.StoreInt32(&bm.deferredPagesCount, int32(len(bm.deferredFlushQueue))) // Correct counter
	}
}

// flushPage_nolock: Modified in Phase 3 for carbon awareness.
func (bm *BufferManagerImpl) flushPage_nolock(btreeID string, pageID PageID, frameIdx int, forceFlush bool) error {
	frame := bm.frames[frameIdx]
	if !frame.IsDirty() {
		return nil
	}

	if bm.config.CarbonAware && bm.intensityProvider != nil && !forceFlush {
		signal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
		if err != nil {
			log.Printf("BufferManager: Error getting carbon intensity: %v. Proceeding with flush for %s:%d.", err, btreeID, pageID)
		} else {
			bm.lastKnownIntensityValue.Store(signal)
			if !signal.IsLow {
				bm.deferredFlushLock.Lock() // Lock before accessing deferredFlushQueue
				inQueue := false
				for _, entry := range bm.deferredFlushQueue {
					if entry.BTreeID == btreeID && entry.PageID == pageID {
						inQueue = true
						break
					}
				}
				if !inQueue {
					bm.deferredFlushQueue = append(bm.deferredFlushQueue, BufferPoolEntry{BTreeID: btreeID, PageID: pageID})
					atomic.AddInt32(&bm.deferredPagesCount, 1)
				}
				bm.deferredFlushLock.Unlock()
				log.Printf(" carboneffectDEFERRED 🟡 Page %s:%d (Frame: %d, Queue: %d) - Carbon: HIGH 🔴",
					btreeID, pageID, frameIdx, atomic.LoadInt32(&bm.deferredPagesCount))
				return nil // Successfully deferred, frame remains dirty
			}
		}
	}

	bm.metaMutex.RLock()
	file, exists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()

	if !exists {
		return fmt.Errorf("flush page internal error: BTree file for '%s' not found (Page: %d, Frame: %d)", btreeID, pageID, frameIdx)
	}

	offset := int64(pageID-1) * defaultPageSize
	n, err := file.WriteAt(frame.Data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %s:%d (Frame: %d) to disk: %w", btreeID, pageID, frameIdx, err)
	}
	if n != defaultPageSize {
		return fmt.Errorf("incomplete write for page %s:%d (Frame: %d): wrote %d bytes, expected %d", btreeID, pageID, frameIdx, n, defaultPageSize)
	}

	frame.ClearDirty() // Clears the dirty flag *after* successful write

	if forceFlush {
		atomic.AddInt32(&bm.flushedForcedCount, 1)
		log.Printf(" carboneffectFLUSHING ❗ Page %s:%d (Frame: %d, Forced) - Carbon: %s",
			btreeID, pageID, frameIdx, bm.getCurrentIntensityEmoji())
	} else if bm.config.CarbonAware && bm.intensityProvider != nil {
		atomic.AddInt32(&bm.flushedNormalCount, 1)
		log.Printf(" carboneffectFLUSHING 🟢 Page %s:%d (Frame: %d, Low Carbon) - Carbon: %s",
			btreeID, pageID, frameIdx, bm.getCurrentIntensityEmoji())
	} else {
		atomic.AddInt32(&bm.flushedNormalCount, 1)
		log.Printf(" carboneffectFLUSHING 💾 Page %s:%d (Frame: %d, Carbon-Aware Disabled)",
			btreeID, pageID, frameIdx)
	}
	return nil
}

// PinPage: Modified in Phase 3.
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

	bm.poolMutex.Lock()
	defer bm.poolMutex.Unlock()

	if frameIdx, exists := bm.pageMap[entry]; exists {
		frame := bm.frames[frameIdx]
		frame.Pin()
		bm.replacementPolicy.RecordAccess(frameIdx)
		return Page{Data: frame.Data}, nil
	}

	frameIdx := bm.findFreeFrame_nolock()
	if frameIdx == -1 {
		frameIdx = bm.replacementPolicy.FindVictim(bm.frames)
		if frameIdx == -1 {
			return Page{}, ErrBufferFull
		}
		victimFrame := bm.frames[frameIdx]
		var victimEntry BufferPoolEntry
		foundVictimInMap := false
		for e, idx := range bm.pageMap {
			if idx == frameIdx {
				victimEntry = e
				foundVictimInMap = true
				break
			}
		}
		if !foundVictimInMap {
			return Page{}, fmt.Errorf("internal error: victim frame %d not found in pageMap", frameIdx)
		}

		if victimFrame.IsDirty() {
			// Pass 'forceFlush = false' for victim eviction.
			if err := bm.flushPage_nolock(victimEntry.BTreeID, victimEntry.PageID, frameIdx, false); err != nil {
				return Page{}, fmt.Errorf("failed to process victim page %s:%d for eviction: %w", victimEntry.BTreeID, victimEntry.PageID, err)
			}
		}
		delete(bm.pageMap, victimEntry)
	}

	frame := bm.frames[frameIdx]
	if err := bm.readPage_nolock(btreeID, pageID, frame.Data); err != nil {
		bm.frames[frameIdx] = NewPageFrame(defaultPageSize)
		return Page{}, err
	}

	frame.BTreeID = btreeID
	frame.PageID = pageID
	frame.PinCount = 1
	frame.Dirty = false
	frame.LastAccess = time.Now()
	bm.pageMap[entry] = frameIdx
	bm.replacementPolicy.RecordAccess(frameIdx)

	return Page{Data: frame.Data}, nil
}

// flushAllDirtyPages: Modified in Phase 3.
func (bm *BufferManagerImpl) flushAllDirtyPages(btreeID string, forceFlush bool) error {
	log.Printf("BufferManager: Flushing all dirty pages for BTree '%s' (Forced: %v)\n", btreeID, forceFlush)

	bm.poolMutex.RLock() // RLock for initial scan of pageMap
	pagesToFlushCandidates := []BufferPoolEntry{}
	frameIndicesToFlush := []int{}

	for entry, frameIdx := range bm.pageMap {
		// Must check frame directly, as IsDirty can change.
		// This RLock only protects the pageMap iteration. Frame data is protected by frame's own RWMutex.
		// For IsDirty(), it uses frame's RLock.
		if entry.BTreeID == btreeID && bm.frames[frameIdx].IsDirty() {
			pagesToFlushCandidates = append(pagesToFlushCandidates, entry)
			frameIndicesToFlush = append(frameIndicesToFlush, frameIdx)
		}
	}
	bm.poolMutex.RUnlock()

	var lastErr error
	flushedCount := 0

	for i, entry := range pagesToFlushCandidates {
		frameIdx := frameIndicesToFlush[i]
		// Re-check dirty status under a frame lock if necessary, or rely on flushPage_nolock to re-check.
		// flushPage_nolock already checks IsDirty().
		if err := bm.flushPage_nolock(entry.BTreeID, entry.PageID, frameIdx, forceFlush); err != nil {
			// This error means an actual I/O error, as deferral returns nil.
			log.Printf("Error flushing page %s:%d during flushAll (Forced: %v): %v\n", entry.BTreeID, entry.PageID, forceFlush, err)
			lastErr = err
		} else {
			// If err is nil, it means it was either flushed or re-deferred (if !forceFlush).
			// We only count actual flushes here for this log message.
			// Need to check if it's still dirty to confirm if it was flushed vs re-deferred.
			bm.poolMutex.RLock() // RLock to check frame status
			frame := bm.frames[frameIdx]
			isActuallyFlushed := !frame.IsDirty()
			bm.poolMutex.RUnlock()
			if isActuallyFlushed {
				flushedCount++
			}
		}
	}

	if flushedCount > 0 {
		log.Printf("BufferManager: Successfully flushed %d dirty pages for BTree '%s' during flushAll (Forced: %v)\n", flushedCount, btreeID, forceFlush)
	} else {
		log.Printf("BufferManager: No pages were actually flushed for BTree '%s' during this flushAll call (Forced: %v) (may have been deferred or already clean)\n", btreeID, forceFlush)
	}
	return lastErr
}

// CloseBTree: Modified in Phase 3 and Phase 4.
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
	if err := bm.flushAllDirtyPages(name, true); err != nil {
		log.Printf("Warning: force flushAllDirtyPages failed during close of '%s': %v. Proceeding with close.\n", name, err)
	}

	// Phase 4: Force flush any pages for this BTree from the deferred queue.
	if bm.config.CarbonAware { // Only if carbon aware features are active
		log.Printf("BufferManager: Force flushing deferred queue for BTree '%s' during close.\n", name)
		bm.flushDeferredQueue(true, name) // forceFlush = true, specific BTree
	}

	bm.poolMutex.Lock()
	var pinnedPages []PageID
	pagesToRemoveFromMap := []BufferPoolEntry{}
	for entry, frameIdx := range bm.pageMap {
		if entry.BTreeID == name {
			if bm.frames[frameIdx].IsPinned() {
				pinnedPages = append(pinnedPages, entry.PageID)
			} else {
				pagesToRemoveFromMap = append(pagesToRemoveFromMap, entry)
			}
		}
	}

	if len(pinnedPages) > 0 {
		bm.poolMutex.Unlock()
		return fmt.Errorf("cannot close BTree '%s': pages %v are still pinned", name, pinnedPages)
	}

	for _, entry := range pagesToRemoveFromMap {
		frameIdx := bm.pageMap[entry]
		delete(bm.pageMap, entry)
		bm.frames[frameIdx] = NewPageFrame(defaultPageSize)
	}
	bm.poolMutex.Unlock()

	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()
	file, stillExistsInMeta := bm.btreeFiles[name]
	if !stillExistsInMeta {
		log.Printf("BufferManager: BTree '%s' was removed from meta map by another operation before final lock during close.\n", name)
		return ErrBTreeNotFound // Or nil, if this state is acceptable.
	}

	err := file.Close()
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
	if bm.config.CarbonAware {
		close(bm.shutdownChan) // Signal background goroutines to stop
	}
	// Add any other cleanup needed for buffer manager itself, like ensuring all BTrees are closed.
	// For now, this primarily handles the carbon watchdog.
	// It might be prudent to iterate all open btrees and try to close them.
	bm.metaMutex.RLock()
	openBTrees := make([]string, 0, len(bm.btreeFiles))
	for name := range bm.btreeFiles {
		openBTrees = append(openBTrees, name)
	}
	bm.metaMutex.RUnlock()

	for _, name := range openBTrees {
		log.Printf("BufferManager: Closing BTree '%s' during shutdown.\n", name)
		if err := bm.CloseBTree(name); err != nil {
			log.Printf("BufferManager: Error closing BTree '%s' during shutdown: %v\n", name, err)
		}
	}
	log.Println("BufferManager: Shutdown complete.")
}

// --- Unchanged methods from original provided code for Phase 3/4 scope ---
// CreateBTree, OpenBTree, DeleteBTree, UnpinPage, AllocatePage, FreePage,
// findFreeFrame_nolock, readPage_nolock

// In minibtreestore/storage/buffer/buffermanager_impl.go

func (bm *BufferManagerImpl) CreateBTree(name string) error {
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()

	if _, exists := bm.btreeFiles[name]; exists {
		return fmt.Errorf("btree '%s' already open in buffer manager", name) // Changed error message slightly for clarity
	}
	if name == "" {
		return fmt.Errorf("btree name cannot be empty")
	}

	filePath := filepath.Join(bm.config.Directory, name)
	if _, err := os.Stat(filePath); err == nil {
		// File exists, this implies the BTree might already exist on disk.
		// Depending on desired semantics, you might want to return an error
		// or allow opening it. For CreateBTree, usually means error if exists.
		return fmt.Errorf("file '%s' already exists on disk", filePath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking status of '%s': %w", filePath, err)
	}

	file, err := os.Create(filePath) // Creates or truncates if exists (but os.Stat check above should prevent overwrite)
	if err != nil {
		return fmt.Errorf("failed to create file '%s': %w", filePath, err)
	}

	// *** THIS IS THE CRUCIAL FIX ***
	// Ensure the new file has space for at least the root page.
	if err := file.Truncate(defaultPageSize); err != nil {
		file.Close()        // Close the file descriptor
		os.Remove(filePath) // Attempt to clean up the partially created file
		return fmt.Errorf("failed to truncate new btree file '%s' to initial size: %w", name, err)
	}
	// *******************************

	bm.btreeFiles[name] = file
	bm.nextPageIDs[name] = 1 // After creating, page 1 is the root, next to allocate would be 2 if root is taken.
	// However, NewBTreePersistent will manage the root page.
	// Let's set nextPageIDs[name] to 2 assuming page 1 is the root and will be claimed.
	// This aligns with the original CreateBTree logic
	bm.nextPageIDs[name] = 2
	bm.freePages[name] = []PageID{}
	log.Printf("BufferManager: Created BTree file '%s' and truncated to initial size.\n", name)
	return nil
}

func (bm *BufferManagerImpl) OpenBTree(name string) error {
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()

	if _, exists := bm.btreeFiles[name]; exists {
		return nil
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
		bm.nextPageIDs[name] = 1
	} else {
		bm.nextPageIDs[name] = PageID(fileInfo.Size()/defaultPageSize) + 1
	}

	if fileInfo.Size() > 0 && fileInfo.Size()%defaultPageSize != 0 {
		log.Printf("Warning: File '%s' size %d not a perfect multiple of page size %d\n", name, fileInfo.Size(), defaultPageSize)
	}
	bm.freePages[name] = []PageID{}
	log.Printf("BufferManager: Opened BTree '%s', next page ID currently estimated as: %d\n", name, bm.nextPageIDs[name])
	return nil
}

func (bm *BufferManagerImpl) DeleteBTree(name string) error {
	log.Printf("BufferManager: Attempting to delete BTree '%s'\n", name)
	if err := bm.CloseBTree(name); err != nil && err != ErrBTreeNotFound {
		// If CloseBTree fails for reasons other than "not found" (e.g. pages pinned),
		// we should probably not proceed with deletion.
		return fmt.Errorf("delete BTree '%s' failed during pre-close: %w", name, err)
	}

	bm.metaMutex.Lock() // Changed from RLock to Lock as we might modify maps if CloseBTree was called internally
	defer bm.metaMutex.Unlock()

	filePath := filepath.Join(bm.config.Directory, name)
	// Clean up metadata even if file remove fails or file was already gone.
	delete(bm.btreeFiles, name)
	delete(bm.nextPageIDs, name)
	delete(bm.freePages, name)

	log.Printf("BufferManager: Removing file '%s'\n", filePath)
	err := os.Remove(filePath)
	if err != nil && !os.IsNotExist(err) {
		// Log error but metadata is already cleaned up.
		log.Printf("BufferManager: Failed to delete file '%s': %v. Metadata cleaned.", name, err)
		return fmt.Errorf("failed to delete file '%s': %w", name, err)
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
		return fmt.Errorf("unpin failed for page %s:%d: either not pinned or pin count is zero", btreeID, pageID)
	}
	return nil
}

func (bm *BufferManagerImpl) AllocatePage(btreeID string) (PageID, error) {
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()

	file, exists := bm.btreeFiles[btreeID]
	if !exists {
		return 0, fmt.Errorf("allocate page failed: BTree '%s' not found or not open", btreeID)
	}

	var pageID PageID
	allocatedFromFreeList := false

	freeList := bm.freePages[btreeID]
	if len(freeList) > 0 {
		pageID = freeList[len(freeList)-1]
		bm.freePages[btreeID] = freeList[:len(freeList)-1]
		allocatedFromFreeList = true
	} else {
		pageID = bm.nextPageIDs[btreeID]
		if pageID == 0 {
			pageID = 1 // Ensure pageID starts from 1 if it somehow became 0
		}
		bm.nextPageIDs[btreeID] = pageID + 1
	}

	if !allocatedFromFreeList {
		requiredSize := int64(pageID) * defaultPageSize
		fileInfo, err := file.Stat()
		if err != nil {
			if bm.nextPageIDs[btreeID] > pageID && pageID > 0 {
				bm.nextPageIDs[btreeID]--
			} else if pageID > 0 {
				bm.nextPageIDs[btreeID] = pageID
			}
			return 0, fmt.Errorf("failed to stat file for BTree '%s' during page allocation: %w", btreeID, err)
		}
		if fileInfo.Size() < requiredSize {
			if err := file.Truncate(requiredSize); err != nil {
				if bm.nextPageIDs[btreeID] > pageID && pageID > 0 {
					bm.nextPageIDs[btreeID]--
				} else if pageID > 0 {
					bm.nextPageIDs[btreeID] = pageID
				}
				return 0, fmt.Errorf("failed to truncate file for BTree '%s' to size %d for new page %d: %w", btreeID, requiredSize, pageID, err)
			}
		}
	}
	return pageID, nil
}

func (bm *BufferManagerImpl) FreePage(btreeID string, pageID PageID) error {
	if pageID == 0 {
		return fmt.Errorf("free page failed: invalid page ID 0 for BTree '%s'", btreeID)
	}

	bm.metaMutex.RLock()
	_, btreeExists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()
	if !btreeExists {
		return fmt.Errorf("free page failed: BTree '%s' not found or not open", btreeID)
	}

	entry := BufferPoolEntry{BTreeID: btreeID, PageID: pageID}

	bm.poolMutex.Lock()
	if frameIdx, exists := bm.pageMap[entry]; exists {
		frame := bm.frames[frameIdx]
		if frame.IsPinned() {
			bm.poolMutex.Unlock()
			return ErrPagePinned
		}
		delete(bm.pageMap, entry)
		bm.frames[frameIdx] = NewPageFrame(defaultPageSize)
	}
	bm.poolMutex.Unlock()

	bm.metaMutex.Lock()
	if _, stillExists := bm.btreeFiles[btreeID]; stillExists {
		isAlreadyFree := false
		for _, freeID := range bm.freePages[btreeID] {
			if freeID == pageID {
				isAlreadyFree = true
				break
			}
		}
		if !isAlreadyFree {
			bm.freePages[btreeID] = append(bm.freePages[btreeID], pageID)
		}
	}
	bm.metaMutex.Unlock()
	return nil
}

func (bm *BufferManagerImpl) findFreeFrame_nolock() int {
	for i, frame := range bm.frames {
		if frame.PageID == 0 && !frame.IsPinned() {
			return i
		}
	}
	return -1
}

func (bm *BufferManagerImpl) readPage_nolock(btreeID string, pageID PageID, buffer []byte) error {
	if len(buffer) != defaultPageSize {
		return fmt.Errorf("internal read error: provided buffer size %d is incorrect, expected %d", len(buffer), defaultPageSize)
	}

	bm.metaMutex.RLock()
	file, exists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()

	if !exists {
		return fmt.Errorf("read page internal error: BTree file for '%s' not found", btreeID)
	}

	offset := int64(pageID-1) * defaultPageSize
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file for BTree '%s' before reading page %d: %w", btreeID, pageID, err)
	}
	if fileInfo.Size() < offset+defaultPageSize {
		return ErrPageNotFound
	}

	n, err := file.ReadAt(buffer, offset)
	if err != nil {
		return fmt.Errorf("failed to read page %s:%d from disk (read %d bytes): %w", btreeID, pageID, n, err)
	}
	if n != defaultPageSize {
		return fmt.Errorf("incomplete read for page %s:%d: read %d bytes, expected %d", btreeID, pageID, n, defaultPageSize)
	}
	return nil
}

// Add to BufferManagerImpl in buffermanager_impl.go

// GetConfig returns the current buffer manager configuration.
func (bm *BufferManagerImpl) GetConfig() BufferManagerConfig {
	return bm.config
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

// Add to BufferManagerImpl in buffermanager_impl.go

// GetIntensityProvider returns the configured intensity provider.
// Useful for testing or external control of a mock provider.
func (bm *BufferManagerImpl) GetIntensityProvider() carbonaware.IntensityProvider {
	return bm.intensityProvider
}*/

package buffer

import (
	"fmt"
	"log"
	"minibtreestore/carbonaware"
	"minibtreestore/storage/page"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
		// This indicates an issue, either an extra unpin or a bug.
		// log.Printf("Warning: Unpin called on page %s:%d with PinCount %d\n", pf.BTreeID, pf.PageID, pf.PinCount)
		return false // Or panic, depending on strictness
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
		// If frame.PageID == 0, it's effectively free, but findFreeFrame_nolock should handle this.
		// FindVictim is for evicting an *in-use* unpinned page.
		if frame.PageID == 0 { // Should ideally be filtered out by findFreeFrame_nolock first
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
		Directory:             ".",
		BufferSize:            1024,
		CarbonAware:           false,
		CarbonRegion:          "default-region",
		DeferredFlushInterval: 1 * time.Minute,
		VisualizerPort:        "",
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

	bm.lastKnownIntensityValue.Store(carbonaware.IntensitySignal{IsLow: true, Value: 0})
	atomic.StoreInt32(&bm.deferredPagesCount, 0)
	atomic.StoreInt32(&bm.flushedNormalCount, 0)
	atomic.StoreInt32(&bm.flushedDeferredCount, 0)
	atomic.StoreInt32(&bm.flushedForcedCount, 0)

	if config.CarbonAware {
		bm.intensityProvider = carbonaware.NewMockIntensityProvider(config.CarbonRegion)
		log.Printf("BufferManager: Carbon-aware mode ENABLED for region '%s'. Interval: %v\n",
			config.CarbonRegion, config.DeferredFlushInterval)
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

func (bm *BufferManagerImpl) getCurrentIntensityEmoji() string {
	if !bm.config.CarbonAware || bm.intensityProvider == nil {
		return "N/A ⚫"
	}
	var currentSignalToUse carbonaware.IntensitySignal
	storedSignal, ok := bm.lastKnownIntensityValue.Load().(carbonaware.IntensitySignal)
	if !ok || storedSignal.Timestamp.IsZero() {
		freshSignal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
		if err == nil {
			currentSignalToUse = freshSignal
			bm.lastKnownIntensityValue.Store(freshSignal) // Keep it updated
		} else {
			log.Printf("BufferManager: Error fetching fresh intensity for emoji: %v. Using stored/default.", err)
			if ok { // Use stale stored if fresh fetch failed
				currentSignalToUse = storedSignal
			} else { // Very default if everything fails
				return "ERR ❓"
			}
		}
	} else {
		currentSignalToUse = storedSignal
	}
	if currentSignalToUse.IsLow {
		return fmt.Sprintf("LOW 🟢 (%.0f)", currentSignalToUse.Value)
	}
	return fmt.Sprintf("HIGH 🔴 (%.0f)", currentSignalToUse.Value)
}

func (bm *BufferManagerImpl) GetConfig() BufferManagerConfig {
	return bm.config
}
func (bm *BufferManagerImpl) GetIntensityProvider() carbonaware.IntensityProvider {
	return bm.intensityProvider
}
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
func (bm *BufferManagerImpl) GetCurrentPageMapSize() int {
	bm.poolMutex.RLock()
	defer bm.poolMutex.RUnlock()
	return len(bm.pageMap)
}
func (bm *BufferManagerImpl) GetCurrentDirtyPagesCount() int {
	bm.poolMutex.RLock()
	defer bm.poolMutex.RUnlock()
	count := 0
	for _, frame := range bm.frames {
		if frame.PageID != 0 && frame.IsDirty() {
			count++
		}
	}
	return count
}

func (bm *BufferManagerImpl) processDeferredFlushes() {
	if bm.config.DeferredFlushInterval <= 0 {
		log.Println("[CarbonWatchdog] Deferred flush interval is not positive, watchdog will not run.")
		return
	}
	ticker := time.NewTicker(bm.config.DeferredFlushInterval)
	defer ticker.Stop()
	log.Println("[CarbonWatchdog] Started: Watching deferred flush queue.")
	for {
		select {
		case <-ticker.C:
			if bm.intensityProvider == nil || !bm.config.CarbonAware {
				continue
			}
			currentDeferredCount := atomic.LoadInt32(&bm.deferredPagesCount)
			if currentDeferredCount == 0 {
				continue
			}
			log.Printf("[CarbonWatchdog] Checking deferred queue (Size: %d). Current Intensity: %s\n",
				currentDeferredCount, bm.getCurrentIntensityEmoji()) // Emoji will use lastKnown or try fresh
			signal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
			if err != nil {
				log.Printf("[CarbonWatchdog] Error getting carbon intensity: %v. Will retry later.", err)
				continue
			}
			bm.lastKnownIntensityValue.Store(signal)
			log.Printf("[CarbonWatchdog] Fetched intensity. Stored in lastKnownIntensityValue. IsLow: %v, Value: %.2f", signal.IsLow, signal.Value)
			if signal.IsLow {
				log.Printf("[CarbonWatchdog] Intensity is LOW 🟢. Attempting to flush deferred queue.\n")
				bm.flushDeferredQueue(false)
			} else {
				log.Printf("[CarbonWatchdog] Intensity is HIGH 🔴. Deferring flush of queue.\n")
			}
		case <-bm.shutdownChan:
			log.Println("[CarbonWatchdog] Shutting down.")
			return
		}
	}
}

func (bm *BufferManagerImpl) flushDeferredQueue(forceFlush bool, specificBTreeID ...string) {
	bm.deferredFlushLock.Lock()
	if len(bm.deferredFlushQueue) == 0 {
		bm.deferredFlushLock.Unlock()
		return
	}
	targetBTreeID := ""
	if len(specificBTreeID) > 0 && specificBTreeID[0] != "" {
		targetBTreeID = specificBTreeID[0]
		log.Printf("BufferManager: Flushing deferred queue specifically for BTree '%s' (Forced: %v, Queue size: %d)\n", targetBTreeID, forceFlush, len(bm.deferredFlushQueue))
	} else {
		log.Printf("BufferManager: Flushing entire deferred queue (Forced: %v, Queue size: %d)\n", forceFlush, len(bm.deferredFlushQueue))
	}

	remainingQueue := make([]BufferPoolEntry, 0, len(bm.deferredFlushQueue))
	actuallyFlushedCount := 0
	reDeferredCount := 0

	for _, entry := range bm.deferredFlushQueue {
		if targetBTreeID != "" && entry.BTreeID != targetBTreeID {
			remainingQueue = append(remainingQueue, entry)
			continue
		}
		bm.poolMutex.RLock()
		frameIdx, pageExistsInPool := bm.pageMap[entry]
		var frame *PageFrame
		if pageExistsInPool {
			frame = bm.frames[frameIdx]
		}
		bm.poolMutex.RUnlock()

		if pageExistsInPool && frame.IsDirty() && frame.BTreeID == entry.BTreeID && frame.PageID == entry.PageID {
			err := bm.flushPage_nolock(entry.BTreeID, entry.PageID, frameIdx, forceFlush) // Pass frameIdx here
			if err != nil {
				log.Printf(" carboneffectERROR ❌ Error flushing deferred page %s:%d: %v. Will retry later.", entry.BTreeID, entry.PageID, err)
				remainingQueue = append(remainingQueue, entry)
			} else {
				bm.poolMutex.RLock()
				isStillDirtyAfterAttempt := bm.frames[frameIdx].IsDirty()
				bm.poolMutex.RUnlock()
				if !isStillDirtyAfterAttempt {
					actuallyFlushedCount++
					atomic.AddInt32(&bm.flushedDeferredCount, 1)
					atomic.AddInt32(&bm.deferredPagesCount, -1) // Decrement because it's truly flushed from deferral
					log.Printf(" carboneffectDEFERRED_FLUSH ➡️💾 Page %s:%d flushed from deferred queue (Forced: %v). Queue: %d",
						entry.BTreeID, entry.PageID, forceFlush, atomic.LoadInt32(&bm.deferredPagesCount))
				} else {
					reDeferredCount++
					remainingQueue = append(remainingQueue, entry)
					log.Printf(" carboneffectRE_DEFERRED 🔄 Page %s:%d re-deferred (Carbon still high). Queue: %d",
						entry.BTreeID, entry.PageID, atomic.LoadInt32(&bm.deferredPagesCount))
				}
			}
		} else {
			atomic.AddInt32(&bm.deferredPagesCount, -1) // Page gone or clean, remove from deferred count
			log.Printf("BufferManager: Page %s:%d from deferred queue no longer needs flushing (not found/dirty from pool check). Queue: %d",
				entry.BTreeID, entry.PageID, atomic.LoadInt32(&bm.deferredPagesCount))
		}
	}
	bm.deferredFlushQueue = remainingQueue
	bm.deferredFlushLock.Unlock() // Unlock before logging summary

	if actuallyFlushedCount > 0 {
		log.Printf("BufferManager: Actually flushed %d pages from deferred queue processing.\n", actuallyFlushedCount)
	}
	if reDeferredCount > 0 {
		log.Printf("BufferManager: Re-deferred %d pages during deferred queue processing.\n", reDeferredCount)
	}
	// Safety check for counter
	bm.deferredFlushLock.Lock()
	currentQueueLen := len(bm.deferredFlushQueue)
	bm.deferredFlushLock.Unlock()
	if int32(currentQueueLen) != atomic.LoadInt32(&bm.deferredPagesCount) {
		log.Printf("WARN: Deferred queue length (%d) and counter (%d) mismatch after processing! Correcting counter.", currentQueueLen, atomic.LoadInt32(&bm.deferredPagesCount))
		atomic.StoreInt32(&bm.deferredPagesCount, int32(currentQueueLen))
	}
}

func (bm *BufferManagerImpl) flushPage_nolock(btreeID string, pageID PageID, frameIdx int, forceFlush bool) error {
	frame := bm.frames[frameIdx]
	if !frame.IsDirty() {
		return nil
	}

	if bm.config.CarbonAware && bm.intensityProvider != nil && !forceFlush {
		signal, err := bm.intensityProvider.GetCurrentIntensity(bm.config.CarbonRegion)
		if err != nil {
			log.Printf("BufferManager: Error getting carbon intensity: %v. Proceeding with flush for %s:%d.", err, btreeID, pageID)
		} else {
			bm.lastKnownIntensityValue.Store(signal)
			if !signal.IsLow {
				bm.deferredFlushLock.Lock()
				inQueue := false
				for _, entry := range bm.deferredFlushQueue {
					if entry.BTreeID == btreeID && entry.PageID == pageID {
						inQueue = true
						break
					}
				}
				if !inQueue {
					bm.deferredFlushQueue = append(bm.deferredFlushQueue, BufferPoolEntry{BTreeID: btreeID, PageID: pageID})
					atomic.AddInt32(&bm.deferredPagesCount, 1)
				}
				bm.deferredFlushLock.Unlock()
				log.Printf(" carboneffectDEFERRED 🟡 Page %s:%d (Frame: %d, Queue: %d) - Carbon: %s",
					btreeID, pageID, frameIdx, atomic.LoadInt32(&bm.deferredPagesCount), bm.getCurrentIntensityEmoji())
				return nil
			}
		}
	}

	bm.metaMutex.RLock()
	file, exists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()
	if !exists {
		return fmt.Errorf("flush page internal error: BTree file for '%s' not found (Page: %d, Frame: %d)", btreeID, pageID, frameIdx)
	}

	offset := int64(pageID-1) * defaultPageSize
	n, err := file.WriteAt(frame.Data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %s:%d (Frame: %d) to disk: %w", btreeID, pageID, frameIdx, err)
	}
	if n != defaultPageSize {
		return fmt.Errorf("incomplete write for page %s:%d (Frame: %d): wrote %d bytes, expected %d", btreeID, pageID, frameIdx, n, defaultPageSize)
	}

	frame.ClearDirty()

	if forceFlush {
		atomic.AddInt32(&bm.flushedForcedCount, 1)
		log.Printf(" carboneffectFLUSHING ❗ Page %s:%d (Frame: %d, Forced) - Carbon: %s",
			btreeID, pageID, frameIdx, bm.getCurrentIntensityEmoji())
	} else if bm.config.CarbonAware && bm.intensityProvider != nil {
		atomic.AddInt32(&bm.flushedNormalCount, 1)
		log.Printf(" carboneffectFLUSHING 🟢 Page %s:%d (Frame: %d, Low Carbon) - Carbon: %s",
			btreeID, pageID, frameIdx, bm.getCurrentIntensityEmoji())
	} else {
		atomic.AddInt32(&bm.flushedNormalCount, 1)
		log.Printf(" carboneffectFLUSHING 💾 Page %s:%d (Frame: %d, Carbon-Aware Disabled)",
			btreeID, pageID, frameIdx)
	}
	return nil
}

// PinPage: Modified to correctly handle deferred victim flushes.
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

	// 2. Page not in buffer, try to find a frame. Loop to handle deferred victims.
	// Max attempts to prevent potential infinite loop if all frames are pinned or constantly deferred.
	// Number of attempts can be bm.config.BufferSize, if after trying all frames, none are available.
	for attempts := 0; attempts < bm.config.BufferSize+1; attempts++ {
		targetFrameIdx := bm.findFreeFrame_nolock()

		if targetFrameIdx != -1 { // Found a completely free frame
			frame := bm.frames[targetFrameIdx]
			if err := bm.readPage_nolock(btreeID, pageID, frame.Data); err != nil {
				bm.frames[targetFrameIdx] = NewPageFrame(defaultPageSize) // Reset on error
				bm.poolMutex.Unlock()
				return Page{}, fmt.Errorf("PinPage: read new page %s:%d into free frame %d failed: %w", btreeID, pageID, targetFrameIdx, err)
			}
			frame.BTreeID = btreeID
			frame.PageID = pageID
			frame.PinCount = 1
			frame.Dirty = false
			frame.LastAccess = time.Now()
			bm.pageMap[entry] = targetFrameIdx
			bm.replacementPolicy.RecordAccess(targetFrameIdx)
			bm.poolMutex.Unlock()
			return Page{Data: frame.Data}, nil
		}

		// No free frames, try to evict a victim
		victimFrameIdx := bm.replacementPolicy.FindVictim(bm.frames)
		if victimFrameIdx == -1 { // All pages are pinned
			bm.poolMutex.Unlock()
			return Page{}, ErrBufferFull
		}

		victimFrame := bm.frames[victimFrameIdx]
		var victimEntry BufferPoolEntry
		foundVictimInMap := false
		for e, idx := range bm.pageMap { // Find victim's BufferPoolEntry
			if idx == victimFrameIdx {
				victimEntry = e
				foundVictimInMap = true
				break
			}
		}
		if !foundVictimInMap { // Should not happen
			bm.poolMutex.Unlock()
			return Page{}, fmt.Errorf("internal error: PinPage victim frame %d not in pageMap", victimFrameIdx)
		}

		if victimFrame.IsDirty() {
			// Try to flush the victim. Pass forceFlush = false.
			errFlush := bm.flushPage_nolock(victimEntry.BTreeID, victimEntry.PageID, victimFrameIdx, false)
			if errFlush != nil { // Actual I/O error during flush attempt
				bm.poolMutex.Unlock()
				return Page{}, fmt.Errorf("PinPage: failed to flush victim page %s:%d for eviction: %w", victimEntry.BTreeID, victimEntry.PageID, errFlush)
			}

			if victimFrame.IsDirty() { // Check again: if still dirty, it means flush was deferred
				log.Printf("BufferManager: PinPage: Victim %s:%d (Frame %d) flush deferred. Frame remains in use. Attempt: %d. Seeking another victim.",
					victimEntry.BTreeID, victimEntry.PageID, victimFrameIdx, attempts+1)
				// The frame is still "occupied" by the deferred page.
				// Mark it as accessed so LRU might pick something else next time if possible.
				bm.replacementPolicy.RecordAccess(victimFrameIdx)
				// Continue the loop to find another victim or a free frame.
				if attempts == bm.config.BufferSize { // Last attempt, about to fail
					log.Printf("BufferManager: PinPage: Max attempts reached to find frame for %s:%d due to deferrals/pins.", btreeID, pageID)
				}
				continue // Try to find another frame in the next iteration
			}
		}

		// If we reach here, the victimFrame was either clean or successfully flushed (not deferred).
		// It's now available for reuse.
		delete(bm.pageMap, victimEntry)
		targetFrameIdx = victimFrameIdx // This is the frame we'll use

		// Load the requested page into this now-available frame
		frame := bm.frames[targetFrameIdx]
		if err := bm.readPage_nolock(btreeID, pageID, frame.Data); err != nil {
			// If read fails, we need to carefully reset state or handle this.
			// For now, just return error. The frame might be in an inconsistent state.
			// Ideally, reset the frame: bm.frames[targetFrameIdx] = NewPageFrame(defaultPageSize)
			// And potentially re-add victimEntry to pageMap if its flush failed earlier (though here it was successful or clean).
			bm.poolMutex.Unlock()
			return Page{}, fmt.Errorf("PinPage: read new page %s:%d into victim frame %d failed: %w", btreeID, pageID, targetFrameIdx, err)
		}
		frame.BTreeID = btreeID
		frame.PageID = pageID
		frame.PinCount = 1
		frame.Dirty = false
		frame.LastAccess = time.Now()
		bm.pageMap[entry] = targetFrameIdx
		bm.replacementPolicy.RecordAccess(targetFrameIdx)
		bm.poolMutex.Unlock()
		return Page{Data: frame.Data}, nil
	}

	// If loop finishes, all attempts to find a frame failed.
	bm.poolMutex.Unlock()
	log.Printf("BufferManager: PinPage for %s:%d failed after %d attempts. Buffer likely full of pinned/deferred pages.", btreeID, pageID, bm.config.BufferSize+1)
	return Page{}, ErrBufferFull
}

func (bm *BufferManagerImpl) flushAllDirtyPages(btreeID string, forceFlush bool) error {
	log.Printf("BufferManager: Flushing all dirty pages for BTree '%s' (Forced: %v)\n", btreeID, forceFlush)

	bm.poolMutex.RLock()
	pagesToFlushInfo := []struct {
		entry    BufferPoolEntry
		frameIdx int
	}{}

	for entry, frameIdx := range bm.pageMap {
		if entry.BTreeID == btreeID && bm.frames[frameIdx].IsDirty() { // Check IsDirty under RLock
			pagesToFlushInfo = append(pagesToFlushInfo, struct {
				entry    BufferPoolEntry
				frameIdx int
			}{entry, frameIdx})
		}
	}
	bm.poolMutex.RUnlock()

	var lastErr error
	flushedCount := 0

	for _, info := range pagesToFlushInfo {
		// Re-check dirty status more safely if needed, though flushPage_nolock does it.
		// The frame data itself is protected by its own RWMutex for dirty flag.
		err := bm.flushPage_nolock(info.entry.BTreeID, info.entry.PageID, info.frameIdx, forceFlush)
		if err != nil {
			log.Printf("Error flushing page %s:%d during flushAll (Forced: %v): %v\n", info.entry.BTreeID, info.entry.PageID, forceFlush, err)
			lastErr = err
		} else {
			// Check if it was actually flushed (not re-deferred if forceFlush was false, though for flushAll it's usually true)
			bm.poolMutex.RLock() // Need to lock to safely access bm.frames[info.frameIdx]
			frameIsStillDirty := bm.frames[info.frameIdx].IsDirty()
			bm.poolMutex.RUnlock()
			if !frameIsStillDirty {
				flushedCount++
			}
		}
	}

	if flushedCount > 0 {
		log.Printf("BufferManager: Successfully flushed %d dirty pages for BTree '%s' during flushAll (Forced: %v)\n", flushedCount, btreeID, forceFlush)
	} else {
		log.Printf("BufferManager: No pages were actually flushed for BTree '%s' during this flushAll call (Forced: %v) (may have been deferred, already clean, or errors occurred)\n", btreeID, forceFlush)
	}
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

	if err := bm.flushAllDirtyPages(name, true); err != nil {
		log.Printf("Warning: force flushAllDirtyPages failed during close of '%s': %v. Proceeding with close.\n", name, err)
	}

	if bm.config.CarbonAware {
		log.Printf("BufferManager: Force flushing deferred queue for BTree '%s' during close.\n", name)
		bm.flushDeferredQueue(true, name)
	}

	bm.poolMutex.Lock()
	var pinnedPages []PageID
	pagesToRemoveFromMap := []BufferPoolEntry{}
	for entry, frameIdx := range bm.pageMap {
		if entry.BTreeID == name {
			if bm.frames[frameIdx].IsPinned() {
				pinnedPages = append(pinnedPages, entry.PageID)
			} else {
				pagesToRemoveFromMap = append(pagesToRemoveFromMap, entry)
			}
		}
	}

	if len(pinnedPages) > 0 {
		bm.poolMutex.Unlock()
		return fmt.Errorf("cannot close BTree '%s': pages %v are still pinned", name, pinnedPages)
	}

	for _, entry := range pagesToRemoveFromMap {
		frameIdx := bm.pageMap[entry]
		delete(bm.pageMap, entry)
		bm.frames[frameIdx] = NewPageFrame(defaultPageSize)
	}
	bm.poolMutex.Unlock()

	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()
	file, stillExistsInMeta := bm.btreeFiles[name]
	if !stillExistsInMeta {
		log.Printf("BufferManager: BTree '%s' was removed from meta map by another operation before final lock during close.\n", name)
		return ErrBTreeNotFound
	}

	err := file.Close()
	delete(bm.btreeFiles, name)
	delete(bm.nextPageIDs, name)
	delete(bm.freePages, name)

	if err != nil {
		return fmt.Errorf("failed to close file for BTree '%s': %w", name, err)
	}
	log.Printf("BufferManager: Closed BTree '%s'\n", name)
	return nil
}

func (bm *BufferManagerImpl) Shutdown() {
	log.Println("BufferManager: Shutdown initiated.")
	if bm.config.CarbonAware {
		close(bm.shutdownChan)
	}
	bm.metaMutex.RLock()
	openBTrees := make([]string, 0, len(bm.btreeFiles))
	for name := range bm.btreeFiles {
		openBTrees = append(openBTrees, name)
	}
	bm.metaMutex.RUnlock()

	for _, name := range openBTrees {
		log.Printf("BufferManager: Closing BTree '%s' during shutdown.\n", name)
		if err := bm.CloseBTree(name); err != nil {
			log.Printf("BufferManager: Error closing BTree '%s' during shutdown: %v\n", name, err)
		}
	}
	log.Println("BufferManager: Shutdown complete.")
}

// --- Unchanged methods from original ---
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
	bm.nextPageIDs[name] = 2
	bm.freePages[name] = []PageID{}
	log.Printf("BufferManager: Created BTree file '%s' and truncated to initial size.\n", name)
	return nil
}

func (bm *BufferManagerImpl) OpenBTree(name string) error {
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()
	if _, exists := bm.btreeFiles[name]; exists {
		return nil
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
		bm.nextPageIDs[name] = 1
	} else {
		bm.nextPageIDs[name] = PageID(fileInfo.Size()/defaultPageSize) + 1
	}
	if fileInfo.Size() > 0 && fileInfo.Size()%defaultPageSize != 0 {
		log.Printf("Warning: File '%s' size %d not a perfect multiple of page size %d\n", name, fileInfo.Size(), defaultPageSize)
	}
	bm.freePages[name] = []PageID{}
	log.Printf("BufferManager: Opened BTree '%s', next page ID currently estimated as: %d\n", name, bm.nextPageIDs[name])
	return nil
}

func (bm *BufferManagerImpl) DeleteBTree(name string) error {
	log.Printf("BufferManager: Attempting to delete BTree '%s'\n", name)
	if err := bm.CloseBTree(name); err != nil && err != ErrBTreeNotFound {
		return fmt.Errorf("delete BTree '%s' failed during pre-close: %w", name, err)
	}
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()
	filePath := filepath.Join(bm.config.Directory, name)
	delete(bm.btreeFiles, name)
	delete(bm.nextPageIDs, name)
	delete(bm.freePages, name)
	log.Printf("BufferManager: Removing file '%s'\n", filePath)
	err := os.Remove(filePath)
	if err != nil && !os.IsNotExist(err) {
		log.Printf("BufferManager: Failed to delete file '%s': %v. Metadata cleaned.", name, err)
		return fmt.Errorf("failed to delete file '%s': %w", name, err)
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
		return fmt.Errorf("unpin failed for page %s:%d: either not pinned or pin count is zero", btreeID, pageID)
	}
	return nil
}

func (bm *BufferManagerImpl) AllocatePage(btreeID string) (PageID, error) {
	bm.metaMutex.Lock()
	defer bm.metaMutex.Unlock()
	file, exists := bm.btreeFiles[btreeID]
	if !exists {
		return 0, fmt.Errorf("allocate page failed: BTree '%s' not found or not open", btreeID)
	}
	var pageID PageID
	allocatedFromFreeList := false
	freeList := bm.freePages[btreeID]
	if len(freeList) > 0 {
		pageID = freeList[len(freeList)-1]
		bm.freePages[btreeID] = freeList[:len(freeList)-1]
		allocatedFromFreeList = true
	} else {
		pageID = bm.nextPageIDs[btreeID]
		if pageID == 0 {
			pageID = 1
		}
		bm.nextPageIDs[btreeID] = pageID + 1
	}
	if !allocatedFromFreeList {
		requiredSize := int64(pageID) * defaultPageSize
		fileInfo, err := file.Stat()
		if err != nil {
			if bm.nextPageIDs[btreeID] > pageID && pageID > 0 {
				bm.nextPageIDs[btreeID]--
			} else if pageID > 0 {
				bm.nextPageIDs[btreeID] = pageID
			}
			return 0, fmt.Errorf("failed to stat file for BTree '%s' during page allocation: %w", btreeID, err)
		}
		if fileInfo.Size() < requiredSize {
			if err := file.Truncate(requiredSize); err != nil {
				if bm.nextPageIDs[btreeID] > pageID && pageID > 0 {
					bm.nextPageIDs[btreeID]--
				} else if pageID > 0 {
					bm.nextPageIDs[btreeID] = pageID
				}
				return 0, fmt.Errorf("failed to truncate file for BTree '%s' to size %d for new page %d: %w", btreeID, requiredSize, pageID, err)
			}
		}
	}
	return pageID, nil
}

func (bm *BufferManagerImpl) FreePage(btreeID string, pageID PageID) error {
	if pageID == 0 {
		return fmt.Errorf("free page failed: invalid page ID 0 for BTree '%s'", btreeID)
	}
	bm.metaMutex.RLock()
	_, btreeExists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()
	if !btreeExists {
		return fmt.Errorf("free page failed: BTree '%s' not found or not open", btreeID)
	}
	entry := BufferPoolEntry{BTreeID: btreeID, PageID: pageID}
	bm.poolMutex.Lock()
	if frameIdx, exists := bm.pageMap[entry]; exists {
		frame := bm.frames[frameIdx]
		if frame.IsPinned() {
			bm.poolMutex.Unlock()
			return ErrPagePinned
		}
		delete(bm.pageMap, entry)
		bm.frames[frameIdx] = NewPageFrame(defaultPageSize)
	}
	bm.poolMutex.Unlock()
	bm.metaMutex.Lock()
	if _, stillExists := bm.btreeFiles[btreeID]; stillExists {
		isAlreadyFree := false
		for _, freeID := range bm.freePages[btreeID] {
			if freeID == pageID {
				isAlreadyFree = true
				break
			}
		}
		if !isAlreadyFree {
			bm.freePages[btreeID] = append(bm.freePages[btreeID], pageID)
		}
	}
	bm.metaMutex.Unlock()
	return nil
}

func (bm *BufferManagerImpl) findFreeFrame_nolock() int {
	for i, frame := range bm.frames {
		if frame.PageID == 0 && !frame.IsPinned() {
			return i
		}
	}
	return -1
}

func (bm *BufferManagerImpl) readPage_nolock(btreeID string, pageID PageID, buffer []byte) error {
	if len(buffer) != defaultPageSize {
		return fmt.Errorf("internal read error: provided buffer size %d is incorrect, expected %d", len(buffer), defaultPageSize)
	}
	bm.metaMutex.RLock()
	file, exists := bm.btreeFiles[btreeID]
	bm.metaMutex.RUnlock()
	if !exists {
		return fmt.Errorf("read page internal error: BTree file for '%s' not found", btreeID)
	}
	offset := int64(pageID-1) * defaultPageSize
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file for BTree '%s' before reading page %d: %w", btreeID, pageID, err)
	}
	if fileInfo.Size() < offset+defaultPageSize {
		return ErrPageNotFound
	}
	n, err := file.ReadAt(buffer, offset)
	if err != nil {
		return fmt.Errorf("failed to read page %s:%d from disk (read %d bytes): %w", btreeID, pageID, n, err)
	}
	if n != defaultPageSize {
		return fmt.Errorf("incomplete read for page %s:%d: read %d bytes, expected %d", btreeID, pageID, n, defaultPageSize)
	}
	return nil
}
