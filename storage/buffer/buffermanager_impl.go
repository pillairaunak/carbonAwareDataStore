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
	config                  BufferManagerConfig
	frames                  []*PageFrame
	pageMap                 map[BufferPoolEntry]int
	replacementPolicy       PageReplacementPolicy
	btreeFiles              map[string]*os.File
	nextPageIDs             map[string]PageID
	freePages               map[string][]PageID
	metaMutex               sync.RWMutex
	poolMutex               sync.RWMutex
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
	// Default config values, including new ones for API provider
	config := BufferManagerConfig{
		Directory:                   ".",
		BufferSize:                  1024,
		CarbonAware:                 false,
		CarbonRegion:                "default-region", // Used by MockProvider if no specific region logic
		DeferredFlushInterval:       1 * time.Minute,
		VisualizerPort:              "",
		CarbonProviderType:          "api",                        // Default to mock provider
		CarbonIntensityApiUrl:       "http://localhost:8000/data", // Default API URL
		CarbonIntensityApiThreshold: 100.0,                        // Default threshold (e.g., gCO2eq/kWh)
		CarbonIntensityApiTimeout:   5 * time.Second,              // Default API request timeout
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

	bm.lastKnownIntensityValue.Store(carbonaware.IntensitySignal{IsLow: true, Value: 0, Region: config.CarbonRegion})
	atomic.StoreInt32(&bm.deferredPagesCount, 0)
	atomic.StoreInt32(&bm.flushedNormalCount, 0)
	atomic.StoreInt32(&bm.flushedDeferredCount, 0)
	atomic.StoreInt32(&bm.flushedForcedCount, 0)

	if config.CarbonAware {
		log.Printf("BufferManager: Carbon-aware mode ENABLED for region '%s'. Watchdog Interval: %v\n",
			config.CarbonRegion, config.DeferredFlushInterval)

		switch config.CarbonProviderType {
		case "api":
			apiProvider, err := carbonaware.NewApiIntensityProvider(
				config.CarbonIntensityApiUrl,
				config.CarbonIntensityApiThreshold,
				config.CarbonIntensityApiTimeout,
			)
			if err != nil {
				log.Printf("BufferManager: Failed to initialize ApiIntensityProvider: %v. Falling back to MockIntensityProvider.", err)
				bm.intensityProvider = carbonaware.NewMockIntensityProvider(config.CarbonRegion)
				log.Printf("BufferManager: Using MockIntensityProvider for region '%s'.", config.CarbonRegion)
			} else {
				bm.intensityProvider = apiProvider
				log.Printf("BufferManager: Using ApiIntensityProvider. URL: %s, Threshold: %.2f, Timeout: %v",
					config.CarbonIntensityApiUrl, config.CarbonIntensityApiThreshold, config.CarbonIntensityApiTimeout)
			}
		case "mock":
			bm.intensityProvider = carbonaware.NewMockIntensityProvider(config.CarbonRegion)
			log.Printf("BufferManager: Using MockIntensityProvider for region '%s'.", config.CarbonRegion)
		default:
			log.Printf("BufferManager: Unknown CarbonProviderType '%s'. Falling back to MockIntensityProvider.", config.CarbonProviderType)
			bm.intensityProvider = carbonaware.NewMockIntensityProvider(config.CarbonRegion)
			log.Printf("BufferManager: Using MockIntensityProvider for region '%s'.", config.CarbonRegion)
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

// ... (getCurrentIntensityEmoji, GetConfig, GetIntensityProvider, GetLastKnownIntensitySignal, GetDeferredPagesCount, etc. remain the same as before) ...
// ... (processDeferredFlushes, flushDeferredQueue, flushPage_nolock, PinPage, etc. remain the same as before) ...

// Helper for logging based on current intensity
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
			bm.lastKnownIntensityValue.Store(freshSignal)
		} else {
			log.Printf("BufferManager: Error fetching fresh intensity for emoji: %v. Using stored/default.", err)
			if ok {
				currentSignalToUse = storedSignal
			} else {
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

// Public accessors for config and stats
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

// processDeferredFlushes (No changes from Phase 4, but shown for completeness)
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
			// Log before fetching, emoji shows last known state or fresh if needed
			log.Printf("[CarbonWatchdog] Checking deferred queue (Size: %d). Current Intensity: %s\n",
				currentDeferredCount, bm.getCurrentIntensityEmoji())
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

// flushDeferredQueue (No changes from Phase 4, but shown for completeness)
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
			err := bm.flushPage_nolock(entry.BTreeID, entry.PageID, frameIdx, forceFlush)
			if err != nil {
				log.Printf(" carboneffectERROR ❌ Error flushing deferred page %s:%d: %v. Will retry later.", entry.BTreeID, entry.PageID, err)
				remainingQueue = append(remainingQueue, entry)
			} else {
				bm.poolMutex.RLock()
				isStillDirtyAfterAttempt := false
				if pageExistsInPool { // Check frame again as it might have been unmapped by another op if lock granularity is an issue
					frameToCheck := bm.frames[frameIdx]
					if frameToCheck.PageID == entry.PageID && frameToCheck.BTreeID == entry.BTreeID {
						isStillDirtyAfterAttempt = frameToCheck.IsDirty()
					} else {
						// Page got swapped out or frame reused, consider it "gone" for this deferred entry's purpose
						isStillDirtyAfterAttempt = false
						log.Printf("BufferManager: Page %s:%d seems to have left frame %d during deferred flush attempt.", entry.BTreeID, entry.PageID, frameIdx)
					}
				}
				bm.poolMutex.RUnlock()

				if !isStillDirtyAfterAttempt {
					actuallyFlushedCount++
					atomic.AddInt32(&bm.flushedDeferredCount, 1)
					atomic.AddInt32(&bm.deferredPagesCount, -1)
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
			atomic.AddInt32(&bm.deferredPagesCount, -1)
			log.Printf("BufferManager: Page %s:%d from deferred queue no longer needs flushing (not found/dirty from pool check). Queue: %d",
				entry.BTreeID, entry.PageID, atomic.LoadInt32(&bm.deferredPagesCount))
		}
	}
	bm.deferredFlushQueue = remainingQueue
	bm.deferredFlushLock.Unlock()

	if actuallyFlushedCount > 0 {
		log.Printf("BufferManager: Actually flushed %d pages from deferred queue processing.\n", actuallyFlushedCount)
	}
	if reDeferredCount > 0 {
		log.Printf("BufferManager: Re-deferred %d pages during deferred queue processing.\n", reDeferredCount)
	}
	bm.deferredFlushLock.Lock()
	currentQueueLen := len(bm.deferredFlushQueue)
	bm.deferredFlushLock.Unlock()
	if int32(currentQueueLen) != atomic.LoadInt32(&bm.deferredPagesCount) {
		log.Printf("WARN: Deferred queue length (%d) and counter (%d) mismatch after processing! Correcting counter.", currentQueueLen, atomic.LoadInt32(&bm.deferredPagesCount))
		atomic.StoreInt32(&bm.deferredPagesCount, int32(currentQueueLen))
	}
}

// flushPage_nolock (No changes from Phase 3, but shown for completeness)
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

// PinPage (No changes from fix in previous step, shown for completeness)
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

	if frameIdx, exists := bm.pageMap[entry]; exists {
		frame := bm.frames[frameIdx]
		frame.Pin()
		bm.replacementPolicy.RecordAccess(frameIdx)
		bm.poolMutex.Unlock()
		return Page{Data: frame.Data}, nil
	}

	for attempts := 0; attempts < bm.config.BufferSize+1; attempts++ {
		targetFrameIdx := bm.findFreeFrame_nolock()
		if targetFrameIdx != -1 {
			frame := bm.frames[targetFrameIdx]
			if err := bm.readPage_nolock(btreeID, pageID, frame.Data); err != nil {
				bm.frames[targetFrameIdx] = NewPageFrame(defaultPageSize)
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

		victimFrameIdx := bm.replacementPolicy.FindVictim(bm.frames)
		if victimFrameIdx == -1 {
			bm.poolMutex.Unlock()
			return Page{}, ErrBufferFull
		}

		victimFrame := bm.frames[victimFrameIdx]
		var victimEntry BufferPoolEntry
		foundVictimInMap := false
		for e, idx := range bm.pageMap {
			if idx == victimFrameIdx {
				victimEntry = e
				foundVictimInMap = true
				break
			}
		}
		if !foundVictimInMap {
			bm.poolMutex.Unlock()
			return Page{}, fmt.Errorf("internal error: PinPage victim frame %d not in pageMap", victimFrameIdx)
		}

		if victimFrame.IsDirty() {
			errFlush := bm.flushPage_nolock(victimEntry.BTreeID, victimEntry.PageID, victimFrameIdx, false)
			if errFlush != nil {
				bm.poolMutex.Unlock()
				return Page{}, fmt.Errorf("PinPage: failed to flush victim page %s:%d for eviction: %w", victimEntry.BTreeID, victimEntry.PageID, errFlush)
			}
			if victimFrame.IsDirty() {
				log.Printf("BufferManager: PinPage: Victim %s:%d (Frame %d) flush deferred. Frame remains in use. Attempt: %d. Seeking another victim.",
					victimEntry.BTreeID, victimEntry.PageID, victimFrameIdx, attempts+1)
				bm.replacementPolicy.RecordAccess(victimFrameIdx)
				if attempts == bm.config.BufferSize {
					log.Printf("BufferManager: PinPage: Max attempts reached to find frame for %s:%d due to deferrals/pins.", btreeID, pageID)
				}
				continue
			}
		}
		delete(bm.pageMap, victimEntry)
		targetFrameIdx = victimFrameIdx
		frame := bm.frames[targetFrameIdx]
		if err := bm.readPage_nolock(btreeID, pageID, frame.Data); err != nil {
			bm.frames[targetFrameIdx] = NewPageFrame(defaultPageSize) // Reset on error
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

// CloseBTree (No changes from Phase 4, but shown for completeness)
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

// Shutdown (No changes from Phase 4, but shown for completeness)
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

// --- Unchanged BTree utility methods from user's original file ---
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
