/*package main

import (
	"flag"
	"fmt"
	"log"
	"minibtreestore/carbonaware"   // For manual mock control if needed
	"minibtreestore/storage/btree" // Import for BTree operations
	"minibtreestore/storage/buffer"
	"minibtreestore/visualizer" // Visualizer package
	"os"
	"os/signal"
	"strings" // For checking error messages
	"syscall"
	"time"
)

func main() {
	// Configuration Flags
	dir := flag.String("dir", "./minibtree_data", "Storage directory for BTree files")
	bufferSize := flag.Int("buffersize", 100, "Number of pages in the buffer pool")
	carbonAware := flag.Bool("carbonaware", false, "Enable carbon-aware mode")
	carbonRegion := flag.String("carbonregion", "europe-west6", "Carbon intensity region (e.g., Zurich)")
	deferredInterval := flag.Duration("interval", 30*time.Second, "Interval for deferred flush check")
	vizPort := flag.String("vizport", ":8081", "Port for the visualizer HTTP server (e.g., :8081). Set to empty to disable.")
	mockHighIntensity := flag.Bool("mockhigh", false, "Start mock carbon intensity as HIGH (if carbonaware is true)")

	// Workload Flags
	numInserts := flag.Int("inserts", 0, "Number of key-value pairs to insert for demo workload")
	btreeNameDemo := flag.String("btreename", "demotree", "Name of the BTree for demo operations")

	flag.Parse()

	log.Println("--- MiniBTreeStore Starting ---")

	// Setup BufferManagerConfig
	bmConfigOptions := []buffer.Option{
		buffer.WithDirectory(*dir),
		buffer.WithBufferSize(*bufferSize),
		buffer.WithCarbonAware(*carbonAware),
		buffer.WithCarbonRegion(*carbonRegion),
		buffer.WithDeferredFlushInterval(*deferredInterval),
		buffer.WithVisualizerPort(*vizPort),
	}

	bm, err := buffer.NewBufferManager(bmConfigOptions...)
	if err != nil {
		log.Fatalf("Failed to create BufferManager: %v", err)
	}
	log.Println("BufferManager initialized.")

	currentConfig := bm.GetConfig() // Get the actual config from BM

	// Manually set initial mock intensity if flag is set
	if currentConfig.CarbonAware {
		if provider := bm.GetIntensityProvider(); provider != nil {
			if mockProvider, ok := provider.(*carbonaware.MockIntensityProvider); ok {
				if *mockHighIntensity {
					mockProvider.SetIntensity(false, 400.0) // isLow=false, value=high
				} else {
					mockProvider.SetIntensity(true, 50.0) // isLow=true, value=low
				}
			} else {
				log.Println("Warning: Intensity provider is not the expected MockIntensityProvider type for manual setup.")
			}
		} else {
			log.Println("Warning: Carbon-aware mode is ON but no intensity provider was set in BufferManager.")
		}
	}

	// Start Visualizer Server if a port is configured
	if currentConfig.VisualizerPort != "" {
		visualizer.StartVisualizerServer(bm, currentConfig.VisualizerPort)
	} else {
		log.Println("Visualizer port not configured, server will not start.")
	}

	// --- Actual Demo Workload ---
	if *numInserts > 0 {
		log.Printf("Performing actual demo workload: %d inserts into BTree '%s'\n", *numInserts, *btreeNameDemo)

		// Ensure BTree file exists (CreateBTree is idempotent-like if file exists, or handles error)
		errCreate := bm.CreateBTree(*btreeNameDemo)
		if errCreate != nil {
			// Check if the error is because the BTree (file) already exists
			// This check might need to be more robust depending on CreateBTree's exact error messages
			if !strings.Contains(strings.ToLower(errCreate.Error()), "exists") {
				log.Printf("Could not create BTree file '%s' for demo: %v. Attempting to open anyway.", *btreeNameDemo, errCreate)
			} else {
				log.Printf("BTree file '%s' already exists or was just created.", *btreeNameDemo)
			}
		} else {
			log.Printf("BTree file '%s' created for demo.", *btreeNameDemo)
		}

		// Open the BTree (NewBTreePersistent handles initialization logic)
		tree, errOpen := btree.NewBTreePersistent(bm, *btreeNameDemo)
		if errOpen != nil {
			log.Fatalf("Failed to open/initialize BTree '%s' for demo: %v", *btreeNameDemo, errOpen)
		}
		log.Printf("BTree '%s' opened for demo.", *btreeNameDemo)

		startTime := time.Now()
		for i := 1; i <= *numInserts; i++ {
			key := uint64(i * 10)    // Example key
			value := uint64(i * 100) // Example value

			// For testing deferred flushes, sometimes an update is better than pure inserts
			// as inserts might fill pages leading to splits, while updates dirty existing pages.
			// For simplicity, we'll stick to Insert.
			if errInsert := tree.Insert(key, value); errInsert != nil {
				log.Printf("Demo Insert Error for key %d: %v", key, errInsert)
				// Optionally break or continue
			}

			if i%max(1, (*numInserts/20)) == 0 { // Log progress periodically
				log.Printf("Demo: Inserted %d/%d keys...", i, *numInserts)
				// Brief pause to allow logs/visualizer to be observed,
				// and to allow background tasks (like carbon watchdog) a chance to run.
				// Adjust sleep based on -inserts value for better demo flow.
				time.Sleep(50 * time.Millisecond)
			}
		}
		duration := time.Since(startTime)
		log.Printf("Demo workload: %d Inserts completed in %v.", *numInserts, duration)

		// Optionally close this specific BTree after operations to see its flushes
		// log.Printf("Closing demo BTree '%s' after workload.", *btreeNameDemo)
		// if errClose := bm.CloseBTree(*btreeNameDemo); errClose != nil {
		//    log.Printf("Error closing demo BTree '%s': %v", *btreeNameDemo, errClose)
		// }
	} else {
		log.Println("No demo workload specified (use -inserts flag). Application will idle.")
	}

	log.Println("Application started and idling (if no workload). Press Ctrl+C to exit.")

	// Graceful Shutdown Handling
	quitSignal := make(chan os.Signal, 1)
	signal.Notify(quitSignal, syscall.SIGINT, syscall.SIGTERM)
	<-quitSignal // Block until a signal is received

	log.Println("--- MiniBTreeStore Shutting Down ---")
	bm.Shutdown() // Call the BufferManager's shutdown method
	log.Println("Shutdown complete.")
	fmt.Println("Exited.")
}

// max returns the larger of x or y.
// Needed because numInserts/20 can be 0 if numInserts is small, causing division by zero in modulo.
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}*/

package main

import (
	"bufio" // New: For reading stdin
	"errors"
	"flag"
	"fmt"
	"log"
	"minibtreestore/carbonaware"
	"minibtreestore/storage/btree"
	"minibtreestore/storage/buffer"
	"minibtreestore/visualizer"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Configuration Flags
	dir := flag.String("dir", "./minibtree_data", "Storage directory for BTree files")
	bufferSize := flag.Int("buffersize", 100, "Number of pages in the buffer pool")
	carbonAware := flag.Bool("carbonaware", false, "Enable carbon-aware mode")
	carbonRegion := flag.String("carbonregion", "europe-west6", "Carbon intensity region (e.g., Zurich)")
	deferredInterval := flag.Duration("interval", 30*time.Second, "Interval for deferred flush check")
	vizPort := flag.String("vizport", ":8081", "Port for the visualizer HTTP server (e.g., :8081). Set to empty to disable.")
	mockHighIntensity := flag.Bool("mockhigh", false, "Start mock carbon intensity as HIGH (if carbonaware is true)")

	// Workload Flags
	numInserts := flag.Int("inserts", 0, "Number of key-value pairs to insert for demo workload")
	btreeNameDemo := flag.String("btreename", "demotree", "Name of the BTree for demo operations")

	flag.Parse()

	log.Println("--- MiniBTreeStore Starting ---")

	// Setup BufferManagerConfig
	bmConfigOptions := []buffer.Option{
		buffer.WithDirectory(*dir),
		buffer.WithBufferSize(*bufferSize),
		buffer.WithCarbonAware(*carbonAware),
		buffer.WithCarbonRegion(*carbonRegion),
		buffer.WithDeferredFlushInterval(*deferredInterval),
		buffer.WithVisualizerPort(*vizPort),
	}

	bm, err := buffer.NewBufferManager(bmConfigOptions...)
	if err != nil {
		log.Fatalf("Failed to create BufferManager: %v", err)
	}
	log.Println("BufferManager initialized.")

	currentConfig := bm.GetConfig()

	// Manually set initial mock intensity if flag is set AND carbon aware is enabled
	if currentConfig.CarbonAware {
		if provider := bm.GetIntensityProvider(); provider != nil {
			if mockProvider, ok := provider.(*carbonaware.MockIntensityProvider); ok {
				if *mockHighIntensity {
					mockProvider.SetIntensity(false, 400.0) // isLow=false, value=high
				} else {
					// Default initial state of MockIntensityProvider is already LOW
					// So, no explicit SetIntensity(true, 50.0) needed here if not mockhigh
					// but we can log its initial state from the provider itself for clarity if needed.
					// initialSignal, _ := mockProvider.GetCurrentIntensity(currentConfig.CarbonRegion)
					// log.Printf("[CarbonMock] Initial actual state: %s (Value: %.2f gCO2eq/kWh)",
					//    	iif(initialSignal.IsLow, "LOW 🟢", "HIGH 🔴").(string), initialSignal.Value)

				}
			} else {
				log.Println("Warning: Intensity provider is not the MockIntensityProvider type; cannot apply -mockhigh or manual control.")
			}
		} else {
			log.Println("Warning: Carbon-aware mode is ON but no intensity provider was set in BufferManager.")
		}
	}

	// Start Visualizer Server if a port is configured
	if currentConfig.VisualizerPort != "" {
		visualizer.StartVisualizerServer(bm, currentConfig.VisualizerPort)
	} else {
		log.Println("Visualizer port not configured, server will not start.")
	}

	// --- Actual Demo Workload ---
	if *numInserts > 0 {
		log.Printf("Performing actual demo workload: %d inserts into BTree '%s'\n", *numInserts, *btreeNameDemo)
		errCreate := bm.CreateBTree(*btreeNameDemo)
		if errCreate != nil {
			if !strings.Contains(strings.ToLower(errCreate.Error()), "exists") &&
				!strings.Contains(strings.ToLower(errCreate.Error()), "already open") { // Handle if already exists or open
				log.Printf("Could not create BTree file '%s' for demo: %v. Attempting to open anyway.", *btreeNameDemo, errCreate)
			} else {
				log.Printf("BTree file '%s' already exists or was just created.", *btreeNameDemo)
			}
		} else {
			log.Printf("BTree file '%s' created for demo.", *btreeNameDemo)
		}

		tree, errOpen := btree.NewBTreePersistent(bm, *btreeNameDemo)
		if errOpen != nil {
			log.Fatalf("Failed to open/initialize BTree '%s' for demo: %v", *btreeNameDemo, errOpen)
		}
		log.Printf("BTree '%s' opened for demo.", *btreeNameDemo)

		startTime := time.Now()
		for i := 1; i <= *numInserts; i++ {
			key := uint64(i * 10)
			value := uint64(i * 100)
			if errInsert := tree.Insert(key, value); errInsert != nil {
				log.Printf("Demo Insert Error for key %d: %v", key, errInsert)
				// For the demo, we might want to stop if inserts start failing significantly
				// due to ErrBufferFull to make the demo clearer.
				if errors.Is(errInsert, buffer.ErrBufferFull) {
					log.Println("Stopping demo workload due to ErrBufferFull from BTree Insert.")
					break
				}
			}
			if i%max(1, (*numInserts/20)) == 0 {
				log.Printf("Demo: Inserted %d/%d keys...", i, *numInserts)
				time.Sleep(50 * time.Millisecond)
			}
		}
		duration := time.Since(startTime)
		log.Printf("Demo workload: %d Inserts attempted/completed in %v.", *numInserts, duration)
	} else {
		log.Println("No demo workload specified (use -inserts flag). Application will idle.")
	}

	// --- Interactive Mock Intensity Control ---
	if currentConfig.CarbonAware {
		if provider := bm.GetIntensityProvider(); provider != nil {
			if mockProvider, ok := provider.(*carbonaware.MockIntensityProvider); ok {
				log.Println("Interactive mock control enabled: Press 'L' for LOW intensity, 'H' for HIGH intensity, then Enter.")
				go func() {
					reader := bufio.NewReader(os.Stdin)
					for {
						fmt.Print("Set mock intensity (L/H): ")
						input, _ := reader.ReadString('\n')
						char := strings.TrimSpace(input)
						if len(char) > 0 {
							switch char[0] {
							case 'l', 'L':
								mockProvider.SetIntensity(true, 50.0) // true for isLow
							case 'h', 'H':
								mockProvider.SetIntensity(false, 400.0) // false for isLow
							default:
								log.Println("Invalid input for mock intensity. Use 'L' or 'H'.")
							}
						}
					}
				}()
			}
		}
	}

	log.Println("Application running. Press Ctrl+C to exit.")

	// Graceful Shutdown Handling
	quitSignal := make(chan os.Signal, 1)
	signal.Notify(quitSignal, syscall.SIGINT, syscall.SIGTERM)
	<-quitSignal

	log.Println("--- MiniBTreeStore Shutting Down ---")
	bm.Shutdown()
	log.Println("Shutdown complete.")
	fmt.Println("Exited.")
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// Helper for conditional string, not strictly needed if just logging directly in SetIntensity
// func iif(condition bool, trueVal, falseVal interface{}) interface{} {
// 	if condition {
// 		return trueVal
// 	}
// 	return falseVal
// }
