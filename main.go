package main

import (
	"bufio"
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
	carbonRegion := flag.String("carbonregion", "europe-west6", "Carbon intensity region (e.g., Zurich)") //
	carbonAPIEndpoint := flag.String("carbonapiurl", "", "URL for the carbon intensity API (e.g., http://localhost:8000/data)")
	carbonAPIClientTimeout := flag.Duration("carbonapitimeout", 10*time.Second, "Timeout for the carbon API client")
	deferredInterval := flag.Duration("interval", 30*time.Second, "Interval for deferred flush check")                                          //
	vizPort := flag.String("vizport", ":8081", "Port for the visualizer HTTP server (e.g., :8081). Set to empty to disable.")                   //
	mockHighIntensity := flag.Bool("mockhigh", false, "Start mock carbon intensity as HIGH (if carbonaware is true and mock provider is used)") //

	// Workload Flags
	numInserts := flag.Int("inserts", 0, "Number of key-value pairs to insert for demo workload")  //
	btreeNameDemo := flag.String("btreename", "demotree", "Name of the BTree for demo operations") //

	flag.Parse()

	log.Println("--- MiniBTreeStore Starting ---")

	// Setup BufferManagerConfig
	bmConfigOptions := []buffer.Option{
		buffer.WithDirectory(*dir),             //
		buffer.WithBufferSize(*bufferSize),     //
		buffer.WithCarbonAware(*carbonAware),   //
		buffer.WithCarbonRegion(*carbonRegion), //
		buffer.WithCarbonAPIEndpoint(*carbonAPIEndpoint),
		buffer.WithCarbonAPIClientTimeout(*carbonAPIClientTimeout),
		buffer.WithDeferredFlushInterval(*deferredInterval), //
		buffer.WithVisualizerPort(*vizPort),                 //
	}

	bm, err := buffer.NewBufferManager(bmConfigOptions...)
	if err != nil {
		log.Fatalf("Failed to create BufferManager: %v", err)
	}
	log.Println("BufferManager initialized.")

	currentConfig := bm.GetConfig() //

	// Manually set initial mock intensity if flag is set AND carbon aware is enabled
	// AND if the provider is indeed a MockIntensityProvider (e.g. API endpoint wasn't set or failed)
	if currentConfig.CarbonAware { //
		if provider := bm.GetIntensityProvider(); provider != nil { //
			if mockProvider, ok := provider.(*carbonaware.MockIntensityProvider); ok { //
				if *mockHighIntensity { //
					mockProvider.SetIntensity(false, 400.0) // isLow=false, value=high //
				} else {
					// Default initial state of MockIntensityProvider is already LOW
					// log.Printf("[CarbonMock] Mock provider in use. Initial state: LOW 🟢 (Value: 50.0 gCO2eq/kWh by default or as set by mockhigh=false)")
				}
				log.Println("Using MockIntensityProvider. Manual mock control (L/H) will be available.")
			} else if _, apiOk := provider.(*carbonaware.APIIntensityProvider); apiOk {
				log.Println("Using APIIntensityProvider. Manual mock control (L/H) will NOT be available.")
				if *mockHighIntensity {
					log.Println("Note: -mockhigh flag has no effect when APIIntensityProvider is active.")
				}
			} else {
				log.Println("Warning: Intensity provider is of an unknown type; cannot apply -mockhigh or determine manual control availability.") //
			}
		} else {
			log.Println("Warning: Carbon-aware mode is ON but no intensity provider was set in BufferManager.") //
		}
	}

	// Start Visualizer Server if a port is configured
	if currentConfig.VisualizerPort != "" { //
		visualizer.StartVisualizerServer(bm, currentConfig.VisualizerPort) //
	} else {
		log.Println("Visualizer port not configured, server will not start.") //
	}

	// --- Actual Demo Workload ---
	if *numInserts > 0 { //
		log.Printf("Performing actual demo workload: %d inserts into BTree '%s'\n", *numInserts, *btreeNameDemo) //
		errCreate := bm.CreateBTree(*btreeNameDemo)                                                              //
		if errCreate != nil {                                                                                    //
			if !strings.Contains(strings.ToLower(errCreate.Error()), "exists") && //
				!strings.Contains(strings.ToLower(errCreate.Error()), "already open") { //
				log.Printf("Could not create BTree file '%s' for demo: %v. Attempting to open anyway.", *btreeNameDemo, errCreate) //
			} else {
				log.Printf("BTree file '%s' already exists or was just created.", *btreeNameDemo) //
			}
		} else {
			log.Printf("BTree file '%s' created for demo.", *btreeNameDemo) //
		}

		tree, errOpen := btree.NewBTreePersistent(bm, *btreeNameDemo) //
		if errOpen != nil {                                           //
			log.Fatalf("Failed to open/initialize BTree '%s' for demo: %v", *btreeNameDemo, errOpen) //
		}
		log.Printf("BTree '%s' opened for demo.", *btreeNameDemo) //

		startTime := time.Now()             //
		for i := 1; i <= *numInserts; i++ { //
			key := uint64(i * 10)                                       //
			value := uint64(i * 100)                                    //
			if errInsert := tree.Insert(key, value); errInsert != nil { //
				log.Printf("Demo Insert Error for key %d: %v", key, errInsert) //
				if errors.Is(errInsert, buffer.ErrBufferFull) {                //
					log.Println("Stopping demo workload due to ErrBufferFull from BTree Insert.") //
					break
				}
			}
			if i%max(1, (*numInserts/20)) == 0 { //
				log.Printf("Demo: Inserted %d/%d keys...", i, *numInserts) //
				time.Sleep(50 * time.Millisecond)                          //
			}
		}
		duration := time.Since(startTime)                                                         //
		log.Printf("Demo workload: %d Inserts attempted/completed in %v.", *numInserts, duration) //
	} else {
		log.Println("No demo workload specified (use -inserts flag). Application will idle.") //
	}

	// --- Interactive Mock Intensity Control ---
	// This should only be enabled if MockIntensityProvider is actually in use.
	if currentConfig.CarbonAware { //
		if provider := bm.GetIntensityProvider(); provider != nil { //
			if mockProvider, ok := provider.(*carbonaware.MockIntensityProvider); ok { //
				log.Println("Interactive mock control enabled: Press 'L' for LOW intensity, 'H' for HIGH intensity, then Enter.") //
				go func() {                                                                                                       //
					reader := bufio.NewReader(os.Stdin) //
					for {                               //
						fmt.Print("Set mock intensity (L/H): ") //
						input, _ := reader.ReadString('\n')     //
						char := strings.TrimSpace(input)        //
						if len(char) > 0 {                      //
							switch char[0] { //
							case 'l', 'L': //
								mockProvider.SetIntensity(true, 50.0) // true for isLow //
							case 'h', 'H': //
								mockProvider.SetIntensity(false, 400.0) // false for isLow //
							default: //
								log.Println("Invalid input for mock intensity. Use 'L' or 'H'.") //
							}
						}
					}
				}()
			}
		}
	}

	log.Println("Application running. Press Ctrl+C to exit.") //

	// Graceful Shutdown Handling
	quitSignal := make(chan os.Signal, 1)                      //
	signal.Notify(quitSignal, syscall.SIGINT, syscall.SIGTERM) //
	<-quitSignal                                               //

	log.Println("--- MiniBTreeStore Shutting Down ---") //
	bm.Shutdown()                                       //
	log.Println("Shutdown complete.")                   //
	fmt.Println("Exited.")                              //
}

func max(x, y int) int { //
	if x > y { //
		return x
	}
	return y //
}
