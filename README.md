```markdown
# Carbon Aware DataStore (Zurich Green IT Hackathon Repo)

[![Go Version](https://img.shields.io/badge/Go-1.18+-blue.svg)](https://golang.org/)

MiniBTreeStore is an experimental key-value (KV) store built in Go, utilizing a B+ Tree data structure for efficient on-disk storage. Its standout feature is a **carbon-aware flushing strategy** designed to reduce the environmental impact of its disk I/O operations.

This project was developed as part of a deep-dive exploration into Green Software Engineering principles, demonstrating how even foundational components like a storage engine can be made more environmentally conscious.

## Quick Start

```bash
# Build the application
go build -o minibtreestore_app .

# Run (Example with carbon-awareness enabled, high intensity start, and visualizer)
./minibtreestore_app -carbonaware=true -mockhigh=true -inserts=500 -buffersize=2 -vizport=:8081 -dir=./test_run_data -interval=1s
```

### Interactive Carbon Intensity Control (During Run)
When the application is running with `-carbonaware=true`:

Press `L` (and Enter) in the terminal to set mock carbon emissions to LOW 🟢.
Press `H` (and Enter) in the terminal to set mock carbon emissions to HIGH 🔴.

### Viewing the Frontend Visualizer
After running the backend application with a `-vizport` specified (e.g., `:8081`):

```bash
# Open this file in your web browser
visualizer/frontend/index.html
```

The dashboard will poll data from `http://localhost:PORT/status` (e.g., `http://localhost:8081/status`).

## ✨ Features
* **Core B+ Tree Operations:** Supports Insert, Lookup, and Scan operations. Delete functionality is also present.
* **Buffer Management:** Implements a buffer manager with an LRU (Least Recently Used) page replacement policy to optimize disk access and improve performance.
* **Carbon-Aware Flushing Strategy:**
    * Monitors (simulated) grid carbon intensity (HIGH 🔴 / LOW 🟢) using a `MockIntensityProvider`.
    * Intelligently defers non-critical disk page flushes when carbon intensity is reported as HIGH.
    * Processes the queue of deferred flushes when carbon intensity transitions to LOW, leveraging "greener" energy periods.
    * Aims to reduce the effective CO2 footprint of disk write operations.
* **Interactive Carbon Simulation:**
    * Allows manual toggling of simulated carbon intensity (HIGH/LOW) during runtime via CLI input when `main.go` is running with carbon-awareness enabled.
* **Real-time Visual Feedback:**
    * **Enhanced CLI Logs:** Detailed console output with emojis (e.g., 🟡 Deferred, 🟢 Low Carbon Flush, ❗Forced Flush) to track the system's carbon-aware decisions and actions.
    * **Web Visualizer Dashboard:** A simple web-based dashboard (`visualizer/frontend/index.html`) polls a backend endpoint (`/status` from `visualizer/server.go`) to display:
        * Current carbon intensity status.
        * Size of the deferred flush queue.
        * Counts of normal, deferred, and forced flushes.
        * Buffer pool statistics.

## 📂 Project Structure
```
minibtreestore/
├── carbonaware/             # Carbon intensity provider logic
│   └── intensity_provider.go
├── main.go                  # Main application entry point with CLI flags
├── storage/
│   ├── btree/               # B+ Tree implementation
│   ├── buffer/              # Buffer manager implementation
│   └── page/                # Page formats and constants
├── test/                    # Go tests (e.g., store_test.go)
├── util/
│   └── loader/              # CSV data loader utility (if used)
└── visualizer/
    ├── frontend/            # HTML, CSS, JS for the web dashboard
    └── server.go            # Backend HTTP server for visualizer data
```

## 🚀 Getting Started (Detailed)

### Prerequisites
* Go (version 1.18 or higher recommended)

### Build
Navigate to the project root directory and run:

```bash
go build -o minibtreestore_app .
```

### Running
The application `main.go` serves as the entry point and accepts several flags to configure its behavior and the demo workload.

**Key Command-Line Flags (see `main.go` for all):**

* `-dir`: Storage directory for B-Tree files (default: `./minibtree_data`).
* `-buffersize`: Number of pages in the buffer pool (default: 100). Try small values like 2 or 5 with high insert counts to observe buffer pressure.
* `-carbonaware`: Enable carbon-aware mode (default: `false`).
* `-carbonregion`: Carbon intensity region for simulation (default: `"europe-west6"`).
* `-interval`: Interval for the carbon watchdog to check deferred queue (default: `30s`). Use shorter intervals like `1s` for faster demo feedback.
* `-mockhigh`: Start with mock carbon intensity as HIGH (default: `false`, starts LOW).
* `-vizport`: Port for the visualizer HTTP server (e.g., `:8081`). Set to empty to disable.
* `-inserts`: Number of key-value pairs to insert for a demo workload.
* `-btreename`: Name of the B-Tree for demo operations.

**Example Run (from Quick Start):**

```bash
./minibtreestore_app -carbonaware=true -mockhigh=true -inserts=500 -buffersize=2 -vizport=:8081 -dir=./test_run_data -interval=1s
```

## 🧪 Testing
The `test/` directory contains Go tests (e.g., `store_test.go`). You can run these using standard Go test commands from the root directory:
```bash
go test ./...
```
To test the carbon-aware features interactively, use the command-line flags provided in `main.go` to set up different scenarios. The CLI logs and web visualizer are key to observing behavior. The "Quick Start" section above provides an example for running and interacting.

## 📝 Carbon-Aware Simulation Note
This project currently uses a `MockIntensityProvider` to simulate grid carbon intensity. This is essential for controlled testing and demonstration of the carbon-aware logic. Future work could involve integrating this with real-world carbon intensity data APIs (e.g., from the Green Software Foundation's Carbon Aware SDK, WattTime, Electricity Maps).

## Contributing


## License

