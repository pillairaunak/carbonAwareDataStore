package carbonaware

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// IntensitySignal represents the state of carbon intensity at a given time.
type IntensitySignal struct {
	// Region represents the geographical region for which the intensity is relevant.
	Region string
	// Timestamp is when this intensity signal was generated or considered valid.
	Timestamp time.Time
	// Value is the actual carbon intensity, e.g., in gCO2eq/kWh.
	Value float64
	// IsLow is a simple flag indicating if the current intensity is considered low (good for running workloads).
	IsLow bool
}

// IntensityProvider defines the interface for components that can provide carbon intensity information.
type IntensityProvider interface {
	// GetCurrentIntensity fetches the current carbon intensity signal for a given region.
	GetCurrentIntensity(region string) (IntensitySignal, error)
}

// MockIntensityProvider is an implementation of IntensityProvider
// that simulates carbon intensity data, useful for testing and demos.
type MockIntensityProvider struct {
	mu            sync.RWMutex
	currentSignal IntensitySignal
	region        string
}

// NewMockIntensityProvider creates a new MockIntensityProvider.
// It starts with a default low intensity.
func NewMockIntensityProvider(region string) *MockIntensityProvider {
	provider := &MockIntensityProvider{
		region: region,
		currentSignal: IntensitySignal{
			Region:    region,
			Timestamp: time.Now(),
			Value:     50.0, // Default low value
			IsLow:     true,
		},
	}
	log.Printf("[CarbonMock] Initialized for region '%s'. Intensity: LOW 🟢 (Value: %.2f gCO2eq/kWh)",
		provider.region, provider.currentSignal.Value)
	return provider
}

// GetCurrentIntensity returns the current simulated carbon intensity.
func (m *MockIntensityProvider) GetCurrentIntensity(region string) (IntensitySignal, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if region != m.region {
		return IntensitySignal{}, fmt.Errorf("MockIntensityProvider configured for region '%s', but queried for '%s'", m.region, region)
	}
	// Update timestamp to now for freshness of the mock signal
	// Create a new signal instance to avoid race conditions if currentSignal is used elsewhere concurrently
	signalToSend := m.currentSignal
	signalToSend.Timestamp = time.Now()
	return signalToSend, nil
}

// SetIntensity allows manual control over the simulated carbon intensity.
// This is useful for demos and testing.
func (m *MockIntensityProvider) SetIntensity(isLow bool, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentSignal.IsLow = isLow
	m.currentSignal.Value = value
	m.currentSignal.Timestamp = time.Now() // Update timestamp on change

	var statusEmoji string
	if isLow {
		statusEmoji = "LOW 🟢"
	} else {
		statusEmoji = "HIGH 🔴"
	}

	log.Printf("[CarbonMock] Intensity manually set to: %s (Value: %.2f gCO2eq/kWh)",
		statusEmoji, m.currentSignal.Value)
}

// SimulateTimedChanges provides a simple way to make the mock provider
// change its state over time for demo purposes if manual control isn't used.
// This should be run in a separate goroutine.
func (m *MockIntensityProvider) SimulateTimedChanges(interval time.Duration, quit <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial state is already set by NewMockIntensityProvider
	// We need to read the initial state under lock to be safe if SimulateTimedChanges is called very quickly after New
	m.mu.RLock()
	isCurrentlyLow := m.currentSignal.IsLow
	m.mu.RUnlock()

	for {
		select {
		case <-ticker.C:
			isCurrentlyLow = !isCurrentlyLow // Toggle the state
			var newValue float64
			if isCurrentlyLow {
				newValue = 50.0 // Simulate a low value
			} else {
				newValue = 450.0 // Simulate a high value
			}
			m.SetIntensity(isCurrentlyLow, newValue) // Use SetIntensity to get the logging & proper locking
		case <-quit:
			log.Println("[CarbonMock] Simulation of timed changes stopped.")
			return
		}
	}
}
