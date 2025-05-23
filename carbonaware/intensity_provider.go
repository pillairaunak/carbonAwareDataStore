package carbonaware

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
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

// --- MockIntensityProvider Start ---

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

// --- MockIntensityProvider End ---

// --- APIIntensityProvider Start ---

const (
	// DefaultLowCarbonIntensityThreshold defines the gCO2eq/kWh value below which carbon intensity is considered low.
	// This value can be adjusted based on regional data or specific requirements.
	// For example, ElectricityMap often uses a gradient, but a common heuristic for "low" could be < 100-150.
	DefaultLowCarbonIntensityThreshold = 100.0
)

// APIDataResponse matches the structure of the JSON response from your Python API's /data endpoint.
// It specifically targets the `carbon_data` field.
type APIDataResponse struct {
	CarbonData string
	// Add other fields like pricing_data, forecasting_data if needed elsewhere,
	// but for intensity, only carbon_data is critical.
}

// APIIntensityProvider implements IntensityProvider by fetching data from a specified HTTP endpoint.
type APIIntensityProvider struct {
	apiEndpointURL     string
	httpClient         *http.Client
	lowCarbonThreshold float64 // Threshold to determine if intensity is low.
	region             string  // Region this provider is configured for.
}

// NewAPIIntensityProvider creates a new APIIntensityProvider.
// apiEndpointURL should be the full URL to the /data endpoint (e.g., "http://localhost:8000/carbon-intensity").
// region is the region this provider is considered to be serving.
func NewAPIIntensityProvider(apiEndpointURL string, region string, clientTimeout time.Duration) (*APIIntensityProvider, error) {
	if apiEndpointURL == "" {
		return nil, fmt.Errorf("API endpoint URL cannot be empty")
	}
	if region == "" {
		return nil, fmt.Errorf("region cannot be empty for APIIntensityProvider")
	}

	return &APIIntensityProvider{
		apiEndpointURL: apiEndpointURL,
		httpClient: &http.Client{
			Timeout: clientTimeout, // Configurable timeout for HTTP requests
		},
		lowCarbonThreshold: DefaultLowCarbonIntensityThreshold,
		region:             region,
	}, nil
}

// GetCurrentIntensity fetches the current carbon intensity from the configured API endpoint.
func (p *APIIntensityProvider) GetCurrentIntensity(regionQuery string) (IntensitySignal, error) {
	if regionQuery != p.region {
		return IntensitySignal{}, fmt.Errorf("APIIntensityProvider configured for region '%s', but queried for '%s'", p.region, regionQuery)
	}

	req, err := http.NewRequest("GET", p.apiEndpointURL, nil)
	if err != nil {
		return IntensitySignal{}, fmt.Errorf("failed to create request to %s: %w", p.apiEndpointURL, err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return IntensitySignal{}, fmt.Errorf("failed to fetch data from %s: %w", p.apiEndpointURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return IntensitySignal{}, fmt.Errorf("received non-OK HTTP status %d from %s", resp.StatusCode, p.apiEndpointURL)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return IntensitySignal{}, fmt.Errorf("failed to read response body from %s: %w", p.apiEndpointURL, err)
	}

	bodyStr := string(body) // Convert []byte to string
	carbonValue, err := strconv.ParseFloat(bodyStr, 64)
	isLow := carbonValue <= p.lowCarbonThreshold

	signal := IntensitySignal{
		Region:    p.region,
		Timestamp: time.Now(), // Timestamp of when the data was fetched and processed
		Value:     carbonValue,
		IsLow:     isLow,
	}

	log.Printf("[APIIntensityProvider] Fetched intensity for region '%s'. Value: %.2f, IsLow: %t. Endpoint: %s",
		p.region, signal.Value, signal.IsLow, p.apiEndpointURL)

	return signal, nil
}

// --- APIIntensityProvider End ---
