package carbonaware

import (
	"encoding/json" // New: For parsing API response
	"fmt"
	"io" // New: For reading response body (Go 1.16+)
	"log"
	"net/http" // New: For making HTTP requests
	"sync"
	"time"
)

// IntensitySignal represents the state of carbon intensity at a given time.
// (No changes from before)
type IntensitySignal struct {
	Region    string
	Timestamp time.Time
	Value     float64
	IsLow     bool
}

// IntensityProvider defines the interface for components that can provide carbon intensity information.
// (No changes from before)
type IntensityProvider interface {
	GetCurrentIntensity(region string) (IntensitySignal, error)
}

// --- MockIntensityProvider (Existing Code - No changes from before) ---
type MockIntensityProvider struct {
	mu            sync.RWMutex
	currentSignal IntensitySignal
	region        string
}

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
	log.Printf("[CarbonMock] Initialized for region '%s'. Intensity: LOW :large_green_circle: (Value: %.2f gCO2eq/kWh)",
		provider.region, provider.currentSignal.Value)
	return provider
}
func (m *MockIntensityProvider) GetCurrentIntensity(region string) (IntensitySignal, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.region != "" && region != m.region { // Allow query for specific region if mock is configured for one
		// Or, always return the mock's configured region's data regardless of query.
		// For simplicity, let's assume if mock has a region, query must match or be empty (implying default).
		// If query is empty, use mock's region.
		// This logic can be refined based on desired mock behavior with regions.
	}
	signalToSend := m.currentSignal
	signalToSend.Timestamp = time.Now()
	// If the query region is specified and different from mock's default,
	// you might return an error or a generic signal. For now, it returns its currentSignal.
	if region != "" {
		signalToSend.Region = region // Update signal to reflect queried region if different
	}
	return signalToSend, nil
}
func (m *MockIntensityProvider) SetIntensity(isLow bool, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentSignal.IsLow = isLow
	m.currentSignal.Value = value
	m.currentSignal.Timestamp = time.Now()
	var statusEmoji string
	if isLow {
		statusEmoji = "LOW :large_green_circle:"
	} else {
		statusEmoji = "HIGH :red_circle:"
	}
	log.Printf("[CarbonMock] Intensity manually set to: %s (Value: %.2f gCO2eq/kWh)",
		statusEmoji, m.currentSignal.Value)
}
func (m *MockIntensityProvider) SimulateTimedChanges(interval time.Duration, quit <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	m.mu.RLock()
	isCurrentlyLow := m.currentSignal.IsLow
	m.mu.RUnlock()
	for {
		select {
		case <-ticker.C:
			isCurrentlyLow = !isCurrentlyLow
			var newValue float64
			if isCurrentlyLow {
				newValue = 50.0
			} else {
				newValue = 450.0
			}
			m.SetIntensity(isCurrentlyLow, newValue)
		case <-quit:
			log.Println("[CarbonMock] Simulation of timed changes stopped.")
			return
		}
	}
}

// --- New: ApiIntensityProvider ---
// ApiIntensityData is a struct to help unmarshal the JSON response from your API.
// **IMPORTANT**: Adjust this struct to match the actual JSON response from http://localhost:8000/data
type ApiIntensityData struct {
	ForecastingData float64 `json:"forecasting_data"` // Assuming API provides region
	CarbonIntensity float64 `json:"carbon_data"`      // Assuming this field holds the numeric value
	PricingData     float64 `json:"forecasting_data"` // Assuming API provides unit
	// Add other fields if your API returns more data you might want to use or log
}

// ApiIntensityProvider fetches carbon intensity data from a specified HTTP API.
type ApiIntensityProvider struct {
	client    *http.Client
	apiUrl    string
	threshold float64 // Value below or equal to which intensity is considered "LOW"
	// defaultRegion string  // Can be used if API doesn't provide region or to override
}

// NewApiIntensityProvider creates a new provider that fetches from an API.
// apiUrl: The full URL to the carbon intensity API endpoint.
// threshold: The gCO2eq/kWh (or equivalent unit) value at or below which intensity is considered LOW.
// requestTimeout: Timeout for the HTTP request.
func NewApiIntensityProvider(apiUrl string, threshold float64, requestTimeout time.Duration) (*ApiIntensityProvider, error) {
	if apiUrl == "" {
		return nil, fmt.Errorf("API URL cannot be empty for ApiIntensityProvider")
	}
	return &ApiIntensityProvider{
		client: &http.Client{
			Timeout: requestTimeout,
		},
		apiUrl:    apiUrl,
		threshold: threshold,
	}, nil
}

// GetCurrentIntensity fetches data from the configured API and converts it to an IntensitySignal.
func (p *ApiIntensityProvider) GetCurrentIntensity(queryRegion string) (IntensitySignal, error) {
	// Note: The 'queryRegion' parameter might be used to adjust the API call if the API supports regional data,
	// or it could be used to validate the response if the API returns a region.
	// For this example, we'll assume the apiUrl is already specific or the API handles region implicitly.
	resp, err := p.client.Get(p.apiUrl)
	if err != nil {
		return IntensitySignal{}, fmt.Errorf("ApiIntensityProvider: failed to fetch data from API '%s': %w", p.apiUrl, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body) // Read some of the body for error context
		return IntensitySignal{}, fmt.Errorf("ApiIntensityProvider: API '%s' returned non-OK status %d: %s", p.apiUrl, resp.StatusCode, string(bodyBytes))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return IntensitySignal{}, fmt.Errorf("ApiIntensityProvider: failed to read response body from API '%s': %w", p.apiUrl, err)
	}
	var apiData ApiIntensityData
	if err := json.Unmarshal(body, &apiData); err != nil {
		return IntensitySignal{}, fmt.Errorf("ApiIntensityProvider: failed to parse JSON response from API '%s': %w. Body: %s", p.apiUrl, err, string(body))
	}
	// Construct IntensitySignal
	signal := IntensitySignal{
		Region:    "Hello", // Use region from API data
		Timestamp: time.Now(),
		Value:     apiData.CarbonIntensity,
		IsLow:     apiData.CarbonIntensity <= p.threshold,
	}
	// Log the fetched data for visibility
	log.Printf("[ApiProvider] Fetched from %s: Intensity: %.2f %s,(Threshold: <=%.2f)",
		p.apiUrl, apiData.CarbonIntensity, p.threshold)
	return signal, nil
}
