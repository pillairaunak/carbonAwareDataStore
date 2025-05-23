document.addEventListener('DOMContentLoaded', () => {
    const API_URL = 'http://localhost:8081/status'; // Default, ensure this matches your Go server port from main.go

    const carbonIntensityTextEl = document.getElementById('carbon-intensity-text');
    const carbonIntensityValueEl = document.getElementById('carbon-intensity-value');
    const carbonIntensityCardEl = document.getElementById('carbon-intensity-card');
    
    const deferredQueueSizeEl = document.getElementById('deferred-queue-size');
    
    const configCarbonAwareEl = document.getElementById('config-carbon-aware');
    const configCarbonRegionEl = document.getElementById('config-carbon-region');
    
    const flushedNormalEl = document.getElementById('flushed-normal');
    const flushedDeferredEl = document.getElementById('flushed-deferred');
    const flushedForcedEl = document.getElementById('flushed-forced');

    const bufferTotalPagesEl = document.getElementById('buffer-total-pages');
    const bufferCapacityEl = document.getElementById('buffer-capacity');
    const bufferDirtyPagesEl = document.getElementById('buffer-dirty-pages');

    const connectionStatusEl = document.getElementById('connection-status');

    async function fetchDataAndUpdate() {
        try {
            const response = await fetch(API_URL);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();

            // Carbon Intensity
            if (data.carbonSignal) {
                const intensityText = data.carbonSignal.IsLow ? 'LOW 🟢' : 'HIGH 🔴';
                carbonIntensityTextEl.textContent = intensityText;
                carbonIntensityValueEl.textContent = `Value: ${data.carbonSignal.Value !== -1 ? data.carbonSignal.Value.toFixed(2) : '---'} gCO2eq/kWh`;
                
                carbonIntensityCardEl.classList.remove('carbon-intensity-low', 'carbon-intensity-high');
                if (data.carbonSignal.Value !== -1) { // Only apply class if value is valid
                    carbonIntensityCardEl.classList.add(data.carbonSignal.IsLow ? 'carbon-intensity-low' : 'carbon-intensity-high');
                }
            }

            // Deferred Queue
            deferredQueueSizeEl.textContent = `Size: ${data.deferredQueueSize}`;

            // Configuration
            configCarbonAwareEl.textContent = `Carbon Aware: ${data.configCarbonAware ? 'Enabled ✅' : 'Disabled ❌'}`;
            configCarbonRegionEl.textContent = `Region: ${data.configCarbonRegion || 'N/A'}`;
            
            // Flush Counts
            flushedNormalEl.textContent = `Count: ${data.flushedNormal}`;
            flushedDeferredEl.textContent = `Count: ${data.flushedDeferred}`;
            flushedForcedEl.textContent = `Count: ${data.flushedForced}`;

            // Buffer Pool
            bufferTotalPagesEl.textContent = `Pages In Buffer: ${data.totalPagesInBuffer}`;
            bufferCapacityEl.textContent = `${data.currentBufferSize}`; // Assuming currentBufferSize is capacity
            bufferDirtyPagesEl.textContent = `Dirty Pages: ${data.dirtyPagesInBuffer}`;
            
            connectionStatusEl.textContent = 'Status: Connected ✅';
            connectionStatusEl.className = 'connected';

        } catch (error) {
            console.error("Error fetching or updating status:", error);
            carbonIntensityTextEl.textContent = 'Error';
            deferredQueueSizeEl.textContent = 'Size: Error';
            // ... set other fields to error or ---
            connectionStatusEl.textContent = 'Status: Error ❌ (Could not connect to server)';
            connectionStatusEl.className = 'error';
        }
    }

    // Fetch data every 2 seconds
    const intervalId = setInterval(fetchDataAndUpdate, 2000);
    fetchDataAndUpdate(); // Initial call to populate data immediately

    // Optional: Clear interval if the page is hidden to save resources
    // document.addEventListener("visibilitychange", () => {
    //     if (document.visibilityState === "hidden") {
    //         clearInterval(intervalId);
    //     } else {
    //         fetchDataAndUpdate(); // Fetch immediately when visible again
    //         intervalId = setInterval(fetchDataAndUpdate, 2000);
    //     }
    // });
});