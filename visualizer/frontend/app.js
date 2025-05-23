const mockData = [
  [1747951200, 0.0],
  [1747952100, 0.0],
  [1747953000, 0.0],
  [1747953900, 0.0],
  [1747954800, 0.0],
  [1747955700, 0.0],
  [1747956600, 0.0],
  [1747957500, 0.0],
  [1747958400, 0.0],
  [1747959300, 0.0],
  [1747960200, 0.0],
  [1747961100, 0.0],
  [1747962000, 0.0],
  [1747962900, 0.0],
  [1747963800, 0.0],
  [1747964700, 0.0],
  [1747965600, 0.0],
  [1747966500, 0.0],
  [1747967400, 0.0],
  [1747968300, 0.2],
  [1747969200, 47.0],
  [1747970100, 171.7],
  [1747971000, 467.7],
  [1747971900, 926.0],
  [1747972800, 1621.3],
  [1747973700, 2572.7],
  [1747974600, 3809.2],
  [1747975500, 5341.0],
  [1747976400, 7132.4],
  [1747977300, 9112.3],
  [1747978200, 11231.8],
  [1747979100, 13409.1],
  [1747980000, 15584.4],
  [1747980900, 17727.3],
  [1747981800, 19769.6],
  [1747982700, 21666.2],
  [1747983600, 23396.4],
  [1747984500, 24931.4],
  [1747985400, 26353.7],
  [1747986300, 27585.0],
  [1747987200, 28642.2],
  [1747988100, 29557.6],
  [1747989000, 30348.4],
  [1747989900, 31056.2],
  [1747990800, 31627.6],
  [1747991700, 32073.1],
  [1747992600, 32437.3],
  [1747993500, 32787.5],
  [1747994400, 33072.6],
  [1747995300, 33289.3],
  [1747996200, 33424.9],
  [1747997100, 33538.9],
  [1747998000, 33610.8],
  [1747998900, 33589.9],
  [1747999800, 33491.0],
  [1748000700, 33309.1],
  [1748001600, 33058.5],
  [1748002500, 32730.1],
  [1748003400, 32298.5],
  [1748004300, 31721.3],
  [1748005200, 31046.2],
  [1748006100, 30263.6],
  [1748007000, 29352.2],
  [1748007900, 28368.0],
  [1748008800, 27235.1],
  [1748009700, 26009.8],
  [1748010600, 24701.6],
  [1748011500, 23265.8],
  [1748012400, 21761.0],
  [1748013300, 20162.2],
  [1748014200, 18481.3],
  [1748015100, 16683.9],
  [1748016000, 14831.5],
  [1748016900, 12916.3],
  [1748017800, 11010.3],
  [1748018700, 9184.0],
  [1748019600, 7478.1],
  [1748020500, 5909.4],
  [1748021400, 4581.6],
  [1748022300, 3477.6],
  [1748023200, 2526.1],
  [1748024100, 1704.3],
  [1748025000, 1016.6],
  [1748025900, 474.4],
  [1748026800, 164.2],
  [1748027700, 54.4],
  [1748028600, 23.1],
  [1748029500, 0.0],
  [1748030400, 0.0],
  [1748031300, 0.0],
  [1748032200, 0.0],
  [1748033100, 0.0],
  [1748034000, 0.0],
  [1748034900, 0.0],
  [1748035800, 0.0],
  [1748036700, 0.0]
];

document.addEventListener('DOMContentLoaded', () => {
  const API_URL = 'http://localhost:8081/status'; // Default, ensure this matches your Go server port from main.go
  const FORECAST_URL = 'http://localhost:8000/carbon-intensity-forecast'

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
  const graphElement = document.getElementById('graph');

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

        carbonIntensityCardEl.classList.remove('carbon-intensity-low', 'carbon-intensity-high');
        if (data.carbonSignal.Value !== -1) {
          // Only apply class if value is valid
          carbonIntensityCardEl.classList.add(
            data.carbonSignal.IsLow ? 'carbon-intensity-low' : 'carbon-intensity-high'
          );
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
      console.error('Error fetching or updating status:', error);
      carbonIntensityTextEl.textContent = 'Error';
      deferredQueueSizeEl.textContent = 'Size: Error';
      // ... set other fields to error or ---
      connectionStatusEl.textContent = 'Status: Error ❌ (Could not connect to server)';
      connectionStatusEl.className = 'error';
    }
  }

  async function fetchForecastDataAndUpdate() {
    const response = await fetch(FORECAST_URL);
    const data = await response.json();

    const points = data.forecast.map((entry) => {
      const value = entry.value;
      return [Number(entry.timestamp), value];
    });

    addGraph(
      graphElement,
      points, // curently mocked data,
      300,
      150,
      20
    );

    const epoch = Number(new Date()) * 1e-3;
    const value = points.find(([e, v]) => e > epoch);

    carbonIntensityValueEl.textContent = `Value: ${value !== undefined ? value[1].toFixed(2) : '---'} gCO2eq/kWh`;

    return points;
  }

  // Fetch data every 2 seconds
  const intervalId = setInterval(fetchDataAndUpdate, 2000);
  fetchDataAndUpdate(); // Initial call to populate data immediately

  const forecastIntervalId = setInterval(fetchForecastDataAndUpdate, 2000);
  fetchForecastDataAndUpdate();

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
