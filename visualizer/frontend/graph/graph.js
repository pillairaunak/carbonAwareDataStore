/**
 * Returns a color value for a normalised number to
 * @param {number} n
 * @returns rgb color string
 */
const getGradientColorForValue = (n) => {
  const h = Math.min(1.0, Math.max(n, 0.0)) * 90;
  const s = 100;
  const l = 50;
  const a = 1 - n;
  return `hsla(${h}deg, ${s}%, ${l}%, ${a})`;
};

/**
 * Embeds gradients for each pathpair
 * @param {SVGAElement} graphElement
 * @param {[number, number][]} points - unitized numbers
 * @param {number} height
 * @param {number[]} normalisedValues
 */
const addGradients = (graphElement, points, height, normalisedValues) => {
  // adding the grid background
  for (let i = 0; i < points.length - 1; i++) {
    const [x0, y0] = points[i];
    const [x1, y1] = points[i + 1];

    // Create gradient
    const gradientId = `myGradient-${i}`;
    const gradient = document.createElementNS('http://www.w3.org/2000/svg', 'linearGradient');
    gradient.setAttribute('id', gradientId);
    gradient.setAttribute('x1', '0%');
    gradient.setAttribute('y1', '0%');
    gradient.setAttribute('x2', '100%');
    gradient.setAttribute('y2', '0%');

    // Add color stops
    const stop1 = document.createElementNS('http://www.w3.org/2000/svg', 'stop');
    stop1.setAttribute('offset', '0%');
    stop1.setAttribute('stop-color', getGradientColorForValue(normalisedValues[i])); // side A

    const stop2 = document.createElementNS('http://www.w3.org/2000/svg', 'stop');
    stop2.setAttribute('offset', '100%');
    stop2.setAttribute('stop-color', getGradientColorForValue(normalisedValues[i + 1])); // side B

    gradient.appendChild(stop1);
    gradient.appendChild(stop2);
    graphElement.appendChild(gradient);

    // Create path
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('id', `path-${i}`);
    const d = `
 M ${x0} ${y0}
 L ${x1} ${y1}
 L ${x1} ${height}
 L ${x0} ${height}
 Z
 `;
    path.setAttribute('d', d.trim());
    path.setAttribute('fill', `url(#${gradientId})`);

    graphElement.appendChild(path);
  }
};

/**
 * Embeds a path
 * @param {SVGAElement} graphElement
 * @param {[number, number][]} points
 */
const addPath = (graphElement, points) => {
  const newElement = document.createElementNS('http://www.w3.org/2000/svg', 'path'); //Create a path in SVG's namespace

  // Convert points to SVG path string
  const pathData = points.map(([x, y], index) => `${index === 0 ? 'M' : 'L'} ${x} ${y}`).join(' ');

  // Create SVG path element
  newElement.setAttribute('d', pathData);
  newElement.setAttribute('stroke', 'blue');
  newElement.setAttribute('stroke-width', '2');
  newElement.setAttribute('fill', 'none');
  graphElement.appendChild(newElement);
};

/**
 * Embeds hoveralble and vertical lines
 * @param {SVGAElement} graphElement
 * @param {[number, number][]} points
 * @param {string[]} labels
 * @param {number} height
 */
const addHoverable = (graphElement, points, labels, height) => {
  points.forEach(([x, y], i, arr) => {
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', x);
    line.setAttribute('y1', 0);
    line.setAttribute('x2', x);
    line.setAttribute('y2', height);
    line.setAttribute('stroke', 'gray');
    line.setAttribute('stroke-dasharray', '4');
    line.setAttribute('visibility', 'hidden');
    graphElement.appendChild(line); // Circle point

    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    circle.setAttribute('cx', x);
    circle.setAttribute('cy', y);
    circle.setAttribute('r', 5);
    circle.setAttribute('fill', 'transparent');
    circle.style.cursor = 'pointer'; // Tooltip

    const tooltip = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    tooltip.setAttribute('x', x + 2);
    tooltip.setAttribute('y', y + 2);
    tooltip.setAttribute('fill', 'black');
    tooltip.setAttribute('font-size', '12');
    tooltip.setAttribute('text-anchor', i > arr.length / 2 ? 'end' : 'start');
    tooltip.textContent = labels[i];
    tooltip.setAttribute('visibility', 'hidden');
    graphElement.appendChild(tooltip); // Hover events

    circle.addEventListener('mouseenter', () => {
      tooltip.setAttribute('visibility', 'visible');
      line.setAttribute('visibility', 'visible');
    });
    circle.addEventListener('mouseleave', () => {
      tooltip.setAttribute('visibility', 'hidden');
      line.setAttribute('visibility', 'hidden');
    });
    graphElement.appendChild(circle);
  });
};

const getHour = (date) => date.getUTCHours();
const getMinutes = (date) => date.getUTCMinutes();

/**
 * Constructing the labels for the svg
 * @param {[number, number][]} dataPoints
 */
const getLabels = (dataPoints) =>
  dataPoints.map(([epoch, carbonIntensity]) => {
    const date = new Date(epoch * 1e3);
    return `${getHour(date)}:${getMinutes(date)} - ${carbonIntensity.toFixed(2)}`;
  });

/**
 * Constructing the locations for the svg
 * @param {[number, number][]} dataPoints
 * @param {number} width
 * @param {number} height
 * @param {number} tolerance
 */
const getVisualisationPoints = (dataPoints, width, height, tolerance) => {
  const minX = Math.min(...dataPoints.map(([x]) => x));
  const maxX = Math.max(...dataPoints.map(([x]) => x));
  const minY = Math.min(...dataPoints.map(([, y]) => y));
  const maxY = Math.max(...dataPoints.map(([, y]) => y));

  return dataPoints.map(([x, y]) => [
    ((x - minX) * width) / (maxX - minX),
    height - tolerance - ((y - minY) * (height - 2 * tolerance)) / (maxY - minY)
  ]);
};

/**
 * Constructing the labels for the svg
 * @param {[number, number][]} dataPoints
 */
const getNormalisedValues = (dataPoints) => {
  const minY = Math.min(...dataPoints.map(([, y]) => y));
  const maxY = Math.max(...dataPoints.map(([, y]) => y));

  return dataPoints.map(([, y]) => 1 - (y - minY) / (maxY - minY));
};

/**
 * Add Rectangular grid marking what is projected
 * @param {SVGAElement} graphElement
 * @param {[number, number]} dataPoint
 * @param {number} width
 * @param {number} height
 */
const addTranspartentRectangle = (graphElement, dataPoint, width, height) => {
  const [x0] = dataPoint;

  // Create path
  const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  path.setAttribute('id', `transparent_rect`);
  const d = `
 M ${x0} ${0}
 L ${x0} ${height}
 L ${width} ${height}
 L ${width} ${0}
 Z
 `;
  path.setAttribute('d', d.trim());
  path.setAttribute('fill', `rgba(255,255,255,.7)`);

  graphElement.appendChild(path);
};

/**
 * Embeds a grah element into an SVGElement
 * @param {SVGAElement} graphElement
 * @param {[number, number][]} dataPoints
 * @param {number} width
 * @param {number} height
 * @param {number} tolerance
 */
const addGraph = (graphElement, dataPoints, width, height, tolerance) => {
  // clear graphElement;
  graphElement.innerHTML = '';

  const mappedPoints = getVisualisationPoints(dataPoints, width, height, tolerance);
  const normalisedValues = getNormalisedValues(dataPoints);

  const epoch = Number(new Date()) * 1e-3;
  const index = dataPoints.findIndex(([e, v]) => e > epoch);

  addGradients(graphElement, mappedPoints, height, normalisedValues);
  addPath(graphElement, mappedPoints);
  addTranspartentRectangle(graphElement, mappedPoints[index], width, height);
  addHoverable(graphElement, mappedPoints, getLabels(dataPoints), height);
};
