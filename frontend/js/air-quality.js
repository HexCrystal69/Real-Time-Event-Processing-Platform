/**
 * GRIP — Air Quality Intelligence page
 */
async function initAirQuality() {
  try {
    const data = await GRIP_API.getAirQualityAnalytics();

    renderAQIChart(data.aqi_distribution);
    renderRegionalChart(data.regional_comparisons);
    renderTrendsChart(data.aqi_trends);
    renderHotspotsTable(data.pollution_hotspots);
    await loadAQIForecast();
  } catch (err) {
    GRIP_UI.showError(document.getElementById('aq-content'), err.message);
  }
}

function renderAQIChart(dist) {
  const canvas = document.getElementById('aqi-dist-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'bar',
    data: {
      labels: Object.keys(dist || {}),
      datasets: [{
        label: 'Readings',
        data: Object.values(dist || {}),
        backgroundColor: ['#10b981', '#f59e0b', '#f97316', '#ef4444', '#dc2626', '#64748b'],
      }],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderRegionalChart(regional) {
  const canvas = document.getElementById('aqi-regional-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'bar',
    data: {
      labels: (regional || []).map(r => r.location),
      datasets: [
        {
          label: 'Avg AQI',
          data: (regional || []).map(r => r.avg_aqi),
          backgroundColor: GRIP_CONFIG.CHART_COLORS.airQuality + '88',
        },
        {
          label: 'Max AQI',
          data: (regional || []).map(r => r.max_aqi),
          backgroundColor: GRIP_CONFIG.CHART_COLORS.earthquakes + '66',
        },
      ],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderTrendsChart(byLocation) {
  const canvas = document.getElementById('aqi-trend-chart');
  if (!canvas) return;
  const locations = Object.keys(byLocation || {});
  const colors = ['#10b981', '#06b6d4', '#3b82f6', '#f59e0b', '#ef4444'];

  const datasets = locations.map((loc, i) => ({
    label: loc,
    data: (byLocation[loc] || []).slice(0, 48).reverse().map(o => o.us_aqi),
    borderColor: colors[i % colors.length],
    tension: 0.3,
  }));

  const labels = locations.length > 0
    ? (byLocation[locations[0]] || []).slice(0, 48).reverse().map(o => GRIP_UI.formatTimeShort(o.time))
    : [];

  new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderHotspotsTable(hotspots) {
  const tbody = document.getElementById('aq-hotspots-body');
  if (!tbody) return;
  if (!hotspots?.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No pollution hotspots detected</td></tr>';
    return;
  }
  tbody.innerHTML = hotspots.map(h => `
    <tr>
      <td>${h.location}</td>
      <td>${h.us_aqi}</td>
      <td>${GRIP_UI.riskBadge(h.category === 'Hazardous' ? 'Critical' : h.category === 'Very Poor' ? 'High' : 'Moderate')}</td>
      <td>${GRIP_UI.formatTime(h.time)}</td>
    </tr>
  `).join('');
}

async function loadAQIForecast() {
  const container = document.getElementById('aq-forecast-chart');
  if (!container) return;
  try {
    const data = await GRIP_API.getForecasts({ source: 'air_quality', metric: 'us_aqi', horizon: '24h' });
    if (!data.forecasts?.length) return;

    const byLocation = {};
    data.forecasts.forEach(f => {
      const loc = f.location_name || 'Unknown';
      if (!byLocation[loc]) byLocation[loc] = [];
      byLocation[loc].push(f);
    });

    const locations = Object.keys(byLocation);
    const colors = ['#10b981', '#06b6d4', '#3b82f6', '#f59e0b', '#ef4444'];
    const datasets = locations.map((loc, i) => ({
      label: loc,
      data: byLocation[loc].map(f => f.predicted_value),
      borderColor: colors[i % colors.length],
      tension: 0.3,
    }));

    const canvas = document.createElement('canvas');
    container.appendChild(canvas);
    new Chart(canvas, {
      type: 'line',
      data: {
        labels: byLocation[locations[0]]?.map(f => GRIP_UI.formatTimeShort(f.forecast_time)) || [],
        datasets,
      },
      options: GRIP_UI.chartDefaults(),
    });
  } catch (e) { /* forecast may not be ready */ }
}

document.addEventListener('DOMContentLoaded', initAirQuality);
