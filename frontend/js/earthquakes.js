/**
 * GRIP — Earthquake Intelligence page
 */
async function initEarthquakes() {
  try {
    const data = await GRIP_API.getEarthquakeAnalytics();

    document.getElementById('eq-recent-count').textContent = GRIP_UI.formatNumber(data.recent_count);

    renderMagChart(data.magnitude_distribution);
    renderDepthChart(data.depth_analysis);
    renderTrendChart(data.historical_trends);
    renderRiskChart(data.risk_categories);
    renderEventsTable(data.recent_events);
    await loadEarthquakeForecast();
  } catch (err) {
    GRIP_UI.showError(document.getElementById('eq-content'), err.message);
  }
}

function renderMagChart(dist) {
  const canvas = document.getElementById('mag-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'bar',
    data: {
      labels: Object.keys(dist),
      datasets: [{
        label: 'Events',
        data: Object.values(dist),
        backgroundColor: GRIP_CONFIG.CHART_COLORS.earthquakes + '88',
        borderColor: GRIP_CONFIG.CHART_COLORS.earthquakes,
        borderWidth: 1,
      }],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderDepthChart(dist) {
  const canvas = document.getElementById('depth-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'pie',
    data: {
      labels: Object.keys(dist),
      datasets: [{
        data: Object.values(dist),
        backgroundColor: ['#ef4444', '#f97316', '#f59e0b', '#64748b'],
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { color: GRIP_CONFIG.CHART_COLORS.text } } },
    },
  });
}

function renderTrendChart(trends) {
  const canvas = document.getElementById('trend-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'line',
    data: {
      labels: trends.map(t => t.day?.slice(0, 10)),
      datasets: [{
        label: 'Daily Events',
        data: trends.map(t => t.count),
        borderColor: GRIP_CONFIG.CHART_COLORS.earthquakes,
        backgroundColor: GRIP_CONFIG.CHART_COLORS.earthquakes + '22',
        fill: true,
        tension: 0.3,
      }],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderRiskChart(cats) {
  const canvas = document.getElementById('risk-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'doughnut',
    data: {
      labels: Object.keys(cats),
      datasets: [{
        data: Object.values(cats),
        backgroundColor: ['#10b981', '#f59e0b', '#f97316', '#ef4444'],
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { color: GRIP_CONFIG.CHART_COLORS.text } } },
    },
  });
}

function renderEventsTable(events) {
  const tbody = document.getElementById('eq-events-body');
  if (!tbody) return;
  if (!events.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No recent earthquakes in pipeline</td></tr>';
    return;
  }
  tbody.innerHTML = events.map(e => `
    <tr>
      <td>${e.magnitude ?? '—'}</td>
      <td>${e.depth_km ?? '—'} km</td>
      <td>${GRIP_UI.riskBadge(e.risk_category)}</td>
      <td>${e.latitude?.toFixed(2)}, ${e.longitude?.toFixed(2)}</td>
      <td>${GRIP_UI.formatTime(e.event_time)}</td>
    </tr>
  `).join('');
}

async function loadEarthquakeForecast() {
  const container = document.getElementById('eq-forecast-chart');
  if (!container) return;
  try {
    const data = await GRIP_API.getForecasts({ source: 'earthquakes', metric: 'event_count', horizon: '7d' });
    if (!data.forecasts?.length) {
      container.innerHTML = '<div class="empty-state">Forecast will appear once sufficient historical data is available</div>';
      return;
    }
    const canvas = document.createElement('canvas');
    container.appendChild(canvas);
    new Chart(canvas, {
      type: 'line',
      data: {
        labels: data.forecasts.map(f => f.forecast_time?.slice(0, 10)),
        datasets: [{
          label: 'Predicted Event Count',
          data: data.forecasts.map(f => f.predicted_value),
          borderColor: GRIP_CONFIG.CHART_COLORS.earthquakes,
          backgroundColor: GRIP_CONFIG.CHART_COLORS.earthquakes + '22',
          fill: true,
          tension: 0.3,
        }],
      },
      options: GRIP_UI.chartDefaults(),
    });
  } catch (e) { /* forecast may not be ready yet */ }
}

document.addEventListener('DOMContentLoaded', initEarthquakes);
