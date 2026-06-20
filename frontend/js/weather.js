/**
 * GRIP — Weather Intelligence page
 */
async function initWeather() {
  try {
    const data = await GRIP_API.getWeatherAnalytics();

    renderStormChart(data.storm_risks);
    renderWindChart(data.wind_severity);
    renderTempTrends(data.temperature_trends);
    renderAlertsTable(data.weather_alerts);
    await loadWeatherForecast();
  } catch (err) {
    GRIP_UI.showError(document.getElementById('wx-content'), err.message);
  }
}

function renderStormChart(dist) {
  const canvas = document.getElementById('storm-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'bar',
    data: {
      labels: Object.keys(dist || {}),
      datasets: [{
        label: 'Observations',
        data: Object.values(dist || {}),
        backgroundColor: GRIP_CONFIG.CHART_COLORS.weather + '88',
        borderColor: GRIP_CONFIG.CHART_COLORS.weather,
        borderWidth: 1,
      }],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderWindChart(dist) {
  const canvas = document.getElementById('wind-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'doughnut',
    data: {
      labels: Object.keys(dist || {}),
      datasets: [{ data: Object.values(dist || {}), backgroundColor: ['#10b981', '#06b6d4', '#3b82f6', '#f59e0b', '#ef4444'] }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { color: GRIP_CONFIG.CHART_COLORS.text } } },
    },
  });
}

function renderTempTrends(byLocation) {
  const canvas = document.getElementById('temp-chart');
  if (!canvas) return;
  const locations = Object.keys(byLocation || {});
  const colors = ['#3b82f6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444'];

  const datasets = locations.map((loc, i) => ({
    label: loc,
    data: (byLocation[loc] || []).slice(0, 48).reverse().map(o => o.temperature_c),
    borderColor: colors[i % colors.length],
    tension: 0.3,
    fill: false,
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

function renderAlertsTable(alerts) {
  const tbody = document.getElementById('wx-alerts-body');
  if (!tbody) return;
  if (!alerts?.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No severe weather alerts</td></tr>';
    return;
  }
  tbody.innerHTML = alerts.map(a => `
    <tr>
      <td>${a.location}</td>
      <td>${GRIP_UI.riskBadge(a.storm_severity)}</td>
      <td>${a.wind_kmh ?? '—'} km/h</td>
      <td>${GRIP_UI.formatTime(a.time)}</td>
    </tr>
  `).join('');
}

async function loadWeatherForecast() {
  const container = document.getElementById('wx-forecast-chart');
  if (!container) return;
  try {
    const data = await GRIP_API.getForecasts({ source: 'weather', metric: 'temperature_c', horizon: '24h' });
    if (!data.forecasts?.length) return;

    const byLocation = {};
    data.forecasts.forEach(f => {
      const loc = f.location_name || 'Unknown';
      if (!byLocation[loc]) byLocation[loc] = [];
      byLocation[loc].push(f);
    });

    const locations = Object.keys(byLocation);
    const colors = ['#3b82f6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444'];
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

document.addEventListener('DOMContentLoaded', initWeather);
