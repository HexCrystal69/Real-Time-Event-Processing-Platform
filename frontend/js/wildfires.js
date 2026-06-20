/**
 * GRIP — Wildfire Intelligence page
 */
async function initWildfires() {
  try {
    const data = await GRIP_API.getWildfireAnalytics();

    document.getElementById('wf-active-count').textContent = GRIP_UI.formatNumber(data.active_fires?.length || 0);

    renderSeverityChart(data.severity_distribution);
    renderConfidenceChart(data.confidence_distribution);
    renderGrowthChart(data.growth_trends);
    renderFiresTable(data.active_fires);
    renderRegionsTable(data.high_risk_regions);
    await loadWildfireForecast();
  } catch (err) {
    GRIP_UI.showError(document.getElementById('wf-content'), err.message);
  }
}

function renderSeverityChart(dist) {
  const canvas = document.getElementById('wf-severity-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'bar',
    data: {
      labels: Object.keys(dist || {}),
      datasets: [{
        label: 'Detections',
        data: Object.values(dist || {}),
        backgroundColor: GRIP_CONFIG.CHART_COLORS.wildfires + '88',
        borderColor: GRIP_CONFIG.CHART_COLORS.wildfires,
        borderWidth: 1,
      }],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderConfidenceChart(dist) {
  const canvas = document.getElementById('wf-confidence-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'doughnut',
    data: {
      labels: Object.keys(dist || {}),
      datasets: [{ data: Object.values(dist || {}), backgroundColor: ['#10b981', '#f59e0b', '#ef4444', '#64748b'] }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { color: GRIP_CONFIG.CHART_COLORS.text } } },
    },
  });
}

function renderGrowthChart(trends) {
  const canvas = document.getElementById('wf-growth-chart');
  if (!canvas) return;
  new Chart(canvas, {
    type: 'line',
    data: {
      labels: (trends || []).map(t => t.date?.slice(0, 10)),
      datasets: [
        {
          label: 'Fire Count',
          data: (trends || []).map(t => t.count),
          borderColor: GRIP_CONFIG.CHART_COLORS.wildfires,
          yAxisID: 'y',
          tension: 0.3,
        },
        {
          label: 'Avg FRP',
          data: (trends || []).map(t => t.avg_frp),
          borderColor: GRIP_CONFIG.CHART_COLORS.accent,
          yAxisID: 'y1',
          tension: 0.3,
        },
      ],
    },
    options: {
      ...GRIP_UI.chartDefaults(),
      scales: {
        ...GRIP_UI.chartDefaults().scales,
        y1: { position: 'right', ticks: { color: GRIP_CONFIG.CHART_COLORS.text }, grid: { drawOnChartArea: false } },
      },
    },
  });
}

function renderFiresTable(fires) {
  const tbody = document.getElementById('wf-fires-body');
  if (!tbody) return;
  if (!fires?.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No active fires in pipeline</td></tr>';
    return;
  }
  tbody.innerHTML = fires.map(f => `
    <tr>
      <td>${f.latitude?.toFixed(2)}, ${f.longitude?.toFixed(2)}</td>
      <td>${f.frp ?? '—'}</td>
      <td>${GRIP_UI.riskBadge(f.severity)}</td>
      <td>${f.confidence || '—'}</td>
      <td>${f.date?.slice(0, 10) || '—'}</td>
    </tr>
  `).join('');
}

function renderRegionsTable(regions) {
  const tbody = document.getElementById('wf-regions-body');
  if (!tbody) return;
  if (!regions?.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty-state">Insufficient data for regional analysis</td></tr>';
    return;
  }
  tbody.innerHTML = regions.map(r => `
    <tr>
      <td>${r.latitude?.toFixed(0)}°, ${r.longitude?.toFixed(0)}°</td>
      <td>${r.fire_count}</td>
      <td>${r.avg_frp}</td>
      <td>${GRIP_UI.riskBadge(r.fire_count > 20 ? 'High' : r.fire_count > 10 ? 'Moderate' : 'Low')}</td>
    </tr>
  `).join('');
}

async function loadWildfireForecast() {
  const container = document.getElementById('wf-forecast-chart');
  if (!container) return;
  try {
    const data = await GRIP_API.getForecasts({ source: 'wildfires', metric: 'fire_count', horizon: '7d' });
    if (!data.forecasts?.length) {
      container.parentElement.innerHTML = '<div class="empty-state">Forecast will appear once sufficient historical data is available</div>';
      return;
    }
    const canvas = document.createElement('canvas');
    container.appendChild(canvas);
    new Chart(canvas, {
      type: 'line',
      data: {
        labels: data.forecasts.map(f => f.forecast_time?.slice(0, 10)),
        datasets: [{
          label: 'Predicted Fire Count',
          data: data.forecasts.map(f => f.predicted_value),
          borderColor: GRIP_CONFIG.CHART_COLORS.wildfires,
          backgroundColor: GRIP_CONFIG.CHART_COLORS.wildfires + '22',
          fill: true,
          tension: 0.3,
        }],
      },
      options: GRIP_UI.chartDefaults(),
    });
  } catch (e) { /* forecast may not be ready yet */ }
}

document.addEventListener('DOMContentLoaded', initWildfires);
