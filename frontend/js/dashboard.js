/**
 * GRIP — Dashboard page
 */
async function initDashboard() {
  const statsGrid = document.getElementById('stats-grid');
  const alertFeed = document.getElementById('alert-feed');
  const riskTable = document.getElementById('risk-table-body');
  const eventsChart = document.getElementById('events-chart');

  try {
    const [summary, events, alerts, rankings] = await Promise.all([
      GRIP_API.getSummary(),
      GRIP_API.getEventsPerHour(24),
      GRIP_API.getActiveAlerts(),
      GRIP_API.getRegionalRankings(),
    ]);

    statsGrid.innerHTML = `
      <div class="card fade-in">
        <div class="card-title">Total Events</div>
        <div class="stat-value">${GRIP_UI.formatNumber(summary.total_events)}</div>
        <div class="stat-label">All sources combined</div>
      </div>
      <div class="card fade-in">
        <div class="card-title">Events / Hour</div>
        <div class="stat-value">${GRIP_UI.formatNumber(summary.events_last_hour)}</div>
        <div class="stat-label">Last 60 minutes</div>
      </div>
      <div class="card fade-in">
        <div class="card-title">Active Alerts</div>
        <div class="stat-value" style="color:var(--accent-red)">${summary.active_alerts}</div>
        <div class="stat-label">Requiring attention</div>
      </div>
      <div class="card fade-in">
        <div class="card-title">Anomalies</div>
        <div class="stat-value" style="color:var(--accent-orange)">${GRIP_UI.formatNumber(summary.total_anomalies)}</div>
        <div class="stat-label">Detected anomalies</div>
      </div>
    `;

    GRIP_UI.renderAlertFeed(alerts.alerts, alertFeed);

    if (rankings.length > 0) {
      riskTable.innerHTML = rankings.map(r => `
        <tr>
          <td>${r.region_name}</td>
          <td>${r.unified_score}</td>
          <td>${GRIP_UI.riskBadge(r.risk_level)}</td>
          <td>${r.earthquake_score}</td>
          <td>${r.wildfire_score}</td>
          <td>${r.weather_score}</td>
          <td>${r.air_quality_score}</td>
        </tr>
      `).join('');
    } else {
      riskTable.innerHTML = '<tr><td colspan="7" class="empty-state">Awaiting risk score computation...</td></tr>';
    }

    renderEventsChart(eventsChart, events);
    renderRiskDistribution(summary.risk_distribution);
  } catch (err) {
    GRIP_UI.showError(statsGrid, err.message);
  }

  gripWS.on('update', (data) => {
    if (data.summary) updateLiveStats(data.summary);
    if (data.alerts) GRIP_UI.renderAlertFeed(data.alerts, alertFeed);
    if (data.risk_scores) updateRiskTable(data.risk_scores);
  });
}

function updateLiveStats(summary) {
  const values = document.querySelectorAll('.stat-value');
  if (values.length >= 4) {
    values[0].textContent = GRIP_UI.formatNumber(summary.total_events);
    values[1].textContent = GRIP_UI.formatNumber(summary.events_last_hour);
    values[2].textContent = summary.active_alerts;
    values[3].textContent = GRIP_UI.formatNumber(summary.total_anomalies);
  }
}

function updateRiskTable(scores) {
  const tbody = document.getElementById('risk-table-body');
  if (!tbody || !scores.length) return;
  const sorted = [...scores].sort((a, b) => b.unified_score - a.unified_score);
  tbody.innerHTML = sorted.map(r => `
    <tr>
      <td>${r.region_name}</td>
      <td>${r.unified_score}</td>
      <td>${GRIP_UI.riskBadge(r.risk_level)}</td>
      <td>${r.earthquake_score}</td>
      <td>${r.wildfire_score}</td>
      <td>${r.weather_score}</td>
      <td>${r.air_quality_score}</td>
    </tr>
  `).join('');
}

function renderEventsChart(canvas, events) {
  if (!canvas) return;
  const sources = ['earthquakes', 'wildfires', 'weather', 'air_quality'];
  const colors = [
    GRIP_CONFIG.CHART_COLORS.earthquakes,
    GRIP_CONFIG.CHART_COLORS.wildfires,
    GRIP_CONFIG.CHART_COLORS.weather,
    GRIP_CONFIG.CHART_COLORS.airQuality,
  ];

  const allHours = new Set();
  sources.forEach(s => (events[s] || []).forEach(e => allHours.add(e.hour)));
  const labels = [...allHours].sort().map(h => GRIP_UI.formatTimeShort(h));

  const datasets = sources.map((s, i) => {
    const hourMap = {};
    (events[s] || []).forEach(e => { hourMap[e.hour] = e.count; });
    const sortedHours = [...allHours].sort();
    return {
      label: s.replace('_', ' '),
      data: sortedHours.map(h => hourMap[h] || 0),
      borderColor: colors[i],
      backgroundColor: colors[i] + '33',
      fill: true,
      tension: 0.3,
    };
  });

  if (window.dashboardChart) window.dashboardChart.destroy();
  window.dashboardChart = new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderRiskDistribution(distribution) {
  const canvas = document.getElementById('risk-distribution-chart');
  if (!canvas || !distribution) return;

  const labels = Object.keys(distribution);
  const data = Object.values(distribution);
  const colors = labels.map(l => GRIP_CONFIG.RISK_COLORS[l] || '#64748b');

  if (window.riskDistChart) window.riskDistChart.destroy();
  window.riskDistChart = new Chart(canvas, {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth: 0 }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { color: GRIP_CONFIG.CHART_COLORS.text } } },
    },
  });
}

document.addEventListener('DOMContentLoaded', initDashboard);
