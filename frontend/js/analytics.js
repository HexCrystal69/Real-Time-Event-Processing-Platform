/**
 * GRIP — Analytics Dashboard page
 */
async function initAnalytics() {
  try {
    const [summary, events, riskDist, sourceActivity, rankings, metrics] = await Promise.all([
      GRIP_API.getSummary(),
      GRIP_API.getEventsPerHour(48),
      GRIP_API.getRiskDistribution(),
      GRIP_API.getSourceActivity(),
      GRIP_API.getRegionalRankings(),
      GRIP_API.getMetrics(),
    ]);

    document.getElementById('analytics-stats').innerHTML = `
      <div class="card"><div class="card-title">Total Events</div><div class="stat-value">${GRIP_UI.formatNumber(summary.total_events)}</div></div>
      <div class="card"><div class="card-title">Events / Hour</div><div class="stat-value">${GRIP_UI.formatNumber(summary.events_last_hour)}</div></div>
      <div class="card"><div class="card-title">Earthquakes</div><div class="stat-value">${GRIP_UI.formatNumber(summary.total_earthquakes)}</div></div>
      <div class="card"><div class="card-title">Wildfires</div><div class="stat-value">${GRIP_UI.formatNumber(summary.total_wildfires)}</div></div>
    `;

    renderThroughputChart(events);
    renderUnifiedRisk(riskDist.unified);
    renderSourceRisk(riskDist);
    renderSourceActivity(sourceActivity);
    renderRankingsTable(rankings);
    renderPipelineMetrics(metrics);
  } catch (err) {
    GRIP_UI.showError(document.getElementById('analytics-content'), err.message);
  }
}

function renderThroughputChart(events) {
  const canvas = document.getElementById('throughput-chart');
  if (!canvas) return;
  const sources = Object.keys(events);
  const allHours = new Set();
  sources.forEach(s => (events[s] || []).forEach(e => allHours.add(e.hour)));
  const sortedHours = [...allHours].sort();

  const colors = ['#ef4444', '#f97316', '#3b82f6', '#10b981'];
  const datasets = sources.map((s, i) => {
    const map = {};
    (events[s] || []).forEach(e => { map[e.hour] = e.count; });
    return {
      label: s,
      data: sortedHours.map(h => map[h] || 0),
      backgroundColor: colors[i % colors.length] + '88',
    };
  });

  new Chart(canvas, {
    type: 'bar',
    data: { labels: sortedHours.map(h => GRIP_UI.formatTimeShort(h)), datasets },
    options: { ...GRIP_UI.chartDefaults(), scales: { ...GRIP_UI.chartDefaults().scales, x: { stacked: true, ...GRIP_UI.chartDefaults().scales.x }, y: { stacked: true, ...GRIP_UI.chartDefaults().scales.y } } },
  });
}

function renderUnifiedRisk(unified) {
  const canvas = document.getElementById('unified-risk-chart');
  if (!canvas || !unified) return;
  new Chart(canvas, {
    type: 'polarArea',
    data: {
      labels: Object.keys(unified),
      datasets: [{ data: Object.values(unified), backgroundColor: Object.keys(unified).map(l => (GRIP_CONFIG.RISK_COLORS[l] || '#64748b') + '88') }],
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: GRIP_CONFIG.CHART_COLORS.text } } } },
  });
}

function renderSourceRisk(riskDist) {
  const canvas = document.getElementById('source-risk-chart');
  if (!canvas) return;
  const sources = ['earthquakes', 'wildfires', 'weather', 'air_quality'];
  const labels = new Set();
  sources.forEach(s => Object.keys(riskDist[s] || {}).forEach(k => labels.add(k)));

  const datasets = sources.map((s, i) => ({
    label: s,
    data: [...labels].map(l => (riskDist[s] || {})[l] || 0),
    backgroundColor: ['#ef4444', '#f97316', '#3b82f6', '#10b981'][i] + '88',
  }));

  new Chart(canvas, {
    type: 'bar',
    data: { labels: [...labels], datasets },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderSourceActivity(activity) {
  const tbody = document.getElementById('source-activity-body');
  if (!tbody) return;
  tbody.innerHTML = (activity || []).map(a => `
    <tr>
      <td>${a.source}</td>
      <td class="${a.status === 'success' ? 'health-healthy' : 'health-unhealthy'}">${a.status}</td>
      <td>${a.records_count ?? '—'}</td>
      <td>${a.latency_ms ?? '—'} ms</td>
      <td>${GRIP_UI.formatTime(a.last_run)}</td>
    </tr>
  `).join('');
}

function renderRankingsTable(rankings) {
  const tbody = document.getElementById('rankings-body');
  if (!tbody) return;
  tbody.innerHTML = (rankings || []).map((r, i) => `
    <tr>
      <td>#${i + 1}</td>
      <td>${r.region_name}</td>
      <td>${r.unified_score}</td>
      <td>${GRIP_UI.riskBadge(r.risk_level)}</td>
    </tr>
  `).join('');
}

function renderPipelineMetrics(metrics) {
  const tbody = document.getElementById('pipeline-body');
  if (!tbody) return;
  tbody.innerHTML = (metrics.pipeline_summary || []).map(p => `
    <tr>
      <td>${p.source}</td>
      <td>${p.total_records ?? '—'}</td>
      <td>${p.avg_processing_time_ms ?? '—'} ms</td>
      <td>${p.avg_records_per_minute ?? '—'}</td>
      <td>${GRIP_UI.formatTime(p.last_batch_at)}</td>
    </tr>
  `).join('');
}

document.addEventListener('DOMContentLoaded', initAnalytics);
