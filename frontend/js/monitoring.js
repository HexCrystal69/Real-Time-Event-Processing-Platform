/**
 * GRIP — System Monitoring page
 */
async function initMonitoring() {
  await refreshMonitoring();
  setInterval(refreshMonitoring, GRIP_CONFIG.REFRESH_INTERVAL);
}

async function refreshMonitoring() {
  try {
    const data = await GRIP_API.getMonitoring();

    renderServiceStatus('postgres-status', 'PostgreSQL', data.postgres);
    renderServiceStatus('kafka-status', 'Kafka', data.kafka);
    renderServiceStatus('spark-status', 'Spark', data.spark);
    renderServiceStatus('api-status', 'FastAPI', data.api);

    renderPipelineHealth(data.pipeline_health);
    renderIngestionRates(data.ingestion_rate);
    renderProcessingLatency(data.processing_latency);
    renderErrorCounts(data.error_counts);

    document.getElementById('monitor-timestamp').textContent =
      `Last updated: ${GRIP_UI.formatTime(data.timestamp)}`;
  } catch (err) {
    GRIP_UI.showError(document.getElementById('monitor-content'), err.message);
  }
}

function renderServiceStatus(containerId, name, status) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const healthy = status?.status === 'healthy' || status?.status === 'connected';
  el.innerHTML = `
    <div class="service-status">
      <span class="status-dot ${healthy ? 'live' : 'offline'}"></span>
      <span class="service-name">${name}</span>
      <span class="service-detail ${healthy ? 'health-healthy' : 'health-unhealthy'}">
        ${status?.status || 'unknown'}
        ${status?.total_records != null ? ` · ${GRIP_UI.formatNumber(status.total_records)} records` : ''}
        ${status?.workers_alive != null ? ` · ${status.workers_alive} workers` : ''}
        ${status?.topics ? ` · ${status.topics.length} topics` : ''}
      </span>
    </div>
  `;
}

function renderPipelineHealth(pipeline) {
  const tbody = document.getElementById('pipeline-health-body');
  if (!tbody) return;
  if (!pipeline?.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">Awaiting pipeline metrics from Spark</td></tr>';
    return;
  }
  tbody.innerHTML = pipeline.map(p => `
    <tr>
      <td>${p.source}</td>
      <td>${p.total_processed ?? '—'}</td>
      <td>${p.avg_processing_ms ?? '—'} ms</td>
      <td>${p.avg_throughput ?? '—'}/min</td>
      <td>${GRIP_UI.formatTime(p.last_batch)}</td>
    </tr>
  `).join('');
}

function renderIngestionRates(rates) {
  const canvas = document.getElementById('ingestion-chart');
  if (!canvas || !rates) return;
  const labels = Object.keys(rates);
  new Chart(canvas, {
    type: 'bar',
    data: {
      labels: labels.map(l => l.replace(/_/g, ' ')),
      datasets: [{ label: 'Avg Latency (ms)', data: Object.values(rates), backgroundColor: GRIP_CONFIG.CHART_COLORS.accent + '88' }],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderProcessingLatency(latency) {
  const canvas = document.getElementById('latency-chart');
  if (!canvas || !latency) return;
  new Chart(canvas, {
    type: 'bar',
    data: {
      labels: Object.keys(latency),
      datasets: [{ label: 'Processing Time (ms)', data: Object.values(latency), backgroundColor: GRIP_CONFIG.CHART_COLORS.weather + '88' }],
    },
    options: GRIP_UI.chartDefaults(),
  });
}

function renderErrorCounts(errors) {
  const tbody = document.getElementById('errors-body');
  if (!tbody) return;
  const entries = Object.entries(errors || {});
  if (!entries.length) {
    tbody.innerHTML = '<tr><td colspan="2" class="empty-state">No errors in last 24 hours</td></tr>';
    return;
  }
  tbody.innerHTML = entries.map(([source, count]) => `
    <tr><td>${source}</td><td class="health-unhealthy">${count}</td></tr>
  `).join('');
}

document.addEventListener('DOMContentLoaded', initMonitoring);
