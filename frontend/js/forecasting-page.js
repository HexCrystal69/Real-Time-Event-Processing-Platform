/**
 * GRIP — Forecasting Intelligence page
 *
 * Renders Prophet-generated forecasts for AQI, weather temperature,
 * wildfire activity, and earthquake frequency across all horizons.
 */

let forecastCharts = {};

async function initForecasting() {
  const horizonSelect = document.getElementById('horizon-select');
  horizonSelect.addEventListener('change', () => loadAllForecasts(horizonSelect.value));
  await loadAllForecasts(horizonSelect.value);
}

async function loadAllForecasts(horizon) {
  const status = document.getElementById('forecast-status');
  status.textContent = 'Loading forecasts…';

  await Promise.all([
    loadForecastChart('aqi', 'forecast-aqi-chart', 'aqi-badge', {
      source: 'air_quality', metric: 'us_aqi', horizon,
      label: 'AQI', color: GRIP_CONFIG.CHART_COLORS.airQuality, groupByLocation: true,
    }),
    loadForecastChart('weather', 'forecast-weather-chart', 'weather-badge', {
      source: 'weather', metric: 'temperature_c', horizon,
      label: 'Temperature (°C)', color: GRIP_CONFIG.CHART_COLORS.weather, groupByLocation: true,
    }),
    loadForecastChart('wildfire', 'forecast-wildfire-chart', 'wildfire-badge', {
      source: 'wildfires', metric: 'fire_count', horizon,
      label: 'Fire Count', color: GRIP_CONFIG.CHART_COLORS.wildfires, groupByLocation: false,
    }),
    loadForecastChart('earthquake', 'forecast-earthquake-chart', 'earthquake-badge', {
      source: 'earthquakes', metric: 'event_count', horizon,
      label: 'Event Count', color: GRIP_CONFIG.CHART_COLORS.earthquakes, groupByLocation: false,
    }),
  ]);

  await loadForecastDetails(horizon);
  status.textContent = `Horizon: ${horizon} · Updated: ${new Date().toLocaleTimeString()}`;
}

async function loadForecastChart(key, canvasId, badgeId, opts) {
  const canvas = document.getElementById(canvasId);
  const badge = document.getElementById(badgeId);
  if (!canvas) return;

  try {
    const data = await GRIP_API.getForecasts({
      source: opts.source, metric: opts.metric, horizon: opts.horizon,
    });

    if (!data.forecasts || data.forecasts.length === 0) {
      if (badge) badge.innerHTML = '<span class="badge badge-moderate">Awaiting Data</span>';
      if (forecastCharts[key]) { forecastCharts[key].destroy(); forecastCharts[key] = null; }
      canvas.parentElement.innerHTML = `<div class="empty-state">Forecast will appear once sufficient historical data is available for ${opts.label}</div>`;
      return;
    }

    if (badge) badge.innerHTML = `<span class="badge badge-low">${data.forecasts.length} points</span>`;

    let datasets;
    let labels;
    const colors = ['#3b82f6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

    if (opts.groupByLocation) {
      const byLocation = {};
      data.forecasts.forEach(f => {
        const loc = f.location_name || 'Global';
        if (!byLocation[loc]) byLocation[loc] = [];
        byLocation[loc].push(f);
      });

      const locations = Object.keys(byLocation);
      labels = byLocation[locations[0]]?.map(f => formatForecastTime(f.forecast_time, opts.horizon)) || [];

      datasets = locations.map((loc, i) => ({
        label: loc,
        data: byLocation[loc].map(f => f.predicted_value),
        borderColor: colors[i % colors.length],
        backgroundColor: colors[i % colors.length] + '15',
        fill: false,
        tension: 0.3,
        pointRadius: 2,
      }));

      /* Add confidence band for the first location */
      if (locations.length > 0) {
        const firstLoc = locations[0];
        datasets.push({
          label: `${firstLoc} Upper`,
          data: byLocation[firstLoc].map(f => f.upper_bound),
          borderColor: 'transparent',
          backgroundColor: colors[0] + '10',
          fill: '+1',
          pointRadius: 0,
          borderDash: [4, 4],
          borderWidth: 1,
        });
        datasets.push({
          label: `${firstLoc} Lower`,
          data: byLocation[firstLoc].map(f => f.lower_bound),
          borderColor: 'transparent',
          backgroundColor: colors[0] + '10',
          fill: false,
          pointRadius: 0,
          borderDash: [4, 4],
          borderWidth: 1,
        });
      }
    } else {
      labels = data.forecasts.map(f => formatForecastTime(f.forecast_time, opts.horizon));
      datasets = [
        {
          label: `Predicted ${opts.label}`,
          data: data.forecasts.map(f => f.predicted_value),
          borderColor: opts.color,
          backgroundColor: opts.color + '22',
          fill: true,
          tension: 0.3,
          pointRadius: 2,
        },
        {
          label: 'Upper Bound',
          data: data.forecasts.map(f => f.upper_bound),
          borderColor: opts.color + '44',
          backgroundColor: opts.color + '08',
          fill: '+1',
          pointRadius: 0,
          borderDash: [4, 4],
          borderWidth: 1,
        },
        {
          label: 'Lower Bound',
          data: data.forecasts.map(f => f.lower_bound),
          borderColor: opts.color + '44',
          backgroundColor: opts.color + '08',
          fill: false,
          pointRadius: 0,
          borderDash: [4, 4],
          borderWidth: 1,
        },
      ];
    }

    if (forecastCharts[key]) forecastCharts[key].destroy();
    forecastCharts[key] = new Chart(canvas, {
      type: 'line',
      data: { labels, datasets },
      options: {
        ...GRIP_UI.chartDefaults(),
        plugins: {
          ...GRIP_UI.chartDefaults().plugins,
          legend: {
            labels: {
              color: GRIP_CONFIG.CHART_COLORS.text,
              font: { size: 11 },
              filter: (item) => !item.text.includes('Upper') && !item.text.includes('Lower'),
            },
          },
        },
      },
    });
  } catch (err) {
    if (badge) badge.innerHTML = '<span class="badge badge-high">Error</span>';
    console.error(`Forecast load error (${key}):`, err);
  }
}

function formatForecastTime(iso, horizon) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (horizon === '30d') return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  if (horizon === '7d') return d.toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit' });
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

async function loadForecastDetails(horizon) {
  const tbody = document.getElementById('forecast-details-body');
  if (!tbody) return;

  try {
    const sources = [
      { source: 'air_quality', metric: 'us_aqi', label: 'AQI' },
      { source: 'weather', metric: 'temperature_c', label: 'Temperature' },
      { source: 'wildfires', metric: 'fire_count', label: 'Fire Count' },
      { source: 'earthquakes', metric: 'event_count', label: 'Event Count' },
    ];

    const results = await Promise.all(
      sources.map(s => GRIP_API.getForecasts({ source: s.source, metric: s.metric, horizon }))
    );

    const rows = [];
    results.forEach((data, i) => {
      if (data.forecasts && data.forecasts.length > 0) {
        const first = data.forecasts[0];
        const locations = new Set(data.forecasts.map(f => f.location_name || 'Global'));
        locations.forEach(loc => {
          const locData = data.forecasts.filter(f => (f.location_name || 'Global') === loc);
          rows.push({
            metric: sources[i].label,
            source: sources[i].source,
            location: loc,
            horizon: horizon,
            count: locData.length,
            generated: first.generated_at,
          });
        });
      }
    });

    if (rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No forecast data available. Forecasts generate automatically from historical pipeline data.</td></tr>';
      return;
    }

    tbody.innerHTML = rows.map(r => `
      <tr>
        <td>${r.metric}</td>
        <td>${r.source}</td>
        <td>${r.location}</td>
        <td>${r.horizon}</td>
        <td>${r.count}</td>
        <td>${GRIP_UI.formatTime(r.generated)}</td>
      </tr>
    `).join('');
  } catch (err) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">Failed to load forecast details</td></tr>';
  }
}

async function triggerRegenerate() {
  const btn = document.getElementById('regen-btn');
  btn.disabled = true;
  btn.textContent = 'Generating…';

  try {
    await GRIP_API.fetch('/api/forecasts/generate');
    const horizon = document.getElementById('horizon-select').value;
    await loadAllForecasts(horizon);
  } catch (err) {
    console.error('Forecast regeneration failed:', err);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Regenerate Forecasts';
  }
}

document.addEventListener('DOMContentLoaded', initForecasting);
