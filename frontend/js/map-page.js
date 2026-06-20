/**
 * GRIP — Global Risk Map page
 */
let mapFilters = { earthquakes: true, wildfires: true, weather: true, air_quality: true };

async function initMapPage() {
  GRIP_MAP.createMap('risk-map', { center: [20, 0], zoom: 2 });

  document.querySelectorAll('.filter-toggle').forEach(toggle => {
    toggle.addEventListener('change', (e) => {
      mapFilters[e.target.dataset.layer] = e.target.checked;
      refreshMap();
    });
  });

  await refreshMap();

  gripWS.on('update', (data) => {
    if (data.map_markers) {
      GRIP_MAP.updateMarkers('risk-map', data.map_markers, mapFilters);
    }
    if (data.risk_scores) {
      GRIP_MAP.addRiskScoreMarkers('risk-map', data.risk_scores);
    }
  });

  gripWS.on('*', async (data) => {
    if (data.risk_scores) {
      GRIP_MAP.addRiskScoreMarkers('risk-map', data.risk_scores);
    }
  });
}

async function refreshMap() {
  try {
    const [markers, riskScores] = await Promise.all([
      GRIP_API.getMapMarkers(500),
      GRIP_API.getRiskScores(),
    ]);
    GRIP_MAP.updateMarkers('risk-map', markers, mapFilters);
    GRIP_MAP.addRiskScoreMarkers('risk-map', riskScores);
    updateMapStats(markers);
  } catch (err) {
    console.error('Map refresh error:', err);
  }
}

function updateMapStats(markers) {
  const el = document.getElementById('map-stats');
  if (!el) return;
  el.innerHTML = `
    <span>🌍 EQ: ${markers.earthquakes?.length || 0}</span>
    <span>🔥 Fires: ${markers.wildfires?.length || 0}</span>
    <span>⛈ Weather: ${markers.weather?.length || 0}</span>
    <span>💨 AQI: ${markers.air_quality?.length || 0}</span>
  `;
}

document.addEventListener('DOMContentLoaded', initMapPage);
