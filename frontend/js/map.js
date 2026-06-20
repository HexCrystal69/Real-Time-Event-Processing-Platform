/**
 * GRIP — Leaflet map utilities
 */
const GRIP_MAP = {
  maps: {},

  severityColor(severity, type) {
    const colors = GRIP_CONFIG.SEVERITY_COLORS;
    if (colors[severity]) return colors[severity];
    if (type === 'earthquake') {
      const mag = parseFloat(severity);
      if (mag >= 6) return '#ef4444';
      if (mag >= 4) return '#f97316';
      if (mag >= 2) return '#f59e0b';
      return '#10b981';
    }
    return '#3b82f6';
  },

  createMap(containerId, options = {}) {
    const map = L.map(containerId, {
      center: options.center || [20, 0],
      zoom: options.zoom || 2,
      zoomControl: true,
      attributionControl: true,
    });

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OpenStreetMap &copy; CARTO',
      subdomains: 'abcd',
      maxZoom: 19,
    }).addTo(map);

    this.maps[containerId] = { map, layers: {} };
    return map;
  },

  getLayerGroup(containerId, name) {
    const entry = this.maps[containerId];
    if (!entry) return null;
    if (!entry.layers[name]) {
      entry.layers[name] = L.layerGroup().addTo(entry.map);
    }
    return entry.layers[name];
  },

  clearLayers(containerId) {
    const entry = this.maps[containerId];
    if (!entry) return;
    Object.values(entry.layers).forEach(layer => layer.clearLayers());
  },

  updateMarkers(containerId, markers, filters = {}) {
    this.clearLayers(containerId);

    if (filters.earthquakes !== false && markers.earthquakes) {
      const layer = this.getLayerGroup(containerId, 'earthquakes');
      markers.earthquakes.forEach(eq => {
        if (!eq.lat || !eq.lng) return;
        const color = this.severityColor(eq.severity || eq.magnitude, 'earthquake');
        L.circleMarker([eq.lat, eq.lng], {
          radius: Math.max(4, (eq.magnitude || 1) * 2),
          fillColor: color,
          color: color,
          weight: 1,
          opacity: 0.8,
          fillOpacity: 0.6,
        }).bindPopup(`
          <strong>M${eq.magnitude || '?'} Earthquake</strong><br>
          ${eq.place || 'Unknown location'}<br>
          Risk: ${eq.severity || 'N/A'}<br>
          ${GRIP_UI.formatTime(eq.time)}
        `).addTo(layer);
      });
    }

    if (filters.wildfires !== false && markers.wildfires) {
      const layer = this.getLayerGroup(containerId, 'wildfires');
      markers.wildfires.forEach(wf => {
        if (!wf.lat || !wf.lng) return;
        const color = this.severityColor(wf.severity, 'wildfire');
        L.circleMarker([wf.lat, wf.lng], {
          radius: Math.max(3, Math.min(12, (wf.frp || 1) / 20)),
          fillColor: color,
          color: '#f97316',
          weight: 1,
          opacity: 0.8,
          fillOpacity: 0.5,
        }).bindPopup(`
          <strong>Wildfire Detection</strong><br>
          FRP: ${wf.frp || 'N/A'} MW<br>
          Severity: ${wf.severity}<br>
          Confidence: ${wf.confidence}
        `).addTo(layer);
      });
    }

    if (filters.weather !== false && markers.weather) {
      const layer = this.getLayerGroup(containerId, 'weather');
      markers.weather.forEach(w => {
        if (!w.lat || !w.lng) return;
        const color = this.severityColor(w.severity, 'weather');
        L.marker([w.lat, w.lng], {
          icon: L.divIcon({
            className: 'weather-marker',
            html: `<div style="background:${color};width:10px;height:10px;border-radius:50%;border:2px solid white;"></div>`,
            iconSize: [14, 14],
          }),
        }).bindPopup(`
          <strong>${w.location}</strong><br>
          Storm: ${w.severity}<br>
          Wind: ${w.wind_kmh || 'N/A'} km/h<br>
          Temp: ${w.temp_c || 'N/A'}°C
        `).addTo(layer);
      });
    }

    if (filters.air_quality !== false && markers.air_quality) {
      const layer = this.getLayerGroup(containerId, 'air_quality');
      markers.air_quality.forEach(aq => {
        if (!aq.lat || !aq.lng) return;
        const color = this.severityColor(aq.category, 'aqi');
        L.circleMarker([aq.lat, aq.lng], {
          radius: 8,
          fillColor: color,
          color: color,
          weight: 1,
          opacity: 0.8,
          fillOpacity: 0.4,
        }).bindPopup(`
          <strong>${aq.location}</strong><br>
          AQI: ${aq.aqi || 'N/A'}<br>
          Category: ${aq.category}
        `).addTo(layer);
      });
    }
  },

  addRiskScoreMarkers(containerId, riskScores) {
    const layer = this.getLayerGroup(containerId, 'risk_scores');
    (riskScores || []).forEach(rs => {
      if (!rs.latitude || !rs.longitude) return;
      const color = GRIP_CONFIG.RISK_COLORS[rs.risk_level] || '#3b82f6';
      L.circleMarker([rs.latitude, rs.longitude], {
        radius: 14,
        fillColor: color,
        color: '#fff',
        weight: 2,
        opacity: 1,
        fillOpacity: 0.3,
      }).bindPopup(`
        <strong>${rs.region_name}</strong><br>
        Unified Score: ${rs.unified_score}<br>
        Level: ${rs.risk_level}<br>
        EQ: ${rs.earthquake_score} | WF: ${rs.wildfire_score}<br>
        WX: ${rs.weather_score} | AQ: ${rs.air_quality_score}
      `).addTo(layer);
    });
  },
};
