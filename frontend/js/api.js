/**
 * GRIP — API client
 */
const GRIP_API = {
  async fetch(endpoint) {
    const response = await fetch(`${GRIP_CONFIG.API_BASE}${endpoint}`);
    if (!response.ok) {
      throw new Error(`API error: ${response.status} ${response.statusText}`);
    }
    return response.json();
  },

  getSummary() { return this.fetch('/api/analytics/summary'); },
  getEventsPerHour(hours = 24) { return this.fetch(`/api/analytics/events-per-hour?hours=${hours}`); },
  getRiskDistribution() { return this.fetch('/api/analytics/risk-distribution'); },
  getSourceActivity() { return this.fetch('/api/analytics/source-activity'); },
  getRegionalRankings() { return this.fetch('/api/analytics/regional-rankings'); },
  getRiskScores() { return this.fetch('/api/analytics/risk-scores'); },
  getMapMarkers(limit = 500) { return this.fetch(`/api/analytics/map-markers?limit=${limit}`); },
  getEarthquakeAnalytics() { return this.fetch('/api/analytics/earthquakes'); },
  getWildfireAnalytics() { return this.fetch('/api/analytics/wildfires'); },
  getWeatherAnalytics() { return this.fetch('/api/analytics/weather'); },
  getAirQualityAnalytics() { return this.fetch('/api/analytics/air-quality'); },
  getForecasts(params = {}) {
    const qs = new URLSearchParams(params).toString();
    return this.fetch(`/api/forecasts?${qs}`);
  },
  getActiveAlerts() { return this.fetch('/api/alerts/active'); },
  getAlertHistory(limit = 50) { return this.fetch(`/api/alerts/history?limit=${limit}`); },
  getMonitoring() { return this.fetch('/api/monitoring'); },
  getMetrics() { return this.fetch('/metrics'); },
  getStatus() { return this.fetch('/status'); },

  exportCSV(type) {
    window.open(`${GRIP_CONFIG.API_BASE}/api/export/csv/${type}`, '_blank');
  },
  exportJSON(type) {
    window.open(`${GRIP_CONFIG.API_BASE}/api/export/json/${type}`, '_blank');
  },
  exportPDF() {
    window.open(`${GRIP_CONFIG.API_BASE}/api/export/pdf/report`, '_blank');
  },
};
