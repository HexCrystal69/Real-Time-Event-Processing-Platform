/**
 * GRIP — API configuration
 */
const GRIP_CONFIG = {
  API_BASE: window.location.origin,
  WS_URL: `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/live`,
  REFRESH_INTERVAL: 30000,
  CHART_COLORS: {
    earthquakes: '#ef4444',
    wildfires: '#f97316',
    weather: '#3b82f6',
    airQuality: '#10b981',
    accent: '#06b6d4',
    grid: 'rgba(148, 163, 184, 0.1)',
    text: '#94a3b8',
  },
  RISK_COLORS: {
    Low: '#10b981',
    Moderate: '#f59e0b',
    High: '#f97316',
    Critical: '#ef4444',
  },
  SEVERITY_COLORS: {
    Low: '#10b981',
    Medium: '#f59e0b',
    Moderate: '#f59e0b',
    High: '#f97316',
    Critical: '#ef4444',
    Extreme: '#ef4444',
    Severe: '#f97316',
    Good: '#10b981',
    Poor: '#f97316',
    'Very Poor': '#ef4444',
    Hazardous: '#dc2626',
  },
};
