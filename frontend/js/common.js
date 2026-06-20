/**
 * GRIP — Shared UI utilities
 */
const GRIP_UI = {
  riskBadge(level) {
    const cls = (level || 'Low').toLowerCase();
    return `<span class="badge badge-${cls}">${level || 'Unknown'}</span>`;
  },

  formatNumber(n) {
    if (n == null) return '—';
    return Number(n).toLocaleString();
  },

  formatTime(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleString();
  },

  formatTimeShort(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  },

  showLoading(container) {
    container.innerHTML = '<div class="loading"><div class="spinner"></div>Loading live data...</div>';
  },

  showError(container, msg) {
    container.innerHTML = `<div class="empty-state">⚠ ${msg}</div>`;
  },

  initSidebar() {
    const toggle = document.querySelector('.menu-toggle');
    const sidebar = document.querySelector('.sidebar');
    if (toggle && sidebar) {
      toggle.addEventListener('click', () => sidebar.classList.toggle('open'));
    }

    const currentPage = window.location.pathname.split('/').pop() || 'index.html';
    document.querySelectorAll('.nav-link').forEach(link => {
      const href = link.getAttribute('href');
      if (href === currentPage || (currentPage === '' && href === 'index.html')) {
        link.classList.add('active');
      }
    });
  },

  initClock() {
    const el = document.getElementById('live-clock');
    if (!el) return;
    const update = () => {
      el.textContent = new Date().toUTCString().replace('GMT', 'UTC');
    };
    update();
    setInterval(update, 1000);
  },

  renderAlertFeed(alerts, container) {
    if (!alerts || alerts.length === 0) {
      container.innerHTML = '<div class="empty-state">No active alerts</div>';
      return;
    }
    container.innerHTML = alerts.map(a => `
      <div class="alert-item ${(a.severity || '').toLowerCase()} fade-in">
        <div class="alert-title">${a.title}</div>
        <div class="alert-meta">${a.severity} · ${a.source} · ${this.formatTime(a.created_at)}</div>
      </div>
    `).join('');
  },

  chartDefaults() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: GRIP_CONFIG.CHART_COLORS.text, font: { size: 11 } } },
      },
      scales: {
        x: {
          ticks: { color: GRIP_CONFIG.CHART_COLORS.text, font: { size: 10 } },
          grid: { color: GRIP_CONFIG.CHART_COLORS.grid },
        },
        y: {
          ticks: { color: GRIP_CONFIG.CHART_COLORS.text, font: { size: 10 } },
          grid: { color: GRIP_CONFIG.CHART_COLORS.grid },
        },
      },
    };
  },
};

document.addEventListener('DOMContentLoaded', () => {
  GRIP_UI.initSidebar();
  GRIP_UI.initClock();
  gripWS.connect();
});
