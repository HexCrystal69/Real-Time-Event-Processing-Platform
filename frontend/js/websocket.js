/**
 * GRIP — WebSocket client for real-time updates
 */
class GRIPWebSocket {
  constructor() {
    this.ws = null;
    this.reconnectDelay = 3000;
    this.listeners = [];
    this.connected = false;
  }

  connect() {
    if (this.ws && this.ws.readyState <= 1) return;

    this.ws = new WebSocket(GRIP_CONFIG.WS_URL);

    this.ws.onopen = () => {
      this.connected = true;
      this._updateStatus(true);
      this._emit('connected', {});
    };

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        this._emit(data.type || 'update', data);
      } catch (e) {
        console.error('WebSocket parse error:', e);
      }
    };

    this.ws.onclose = () => {
      this.connected = false;
      this._updateStatus(false);
      setTimeout(() => this.connect(), this.reconnectDelay);
    };

    this.ws.onerror = () => {
      this.connected = false;
      this._updateStatus(false);
    };
  }

  on(event, callback) {
    this.listeners.push({ event, callback });
  }

  _emit(event, data) {
    this.listeners
      .filter(l => l.event === event || l.event === '*')
      .forEach(l => l.callback(data));
  }

  _updateStatus(live) {
    document.querySelectorAll('.status-dot').forEach(dot => {
      dot.classList.toggle('live', live);
      dot.classList.toggle('offline', !live);
    });
    document.querySelectorAll('.ws-status-text').forEach(el => {
      el.textContent = live ? 'LIVE' : 'RECONNECTING';
    });
  }
}

const gripWS = new GRIPWebSocket();
