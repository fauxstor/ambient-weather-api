'use strict';

const EventEmitter = require('events');
const io = require('socket.io-client');

const API_URL = 'https://api.ambientweather.net/';
const AW_API_URL = `${API_URL}v1/devices/`;

class AmbientWeatherApi extends EventEmitter {
  constructor(opts) {
    super();
    const { apiKey, applicationKey } = opts || {};
    if (!apiKey) {
      throw new Error('You need an apiKey');
    }
    if (!applicationKey) {
      throw new Error('You need an applicationKey');
    }
    this.apiKey = apiKey;
    this.applicationKey = applicationKey;
    this.requestQueue = [];
    this.subscribedDevices = [];
  }

  _buildUrl(macAddress, extraParams) {
    const u = new URL(`${AW_API_URL}${macAddress}`);
    u.searchParams.set('apiKey', this.apiKey);
    u.searchParams.set('applicationKey', this.applicationKey);
    if (extraParams) {
      for (const [k, v] of Object.entries(extraParams)) {
        if (v !== undefined && v !== null) {
          u.searchParams.set(k, String(v));
        }
      }
    }
    return u.toString();
  }

  _apiRequest(url) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        fetch(url)
          .then(async (res) => {
            this.requestQueue = this.requestQueue.filter((u) => u !== url);
            if (res.status === 429) {
              this.requestQueue.push(url);
              return this._apiRequest(url).then(resolve, reject);
            }
            const text = await res.text();
            let body;
            try {
              body = text ? JSON.parse(text) : null;
            } catch {
              const err = new Error(`${res.status} - ${text}`);
              err.statusCode = res.status;
              throw err;
            }
            if (!res.ok) {
              const err = new Error(
                `${res.status} - ${typeof body === 'string' ? body : JSON.stringify(body)}`
              );
              err.statusCode = res.status;
              throw err;
            }
            resolve(body);
          })
          .catch((err) => {
            reject(err);
          });
      }, this.requestQueue.length * 1100);
    });
  }

  userDevices() {
    return this._apiRequest(this._buildUrl(''));
  }

  deviceData(macAddress, opts) {
    if (!macAddress) {
      throw new Error('You need a macAddress for deviceData');
    }
    return this._apiRequest(this._buildUrl(macAddress, opts || {}));
  }

  connect() {
    if (this.socket) {
      return;
    }
    const socketUrl = `${API_URL}?api=1&applicationKey=${encodeURIComponent(this.applicationKey)}`;
    this.socket = io(socketUrl, { transports: ['websocket'] });
    ['error', 'connect'].forEach((key) => {
      this.socket.on(key, (data) => {
        this.emit(key, data);
      });
    });
    this.socket.on('subscribed', (data) => {
      this.subscribedDevices = data.devices || [];
      this.emit('subscribed', data);
    });
    this.socket.on('data', (data) => {
      data.device = this.subscribedDevices.find((d) => d.macAddress === data.macAddress);
      this.emit('data', data);
    });
  }

  disconnect() {
    if (this.socket) {
      this.socket.disconnect();
      delete this.socket;
    }
  }

  subscribe(apiKeyOrApiKeys) {
    const apiKeys = Array.isArray(apiKeyOrApiKeys) ? apiKeyOrApiKeys : [apiKeyOrApiKeys];
    this.socket.emit('subscribe', { apiKeys });
  }

  unsubscribe(apiKeyOrApiKeys) {
    const apiKeys = Array.isArray(apiKeyOrApiKeys) ? apiKeyOrApiKeys : [apiKeyOrApiKeys];
    this.socket.emit('unsubscribe', { apiKeys });
  }
}

module.exports = AmbientWeatherApi;
