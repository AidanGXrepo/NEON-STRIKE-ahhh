/* ============================================================================
 * NEON STRIKE — client networking module
 * ----------------------------------------------------------------------------
 * Deliberately knows NOTHING about Three.js, the map, weapons or the HUD.
 * It owns: the socket, the wire protocol, the snapshot buffer and the
 * interpolation clock. Gameplay subscribes through NeonNet.on(...).
 *
 * The game talks to it only through this surface:
 *   NeonNet.host(addr, opts) / NeonNet.join(addr, opts) / NeonNet.leave()
 *   NeonNet.setMap(id) / NeonNet.startMatch()
 *   NeonNet.sendState(s) / NeonNet.sendShot(s) / NeonNet.sendWeapon(w)
 *   NeonNet.sendReload(on) / NeonNet.requestRespawn()
 *   NeonNet.samplePlayers(renderTimeMs) -> interpolated remote player states
 * ==========================================================================*/
(function (global) {
  'use strict';

  const WEAPON_NAMES = ['rifle', 'pistol', 'sniper', 'shotgun'];

  // Flag bits must match packFlags() in server.js
  const F = { CROUCH: 1, SPRINT: 2, GROUNDED: 4, MOVING: 8, DEAD: 16, RELOAD: 32, SHOOT: 64 };

  const NeonNet = {
    // ---- public read-only state ----
    connected: false,
    connecting: false,
    isHost: false,
    selfId: null,
    phase: 'offline',          // 'offline' | 'lobby' | 'playing'
    mapId: null,
    address: '',
    lobby: { players: [], count: 0, max: 8, hostId: null },
    config: { tickRate: 20, maxPlayers: 8, health: 150, respawnMs: 5000 },
    lastError: null,
    ping: 0,

    // ---- internals ----
    _ws: null,
    _handlers: {},
    _snapshots: [],            // [{ts, recvAt, players:Map}]
    _interpDelayMs: 110,       // render remote players this far in the past
    _clockOffset: 0,           // serverTime - clientTime
    _pingTimer: null,
    _role: 'client'
  };

  // ---------------------------------------------------------------------------
  // Events
  // ---------------------------------------------------------------------------
  NeonNet.on = function (type, fn) {
    (this._handlers[type] = this._handlers[type] || []).push(fn);
    return this;
  };
  NeonNet._emit = function (type, payload) {
    const list = this._handlers[type];
    if (!list) return;
    for (let i = 0; i < list.length; i++) {
      try { list[i](payload); } catch (e) { console.error('[net] handler error for "' + type + '":', e); }
    }
  };

  // ---------------------------------------------------------------------------
  // Address handling
  // ---------------------------------------------------------------------------
  /** Accepts "192.168.2.12", "192.168.2.12:3000", "http://host:3000", "ws://..." */
  NeonNet.normalizeAddress = function (input) {
    let a = String(input || '').trim();
    if (!a) return null;
    a = a.replace(/^wss?:\/\//i, '').replace(/^https?:\/\//i, '').replace(/\/+$/, '');
    if (!a) return null;
    if (!/:\d+$/.test(a)) a += ':3000';      // default port
    if (!/^[\w.\-\[\]:]+$/.test(a)) return null;
    return a;
  };
  /** The address the page itself was served from — the sensible default for JOIN. */
  NeonNet.pageAddress = function () {
    try {
      if (!global.location || !global.location.host) return '';
      if (global.location.protocol === 'file:') return '';
      return global.location.host;
    } catch (e) { return ''; }
  };

  // ---------------------------------------------------------------------------
  // Connect
  // ---------------------------------------------------------------------------
  function connect(role, address, opts) {
    opts = opts || {};
    const addr = NeonNet.normalizeAddress(address);
    if (!addr) {
      NeonNet.lastError = 'That address does not look right.';
      NeonNet._emit('status', { state: 'failed', message: NeonNet.lastError });
      return false;
    }
    NeonNet.leave(true);

    NeonNet._role = role;
    NeonNet.address = addr;
    NeonNet.connecting = true;
    NeonNet.lastError = null;
    NeonNet._emit('status', { state: 'connecting', message: 'Connecting to ' + addr + '…' });

    let ws;
    try {
      ws = new WebSocket('ws://' + addr);
    } catch (e) {
      NeonNet.connecting = false;
      NeonNet.lastError = 'Could not open a connection to ' + addr + '.';
      NeonNet._emit('status', { state: 'failed', message: NeonNet.lastError });
      return false;
    }
    NeonNet._ws = ws;

    const timeout = setTimeout(() => {
      if (NeonNet.connecting) {
        NeonNet.lastError = 'No answer from ' + addr + '. Is the host\'s server running, and are you on the same network?';
        try { ws.close(); } catch (e) {}
        NeonNet.connecting = false;
        NeonNet._emit('status', { state: 'failed', message: NeonNet.lastError });
      }
    }, 8000);

    ws.onopen = () => {
      clearTimeout(timeout);
      NeonNet.connected = true;
      NeonNet.connecting = false;
      NeonNet._emit('status', { state: 'connected', message: 'Connected. Joining…' });
      NeonNet._send({ t: 'join', role: role, name: opts.name || 'Player', mapId: opts.mapId || null });
      NeonNet._pingTimer = setInterval(() => NeonNet._send({ t: 'ping', ts: Date.now() }), 2000);
    };

    ws.onmessage = ev => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      NeonNet._receive(msg);
    };

    ws.onerror = () => {
      if (NeonNet.connecting) {
        NeonNet.lastError = 'Could not reach ' + addr + '.';
        NeonNet._emit('status', { state: 'failed', message: NeonNet.lastError });
      }
    };

    ws.onclose = () => {
      clearTimeout(timeout);
      const wasConnected = NeonNet.connected;
      NeonNet._reset();
      if (wasConnected) {
        NeonNet._emit('status', { state: 'closed', message: NeonNet.lastError || 'Disconnected from the host.' });
        NeonNet._emit('disconnected', { message: NeonNet.lastError });
      } else if (!NeonNet.lastError) {
        NeonNet.lastError = 'Connection failed.';
        NeonNet._emit('status', { state: 'failed', message: NeonNet.lastError });
      }
    };
    return true;
  }

  NeonNet.host = function (address, opts) { return connect('host', address, opts); };
  NeonNet.join = function (address, opts) { return connect('client', address, opts); };

  NeonNet.leave = function (quiet) {
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    const ws = this._ws;
    this._ws = null;
    if (ws) {
      ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null;
      try { ws.close(); } catch (e) {}
    }
    const was = this.connected || this.connecting;
    this._reset();
    if (was && !quiet) this._emit('disconnected', { message: 'Left the game.' });
  };

  NeonNet._reset = function () {
    this.connected = false;
    this.connecting = false;
    this.isHost = false;
    this.selfId = null;
    this.phase = 'offline';
    this.mapId = null;
    this.lobby = { players: [], count: 0, max: 8, hostId: null };
    this._snapshots.length = 0;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
  };

  NeonNet._send = function (obj) {
    const ws = this._ws;
    if (!ws || ws.readyState !== 1) return false;
    try { ws.send(JSON.stringify(obj)); return true; } catch (e) { return false; }
  };

  // ---------------------------------------------------------------------------
  // Inbound protocol
  // ---------------------------------------------------------------------------
  NeonNet._receive = function (msg) {
    switch (msg.t) {
      case 'welcome':
        this.selfId = msg.id;
        this.isHost = !!msg.isHost;
        this.config = msg.config || this.config;
        this.phase = msg.phase || 'lobby';
        this.mapId = msg.mapId;
        this._emit('welcome', msg);
        break;

      case 'lobby':
        this.phase = msg.phase;
        this.mapId = msg.mapId;
        this.lobby = { players: msg.players || [], count: msg.count || 0,
                       max: msg.max || 8, hostId: msg.hostId, settings: msg.settings };
        this._emit('lobby', this.lobby);
        break;

      case 'mapChange':
        this.mapId = msg.mapId;
        this._emit('mapChange', msg);
        break;

      case 'matchStart':
        this.phase = 'playing';
        this.mapId = msg.mapId;
        this._snapshots.length = 0;
        this._emit('matchStart', msg);
        break;

      case 'matchEnd':
        this.phase = 'lobby';
        this._snapshots.length = 0;
        this._emit('matchEnd', msg);
        break;

      case 's': {                      // snapshot
        const players = new Map();
        for (const a of msg.p) {
          players.set(a[0], {
            id: a[0], x: a[1], y: a[2], z: a[3], yaw: a[4], pitch: a[5],
            crouch: !!(a[6] & F.CROUCH), sprint: !!(a[6] & F.SPRINT),
            grounded: !!(a[6] & F.GROUNDED), moving: !!(a[6] & F.MOVING),
            dead: !!(a[6] & F.DEAD), reloading: !!(a[6] & F.RELOAD),
            shooting: !!(a[6] & F.SHOOT),
            hp: a[7], weapon: WEAPON_NAMES[a[8]] || 'rifle'
          });
        }
        const now = Date.now();
        this._clockOffset = msg.ts - now;
        this._snapshots.push({ ts: msg.ts, recvAt: now, players });
        // Keep roughly a second of history; that is plenty for interpolation.
        while (this._snapshots.length > 40) this._snapshots.shift();
        break;
      }

      case 'hp':            this._emit('hp', msg); break;
      case 'damage':        this._emit('damage', msg); break;        // local player was hit
      case 'hitConfirm':    this._emit('hitConfirm', msg); break;    // our shot landed
      case 'shot':          this._emit('shot', msg); break;          // someone else fired
      case 'eliminated':    this._emit('eliminated', msg); break;
      case 'respawned':     this._emit('respawned', msg); break;
      case 'respawnDenied': this._emit('respawnDenied', msg); break;
      case 'correction':    this._emit('correction', msg); break;
      case 'playerJoin':    this._emit('playerJoin', msg); break;
      case 'playerLeave':   this._emit('playerLeave', msg); break;

      case 'hostLeft':
        this.lastError = msg.message || 'The host left the game.';
        this._emit('status', { state: 'closed', message: this.lastError });
        break;

      case 'error':
        this.lastError = msg.message || 'Server error.';
        this._emit('status', { state: 'failed', message: this.lastError });
        this._emit('serverError', msg);
        break;

      case 'pong':
        this.ping = Date.now() - msg.ts;
        break;
    }
  };

  // ---------------------------------------------------------------------------
  // Outbound helpers
  // ---------------------------------------------------------------------------
  NeonNet.setMap       = function (mapId) { return this._send({ t: 'setMap', mapId: mapId }); };
  NeonNet.setSettings  = function (s)     { return this._send({ t: 'setSettings', settings: s }); };
  NeonNet.startMatch   = function ()      { return this._send({ t: 'startMatch' }); };
  NeonNet.endMatch     = function ()      { return this._send({ t: 'endMatch' }); };
  NeonNet.sendState    = function (s)     { s.t = 'state'; return this._send(s); };
  NeonNet.sendShot     = function (s)     { s.t = 'shoot'; return this._send(s); };
  NeonNet.sendWeapon   = function (w)     { return this._send({ t: 'weapon', weapon: w }); };
  NeonNet.sendReload   = function (on)    { return this._send({ t: 'reload', on: !!on }); };
  NeonNet.requestRespawn = function ()    { return this._send({ t: 'respawn' }); };

  // ---------------------------------------------------------------------------
  // Interpolation
  // ---------------------------------------------------------------------------
  function shortestAngle(a, b) {
    let d = (b - a) % (Math.PI * 2);
    if (d > Math.PI) d -= Math.PI * 2;
    if (d < -Math.PI) d += Math.PI * 2;
    return d;
  }

  /**
   * Remote players are rendered slightly in the past (interpDelayMs) and blended
   * between the two snapshots that bracket that moment. This is what keeps other
   * players gliding instead of snapping from packet to packet.
   * Returns a Map(id -> state) excluding the local player.
   */
  NeonNet.samplePlayers = function () {
    const out = new Map();
    const snaps = this._snapshots;
    if (snaps.length === 0) return out;

    const renderTs = Date.now() + this._clockOffset - this._interpDelayMs;

    let older = null, newer = null;
    for (let i = snaps.length - 1; i >= 0; i--) {
      if (snaps[i].ts <= renderTs) { older = snaps[i]; newer = snaps[i + 1] || null; break; }
    }
    if (!older) { older = snaps[0]; newer = snaps[1] || null; }

    if (!newer) {
      // Nothing newer to blend toward yet: hold the latest known pose rather than jumping.
      older.players.forEach((p, id) => { if (id !== this.selfId) out.set(id, Object.assign({}, p)); });
      return out;
    }

    const span = newer.ts - older.ts;
    const t = span > 0 ? Math.max(0, Math.min(1, (renderTs - older.ts) / span)) : 1;

    older.players.forEach((a, id) => {
      if (id === this.selfId) return;
      const b = newer.players.get(id);
      if (!b) { out.set(id, Object.assign({}, a)); return; }
      out.set(id, {
        id: id,
        x: a.x + (b.x - a.x) * t,
        y: a.y + (b.y - a.y) * t,
        z: a.z + (b.z - a.z) * t,
        yaw: a.yaw + shortestAngle(a.yaw, b.yaw) * t,
        pitch: a.pitch + (b.pitch - a.pitch) * t,
        crouch: b.crouch, sprint: b.sprint, grounded: b.grounded, moving: b.moving,
        dead: b.dead, reloading: b.reloading, shooting: a.shooting || b.shooting,
        hp: b.hp, weapon: b.weapon
      });
    });
    // Players that only exist in the newer snapshot (just joined/respawned)
    newer.players.forEach((b, id) => {
      if (id !== this.selfId && !out.has(id)) out.set(id, Object.assign({}, b));
    });
    return out;
  };

  NeonNet.isMultiplayer = function () { return this.connected && this.phase === 'playing'; };

  global.NeonNet = NeonNet;
})(typeof window !== 'undefined' ? window : this);
