/* ============================================================================
 * NEON STRIKE — LAN game server
 * ----------------------------------------------------------------------------
 * One small Node process does two jobs:
 *   1. Serves the game files over HTTP, so other PCs on the network just open
 *      http://<this-pc-lan-ip>:3000 in a browser. Nothing to install on their side.
 *   2. Runs the authoritative match state over WebSockets.
 *
 * LAN only by design: it binds to 0.0.0.0 and is reached by local IP. No port
 * forwarding, no public server, no matchmaking service.
 *
 * Authority model (see README):
 *   - Health, damage, elimination, respawn timing, match phase and map: SERVER.
 *   - Movement: client-simulated (it owns the collision mesh), server-validated
 *     for speed/bounds and re-broadcast. Clients never talk to each other.
 *   - Shooting: client raycasts against its local geometry and reports a CLAIM.
 *     The server re-validates the claim and computes the damage itself from its
 *     own weapon table. A client saying "I hit X for 500" gets that number thrown
 *     away; only server-side numbers are ever applied.
 * ==========================================================================*/
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');

let WebSocketServer;
try {
  WebSocketServer = require('ws').Server;
} catch (e) {
  console.error('\n[!] The "ws" package is missing.\n');
  console.error('    Run this once in the folder containing server.js:\n');
  console.error('        npm install\n');
  process.exit(1);
}

const PORT = Number(process.env.PORT) || 3000;
const ROOT = __dirname;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------
const CONFIG = {
  tickRate: 20,              // snapshots per second
  maxPlayers: 8,
  health: 150,
  regenDelay: 4.0,           // seconds without damage before regen starts
  regenRate: 12,             // hp per second
  respawnMs: 5000,
  maxSpeed: 14,              // generous: sprint 9 + slope/knockback headroom
  speedGraceMs: 400,
  shotRayToleranceDeg: 14,   // how far the claimed hit may sit off the claimed ray
  friendlyFire: true         // default for team mode; overridable per-match by the host
};

// ---------------------------------------------------------------------------
// Game modes — host-configurable, chosen from the lobby before START GAME.
//   ffa          — every player for themself. First to the kill limit wins.
//   team         — two auto-balanced teams (red/blue). First team to the score
//                   limit wins. Friendly fire is a separate per-match toggle.
//   infection    — one or more players start "infected"; anyone an infected
//                   player eliminates converts to infected too. Survivors win
//                   by outlasting the time limit; infected win by converting
//                   everyone.
//   lastStanding — no respawns. Last player alive wins the round.
// A match's settings are held on match.settings and only the host may change
// them, and only while the lobby is between rounds (phase === 'lobby').
// ---------------------------------------------------------------------------
const GAME_MODES = ['ffa', 'team', 'infection', 'lastStanding'];

// Server-side weapon table. This is the ONLY source of damage numbers.
const WEAPONS = {
  rifle:  { damage: 14,  headMult: 2.0, fireRate: 0.09, range: 80,  magSize: 30 },
  pistol: { damage: 18,  headMult: 2.2, fireRate: 0.18, range: 60,  magSize: 12 },
  sniper: { damage: 100, headMult: 1.5, fireRate: 1.2,  range: 200, magSize: 5  },
  // pellets: one trigger pull fires this many independent projectiles in a single
  // client message (see handleShotgunPellets) rather than one message each — sending
  // one message per pellet would trip the fireRate rate-limit below on every pellet
  // after the first. falloffMin/falloffPct match the client's damage-dropoff model.
  shotgun:{ damage: 9,   headMult: 1.8, fireRate: 0.85, range: 26,  magSize: 6, pellets: 9, falloffMin: 0.4, falloffPct: 0.6 }
};

// Spawn points per map. Kept deliberately tiny — the server needs positions, not geometry.
const SPAWNS = {
  downtown: [
    { x:   0, z:  14, yaw: 0 },        { x:   0, z: -14, yaw: Math.PI },
    { x:  14, z:   0, yaw: -Math.PI/2 },{ x: -14, z:   0, yaw: Math.PI/2 },
    { x:  32, z:  32, yaw: -2.4 },     { x: -32, z: -32, yaw: 0.75 },
    { x:  32, z: -32, yaw: 2.4 },      { x: -32, z:  32, yaw: -0.75 }
  ],
  skybridge: [
    { x:   0, z:  34, yaw: Math.PI },  { x:   0, z: -34, yaw: 0 },
    { x:  34, z:   0, yaw: -Math.PI/2 },{ x: -34, z:   0, yaw: Math.PI/2 },
    { x: -46, z: -46, yaw: 0.75 },     { x:  46, z:  46, yaw: -2.4 },
    { x:  46, z: -46, yaw: 2.4 },      { x: -46, z:  46, yaw: -0.75 }
  ],
  frostline: [
    { x:   0, z:  42, yaw: Math.PI },  { x:   0, z: -42, yaw: 0 },
    { x:  42, z:   0, yaw: -Math.PI/2 },{ x: -42, z:   0, yaw: Math.PI/2 },
    { x: -24, z: -10, yaw: 0.4 },       { x:  24, z:  10, yaw: -2.7 },
    { x: -24, z:  10, yaw: 0.8 },       { x:  24, z: -10, yaw: -2.3 }
  ]
};
const DEFAULT_MAP = 'downtown';

// ---------------------------------------------------------------------------
// LAN address detection (used by the welcome message, so clients can display the
// address other players must type — never hard-coded).
// ---------------------------------------------------------------------------
function lanAddresses() {
  const out = [];
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const net of ifaces[name] || []) {
      const family = typeof net.family === 'string' ? net.family : (net.family === 4 ? 'IPv4' : 'IPv6');
      if (family !== 'IPv4' || net.internal) continue;
      const priv = /^10\./.test(net.address) || /^192\.168\./.test(net.address) ||
                   /^172\.(1[6-9]|2\d|3[01])\./.test(net.address);
      out.push({ name, address: net.address, private: priv });
    }
  }
  out.sort((a, b) => (b.private ? 1 : 0) - (a.private ? 1 : 0));
  return out;
}
function primaryLanAddress() {
  const a = lanAddresses();
  return (a.length ? a[0].address : 'localhost') + ':' + PORT;
}

// ---------------------------------------------------------------------------
// Static file serving
// ---------------------------------------------------------------------------
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.ico': 'image/x-icon',
  '.svg': 'image/svg+xml', '.md': 'text/plain; charset=utf-8'
};

const httpServer = http.createServer((req, res) => {
  let urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
  if (urlPath === '/' ) urlPath = '/index.html';

  // Never serve anything outside the game folder, and never the server itself.
  const filePath = path.normalize(path.join(ROOT, urlPath));
  if (!filePath.startsWith(ROOT) || /(^|[\\/])(server\.js|package(-lock)?\.json|node_modules)([\\/]|$)/i.test(urlPath)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found: ' + urlPath); return; }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream',
      'Cache-Control': 'no-cache'
    });
    res.end(data);
  });
});

// ---------------------------------------------------------------------------
// Match state
// ---------------------------------------------------------------------------
const match = {
  phase: 'lobby',            // 'lobby' | 'playing'
  mapId: DEFAULT_MAP,
  hostId: null,
  players: new Map(),        // id -> player
  nextId: 1,
  startedAt: 0,
  // Host-configurable game settings for the *next* (or current) round.
  // scoreLimit / timeLimitMin: 0 means "no limit".
  settings: { mode: 'ffa', scoreLimit: 30, timeLimitMin: 0, friendlyFire: CONFIG.friendlyFire },
  teamScores: { red: 0, blue: 0 }
};

function makePlayer(id, ws, name) {
  return {
    id, ws, name: name || ('Player ' + id),
    isHost: false,
    // transform
    x: 0, y: 0, z: 0, yaw: 0, pitch: 0,
    // movement flags packed for the snapshot
    crouch: false, sprint: false, grounded: true, moving: false,
    // combat
    hp: CONFIG.health, dead: false, deadUntil: 0, killerId: null,
    weapon: 'rifle', reloading: false, shootAnimUntil: 0,
    lastDamageAt: 0, lastShotAt: 0,
    kills: 0, deaths: 0,
    // team: null in FFA/Last Player Standing; 'red'/'blue' in Team Deathmatch;
    // 'infected'/'survivor' in Infection. Reassigned by assignTeams() each round.
    team: null,
    // bookkeeping
    joinedAt: Date.now(), lastStateAt: 0, lastPos: null, spawnProtectUntil: 0,
    alive() { return !this.dead; }
  };
}

function publicPlayer(p) {
  return { id: p.id, name: p.name, isHost: p.isHost, hp: Math.round(p.hp),
           dead: p.dead, weapon: p.weapon, kills: p.kills, deaths: p.deaths, team: p.team || null };
}

function aliveCount() {
  let n = 0;
  for (const p of match.players.values()) if (!p.dead) n++;
  return n;
}

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    const t = arr[i]; arr[i] = arr[j]; arr[j] = t;
  }
  return arr;
}

/** Called at the start of every round. Reassigns every connected player's team
 *  according to the current mode; a no-op (team = null) outside team/infection. */
function assignTeams() {
  const players = shuffle(Array.from(match.players.values()));
  match.teamScores = { red: 0, blue: 0 };
  if (match.settings.mode === 'team') {
    players.forEach((p, i) => { p.team = i % 2 === 0 ? 'red' : 'blue'; });
  } else if (match.settings.mode === 'infection') {
    // Roughly one infected per five players, minimum one.
    const infectedCount = Math.max(1, Math.round(players.length / 5));
    players.forEach((p, i) => { p.team = i < infectedCount ? 'infected' : 'survivor'; });
  } else {
    players.forEach(p => { p.team = null; });
  }
}

function send(p, msg) {
  if (p.ws && p.ws.readyState === 1) {
    try { p.ws.send(JSON.stringify(msg)); } catch (e) { /* socket dying */ }
  }
}
function broadcast(msg, exceptId) {
  const s = JSON.stringify(msg);
  for (const p of match.players.values()) {
    if (p.id === exceptId) continue;
    if (p.ws && p.ws.readyState === 1) { try { p.ws.send(s); } catch (e) {} }
  }
}

function lobbyState() {
  return {
    t: 'lobby',
    phase: match.phase,
    mapId: match.mapId,
    hostId: match.hostId,
    max: CONFIG.maxPlayers,
    count: match.players.size,
    players: Array.from(match.players.values()).map(publicPlayer),
    settings: match.settings
  };
}
function pushLobby() { broadcast(lobbyState()); }

// --- spawning: pick the point furthest from every living player -------------
function pickSpawn() {
  const points = SPAWNS[match.mapId] || SPAWNS[DEFAULT_MAP];
  const others = Array.from(match.players.values()).filter(p => !p.dead && p.lastStateAt);
  let best = points[0], bestScore = -Infinity;
  points.forEach(sp => {
    let score = Infinity;
    others.forEach(o => {
      const d = Math.hypot(sp.x - o.x, sp.z - o.z);
      if (d < score) score = d;
    });
    if (score === Infinity) score = 9999;
    // tiny jitter so two simultaneous joins don't deterministically collide
    score += Math.random() * 0.01;
    if (score > bestScore) { bestScore = score; best = sp; }
  });
  return best;
}

function spawnPlayer(p) {
  const sp = pickSpawn();
  p.x = sp.x; p.y = 0; p.z = sp.z; p.yaw = sp.yaw; p.pitch = 0;
  p.hp = CONFIG.health;
  p.dead = false; p.deadUntil = 0; p.killerId = null;
  p.lastDamageAt = Date.now();
  p.reloading = false;
  p.lastPos = null;
  p.spawnProtectUntil = Date.now() + 1200;
  return sp;
}

// ---------------------------------------------------------------------------
// Damage / elimination — server owns all of this
// ---------------------------------------------------------------------------
function applyDamage(victim, attacker, rawAmount, part) {
  if (victim.dead || match.phase !== 'playing') return;
  if (Date.now() < victim.spawnProtectUntil) return;

  const amount = Math.max(0, Math.min(500, rawAmount));
  victim.hp -= amount;
  victim.lastDamageAt = Date.now();

  // Victim-only feedback, including where it came from (for the directional indicator).
  send(victim, {
    t: 'damage', amount: Math.round(amount), hp: Math.max(0, Math.round(victim.hp)),
    byId: attacker ? attacker.id : null,
    from: attacker ? [attacker.x, attacker.y + 1.5, attacker.z] : null,
    part: part || 'body'
  });
  if (attacker) {
    send(attacker, { t: 'hitConfirm', targetId: victim.id, amount: Math.round(amount),
                     head: part === 'head', lethal: victim.hp <= 0 });
  }

  if (victim.hp <= 0) eliminate(victim, attacker);
  else broadcast({ t: 'hp', id: victim.id, hp: Math.round(victim.hp) });
}

function eliminate(victim, attacker) {
  const mode = match.settings.mode;
  victim.hp = 0;
  victim.dead = true;
  victim.deaths++;
  victim.killerId = attacker ? attacker.id : null;

  // Scoring and Infection conversion both depend on the two players' teams, so
  // they're worked out together before anything is broadcast.
  let infected = false;
  if (attacker && attacker.id !== victim.id) {
    if (mode === 'infection') {
      if (attacker.team === 'infected' && victim.team === 'survivor') {
        victim.team = 'infected';           // the bite that keeps the mode going
        infected = true;
        attacker.kills++;
      } else if (attacker.team === 'survivor' && victim.team === 'infected') {
        attacker.kills++;
      }
      // infected-on-infected or survivor-on-survivor (friendly fire): no credit
    } else if (mode === 'team' && attacker.team && attacker.team === victim.team) {
      // team-kill: no personal or team credit
    } else {
      attacker.kills++;
      if (mode === 'team' && attacker.team) {
        match.teamScores[attacker.team] = (match.teamScores[attacker.team] || 0) + 1;
      }
    }
  }

  // Respawn timing: Last Player Standing removes you from the round entirely;
  // an Infection conversion gets you back in almost immediately; everything
  // else uses the normal respawn timer.
  let respawnMs;
  if (mode === 'lastStanding') {
    respawnMs = 0;
    victim.deadUntil = Infinity;
  } else if (infected) {
    respawnMs = Math.min(CONFIG.respawnMs, 1500);
    victim.deadUntil = Date.now() + respawnMs;
  } else {
    respawnMs = CONFIG.respawnMs;
    victim.deadUntil = Date.now() + respawnMs;
  }

  // Everyone sees the elimination; the victim gets the authoritative respawn time.
  broadcast({
    t: 'eliminated', id: victim.id, byId: victim.killerId,
    byName: attacker ? attacker.name : null,
    mode, infected, final: mode === 'lastStanding',
    respawnAt: victim.deadUntil, respawnMs,
    remaining: mode === 'lastStanding' ? aliveCount() : null,
    scores: Array.from(match.players.values()).map(publicPlayer),
    teamScores: match.teamScores || null
  });

  checkWinCondition(attacker);
}

/** Checked after every elimination. Ends the round the moment this mode's win
 *  condition is met; a timed fallback for score/time limits lives in tick(). */
function checkWinCondition(attacker) {
  if (match.phase !== 'playing') return;
  const s = match.settings;
  if (s.mode === 'ffa') {
    if (s.scoreLimit > 0 && attacker && attacker.kills >= s.scoreLimit) endRound('score', attacker, null);
  } else if (s.mode === 'team') {
    if (s.scoreLimit > 0) {
      if (match.teamScores.red >= s.scoreLimit) endRound('score', null, 'red');
      else if (match.teamScores.blue >= s.scoreLimit) endRound('score', null, 'blue');
    }
  } else if (s.mode === 'infection') {
    let survivors = 0;
    for (const p of match.players.values()) if (p.team === 'survivor') survivors++;
    if (survivors === 0) endRound('infection', null, 'infected');
  } else if (s.mode === 'lastStanding') {
    const alive = aliveCount();
    if (alive <= 1) {
      let last = null;
      if (alive === 1) { for (const p of match.players.values()) if (!p.dead) { last = p; break; } }
      endRound('lastStanding', last, null);
    }
  }
}

/** Ends the current round and drops everyone back to the lobby, where the host
 *  can tweak settings and press START GAME again for another round. */
function endRound(reason, winnerPlayer, winnerTeam) {
  if (match.phase !== 'playing') return;
  match.phase = 'lobby';
  broadcast({
    t: 'matchEnd', reason,
    winnerId: winnerPlayer ? winnerPlayer.id : null,
    winnerName: winnerPlayer ? winnerPlayer.name : null,
    winnerTeam: winnerTeam || null,
    scores: Array.from(match.players.values()).map(publicPlayer),
    teamScores: match.teamScores || null
  });
  console.log('[<] Round ended (' + reason + ')' +
    (winnerPlayer ? ' — winner: ' + winnerPlayer.name : winnerTeam ? ' — winner: ' + winnerTeam + ' team' : ''));
  pushLobby();
}

function respawn(p) {
  const sp = spawnPlayer(p);
  send(p, { t: 'respawned', id: p.id, self: true, x: sp.x, y: 0, z: sp.z, yaw: sp.yaw, hp: p.hp });
  broadcast({ t: 'respawned', id: p.id, x: sp.x, y: 0, z: sp.z, yaw: sp.yaw, hp: p.hp }, p.id);
}

// ---------------------------------------------------------------------------
// Shot validation
// ---------------------------------------------------------------------------
function norm(v) {
  const l = Math.hypot(v[0], v[1], v[2]) || 1;
  return [v[0] / l, v[1] / l, v[2] / l];
}

/**
 * The client reports: "I fired weapon W from O along D, and my raycast says I hit
 * player T on part P". The server keeps the shot event (so everyone sees a tracer)
 * but independently decides whether the hit is plausible and what it is worth.
 */
function handleShot(shooter, msg) {
  if (match.phase !== 'playing' || shooter.dead) return;
  const wpn = WEAPONS[msg.weapon];
  if (!wpn) return;

  // Rate limit against the weapon's own fire rate (with a little network slack).
  const now = Date.now();
  if (now - shooter.lastShotAt < wpn.fireRate * 1000 * 0.8) return;
  shooter.lastShotAt = now;
  shooter.weapon = msg.weapon;
  shooter.shootAnimUntil = now + 90;

  const origin = Array.isArray(msg.origin) ? msg.origin.map(Number) : [shooter.x, shooter.y + 1.6, shooter.z];

  // A shotgun shell arrives as ONE message carrying every pellet, precisely so this
  // rate limit only has to pass once per trigger pull instead of once per pellet.
  if (wpn.pellets) { handleShotgunPellets(shooter, wpn, origin, msg); return; }

  const dir = norm(Array.isArray(msg.dir) ? msg.dir.map(Number) : [0, 0, -1]);

  // Replicate the shot to everyone else so they see tracer + muzzle flash.
  broadcast({ t: 'shot', id: shooter.id, weapon: msg.weapon,
              origin: origin.map(n => +n.toFixed(2)),
              end: Array.isArray(msg.end) ? msg.end.map(n => +Number(n).toFixed(2)) : null }, shooter.id);

  const claim = msg.hit;
  if (!claim || claim.id == null) return;

  const victim = match.players.get(claim.id);
  if (!victim || victim.dead || victim.id === shooter.id) return;
  // Friendly fire only means anything (and is only ever toggled) in Team mode —
  // in every other mode team is null on both sides, which must never match here.
  if (match.settings.mode === 'team' && !match.settings.friendlyFire && victim.team && victim.team === shooter.team) return;

  // --- validation against server-held positions ---
  const part = claim.part === 'head' ? 'head' : 'body';
  const aimY = victim.y + (part === 'head' ? 1.55 : 1.0);
  const to = [victim.x - origin[0], aimY - origin[1], victim.z - origin[2]];
  const dist = Math.hypot(to[0], to[1], to[2]);

  if (dist > wpn.range * 1.05) return;                     // out of the weapon's range
  const n = norm(to);
  const dot = n[0] * dir[0] + n[1] * dir[1] + n[2] * dir[2];
  const angle = Math.acos(Math.max(-1, Math.min(1, dot))) * 180 / Math.PI;
  // Allow more angular slack up close, where a small offset is a big angle.
  const tolerance = CONFIG.shotRayToleranceDeg + Math.min(25, 40 / Math.max(1, dist));
  if (angle > tolerance) return;                            // claimed hit isn't on the claimed ray

  // Damage is computed HERE, from the server's table. The client's opinion is ignored.
  const damage = wpn.damage * (part === 'head' ? wpn.headMult : 1);
  applyDamage(victim, shooter, damage, part);
}

/**
 * One shotgun shell = one message = wpn.pellets independent claims, each validated
 * exactly like a single-projectile shot (range, ray-tolerance angle, own falloff).
 * Hits on the same victim from the same shell are summed into one applyDamage call
 * so the victim sees one damage number / hit event for the shell, not nine.
 */
function handleShotgunPellets(shooter, wpn, origin, msg) {
  const pelletsIn = Array.isArray(msg.pellets) ? msg.pellets.slice(0, wpn.pellets) : [];

  // One broadcast for the whole shell, carrying every pellet's endpoint so onlookers
  // can draw the full spray of tracers instead of just one line.
  broadcast({
    t: 'shot', id: shooter.id, weapon: msg.weapon,
    origin: origin.map(n => +n.toFixed(2)),
    ends: pelletsIn.map(p => Array.isArray(p.end) ? p.end.map(n => +Number(n).toFixed(2)) : null)
  }, shooter.id);

  const perVictim = new Map(); // victim.id -> { victim, dmg, anyHead }
  for (const p of pelletsIn) {
    const claim = p.hit;
    if (!claim || claim.id == null) continue;

    const victim = match.players.get(claim.id);
    if (!victim || victim.dead || victim.id === shooter.id) continue;
    if (match.settings.mode === 'team' && !match.settings.friendlyFire && victim.team && victim.team === shooter.team) continue;

    const dir = norm(Array.isArray(p.dir) ? p.dir.map(Number) : [0, 0, -1]);
    const part = claim.part === 'head' ? 'head' : 'body';
    const aimY = victim.y + (part === 'head' ? 1.55 : 1.0);
    const to = [victim.x - origin[0], aimY - origin[1], victim.z - origin[2]];
    const dist = Math.hypot(to[0], to[1], to[2]);
    if (dist > wpn.range * 1.05) continue;

    const n = norm(to);
    const dot = n[0] * dir[0] + n[1] * dir[1] + n[2] * dir[2];
    const angle = Math.acos(Math.max(-1, Math.min(1, dot))) * 180 / Math.PI;
    const tolerance = CONFIG.shotRayToleranceDeg + Math.min(25, 40 / Math.max(1, dist));
    if (angle > tolerance) continue;

    const falloff = Math.max(wpn.falloffMin != null ? wpn.falloffMin : 1,
                              1 - (dist / wpn.range) * (wpn.falloffPct || 0));
    const dmg = wpn.damage * (part === 'head' ? wpn.headMult : 1) * falloff;

    const acc = perVictim.get(victim.id) || { victim, dmg: 0, anyHead: false };
    acc.dmg += dmg;
    if (part === 'head') acc.anyHead = true;
    perVictim.set(victim.id, acc);
  }

  perVictim.forEach(acc => applyDamage(acc.victim, shooter, acc.dmg, acc.anyHead ? 'head' : 'body'));
}

// ---------------------------------------------------------------------------
// Movement validation
// ---------------------------------------------------------------------------
function handleState(p, s) {
  if (match.phase !== 'playing') return;
  const now = Date.now();
  const x = Number(s.x), y = Number(s.y), z = Number(s.z);
  if (!isFinite(x) || !isFinite(y) || !isFinite(z)) return;

  if (p.dead) return;   // a dead player's transform is frozen server-side

  // Speed check: reject teleports, accept normal running. On a LAN this is a sanity
  // guard rather than real anti-cheat — the structure is here to tighten later.
  if (p.lastPos && p.lastStateAt) {
    const dt = Math.max(0.001, (now - p.lastStateAt) / 1000);
    const moved = Math.hypot(x - p.lastPos[0], z - p.lastPos[2]);
    const limit = CONFIG.maxSpeed * dt + CONFIG.maxSpeed * (CONFIG.speedGraceMs / 1000);
    if (moved > limit) {
      // Snap the client back to the last accepted position instead of trusting it.
      send(p, { t: 'correction', x: p.x, y: p.y, z: p.z });
      return;
    }
  }

  p.x = x; p.y = y; p.z = z;
  p.yaw = Number(s.yaw) || 0;
  p.pitch = Number(s.pitch) || 0;
  p.crouch = !!s.crouch; p.sprint = !!s.sprint;
  p.grounded = !!s.grounded; p.moving = !!s.moving;
  if (s.weapon && WEAPONS[s.weapon]) p.weapon = s.weapon;
  p.reloading = !!s.reloading;
  p.lastPos = [x, y, z];
  p.lastStateAt = now;
}

// ---------------------------------------------------------------------------
// Snapshot loop
// ---------------------------------------------------------------------------
// Movement flags are packed into one integer to keep snapshots small.
function packFlags(p) {
  return (p.crouch ? 1 : 0) | (p.sprint ? 2 : 0) | (p.grounded ? 4 : 0) |
         (p.moving ? 8 : 0) | (p.dead ? 16 : 0) | (p.reloading ? 32 : 0) |
         (Date.now() < p.shootAnimUntil ? 64 : 0);
}
const WEAPON_IDX = { rifle: 0, pistol: 1, sniper: 2, shotgun: 3 };

function tick() {
  const now = Date.now();

  if (match.phase === 'playing') {
    // Time-limit fallback: score/kill limits end a round instantly (checked in
    // checkWinCondition after every elimination); this is what ends a round
    // that never reaches its limit, and the only way Infection survivors win.
    const s = match.settings;
    if (s.timeLimitMin > 0 && match.startedAt && (now - match.startedAt) >= s.timeLimitMin * 60000) {
      if (s.mode === 'infection') {
        endRound('time', null, 'survivor');
      } else if (s.mode === 'team') {
        const ts = match.teamScores;
        const winner = ts.red === ts.blue ? null : (ts.red > ts.blue ? 'red' : 'blue');
        endRound('time', null, winner);
      } else if (s.mode === 'ffa') {
        let best = null;
        for (const p of match.players.values()) { if (!best || p.kills > best.kills) best = p; }
        endRound('time', best, null);
      } else {
        endRound('time', null, null);
      }
      return;
    }

    // Server-side regen. Dead players are skipped entirely: an eliminated player can
    // never regenerate their way out of the respawn screen.
    const dt = 1 / CONFIG.tickRate;
    for (const p of match.players.values()) {
      if (p.dead || p.hp <= 0) continue;
      if (p.hp < CONFIG.health && now - p.lastDamageAt > CONFIG.regenDelay * 1000) {
        const before = Math.round(p.hp);
        p.hp = Math.min(CONFIG.health, p.hp + CONFIG.regenRate * dt);
        if (Math.round(p.hp) !== before) broadcast({ t: 'hp', id: p.id, hp: Math.round(p.hp) });
      }
    }

    const players = [];
    for (const p of match.players.values()) {
      players.push([
        p.id, +p.x.toFixed(2), +p.y.toFixed(2), +p.z.toFixed(2),
        +p.yaw.toFixed(3), +p.pitch.toFixed(3),
        packFlags(p), Math.round(p.hp), WEAPON_IDX[p.weapon] || 0
      ]);
    }
    broadcast({ t: 's', ts: now, p: players });
  }
}
setInterval(tick, 1000 / CONFIG.tickRate);

// ---------------------------------------------------------------------------
// WebSocket handling
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws, req) => {
  if (match.players.size >= CONFIG.maxPlayers) {
    try { ws.send(JSON.stringify({ t: 'error', code: 'full', message: 'Server is full.' })); } catch (e) {}
    ws.close(); return;
  }

  const id = match.nextId++;
  const player = makePlayer(id, ws, null);
  let joined = false;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.t !== 'string') return;

    // ---- join handshake ----
    if (msg.t === 'join') {
      if (joined) return;
      player.name = String(msg.name || '').slice(0, 16) || ('Player ' + id);

      // The first client asking to host becomes host. The host is the person who
      // launched this process and pressed HOST in their own browser.
      if (msg.role === 'host') {
        if (match.hostId !== null && match.players.has(match.hostId)) {
          send(player, { t: 'error', code: 'host-taken', message: 'This server already has a host.' });
          ws.close(); return;
        }
        player.isHost = true;
        match.hostId = id;
        if (msg.mapId && SPAWNS[msg.mapId]) match.mapId = msg.mapId;
      } else if (match.hostId === null) {
        send(player, { t: 'error', code: 'no-host', message: 'No host has opened this server yet.' });
        ws.close(); return;
      }

      joined = true;
      match.players.set(id, player);

      // A player who connects mid-round needs a team the moment they join —
      // assignTeams() only runs at the start of a round, so late joiners are
      // slotted in here instead: onto whichever team mode is lighter, or
      // straight into the survivor pool in Infection.
      if (match.phase === 'playing') {
        if (match.settings.mode === 'team') {
          let red = 0, blue = 0;
          for (const p of match.players.values()) { if (p.team === 'red') red++; else if (p.team === 'blue') blue++; }
          player.team = red <= blue ? 'red' : 'blue';
        } else if (match.settings.mode === 'infection') {
          player.team = 'survivor';
        }
      }
      spawnPlayer(player);

      send(player, {
        t: 'welcome', id, isHost: player.isHost,
        lan: primaryLanAddress(),
        config: { tickRate: CONFIG.tickRate, maxPlayers: CONFIG.maxPlayers,
                  health: CONFIG.health, respawnMs: CONFIG.respawnMs },
        phase: match.phase, mapId: match.mapId, settings: match.settings
      });
      if (match.phase === 'playing') {
        send(player, { t: 'matchStart', mapId: match.mapId, settings: match.settings,
                       spawn: { x: player.x, y: 0, z: player.z, yaw: player.yaw }, late: true });
      }
      broadcast({ t: 'playerJoin', player: publicPlayer(player) }, id);
      pushLobby();
      console.log(`[+] ${player.name} (#${id})${player.isHost ? ' [HOST]' : ''} joined — ${match.players.size}/${CONFIG.maxPlayers}`);
      return;
    }

    if (!joined) return;

    switch (msg.t) {
      case 'setMap':
        // Only the host picks the map, and only before the match starts.
        if (!player.isHost || match.phase !== 'lobby') return;
        if (!SPAWNS[msg.mapId]) return;
        match.mapId = msg.mapId;
        broadcast({ t: 'mapChange', mapId: match.mapId });
        pushLobby();
        break;

      case 'setSettings': {
        // Only the host picks game settings, and only from the lobby between rounds.
        if (!player.isHost || match.phase !== 'lobby') return;
        const s = msg.settings || {};
        const mode = GAME_MODES.indexOf(s.mode) !== -1 ? s.mode : match.settings.mode;
        const scoreLimit = Number.isFinite(s.scoreLimit) ? Math.max(0, Math.min(200, Math.round(s.scoreLimit))) : match.settings.scoreLimit;
        const timeLimitMin = Number.isFinite(s.timeLimitMin) ? Math.max(0, Math.min(60, Math.round(s.timeLimitMin))) : match.settings.timeLimitMin;
        const friendlyFire = typeof s.friendlyFire === 'boolean' ? s.friendlyFire : match.settings.friendlyFire;
        match.settings = { mode, scoreLimit, timeLimitMin, friendlyFire };
        pushLobby();
        break;
      }

      case 'startMatch': {
        if (!player.isHost || match.phase !== 'lobby') return;
        match.phase = 'playing';
        match.startedAt = Date.now();
        for (const p of match.players.values()) { p.kills = 0; p.deaths = 0; }
        assignTeams();
        for (const p of match.players.values()) spawnPlayer(p);
        for (const p of match.players.values()) {
          send(p, { t: 'matchStart', mapId: match.mapId, settings: match.settings,
                    spawn: { x: p.x, y: 0, z: p.z, yaw: p.yaw },
                    players: Array.from(match.players.values()).map(publicPlayer) });
        }
        console.log(`[>] Match started on "${match.mapId}" (${match.settings.mode}) with ${match.players.size} player(s)`);
        break;
      }

      case 'endMatch':
        if (!player.isHost) return;
        endRound('hostEnded', null, null);
        break;

      case 'state':   handleState(player, msg); break;
      case 'shoot':   handleShot(player, msg);  break;

      case 'weapon':
        if (WEAPONS[msg.weapon]) player.weapon = msg.weapon;
        break;
      case 'reload':
        player.reloading = !!msg.on;
        break;

      case 'respawn': {
        // Authoritative gate: the client's own countdown is irrelevant here.
        if (!player.dead || match.phase !== 'playing') return;
        if (Date.now() < player.deadUntil) {
          send(player, { t: 'respawnDenied', respawnAt: player.deadUntil });
          return;
        }
        respawn(player);
        break;
      }

      case 'ping':
        send(player, { t: 'pong', ts: msg.ts });
        break;
    }
  });

  const drop = () => {
    if (!joined) return;
    match.players.delete(id);
    console.log(`[-] ${player.name} (#${id}) left — ${match.players.size}/${CONFIG.maxPlayers}`);
    broadcast({ t: 'playerLeave', id });

    if (match.hostId === id) {
      // Host left: the match cannot continue meaningfully, so everyone goes back to menu.
      match.hostId = null;
      match.phase = 'lobby';
      broadcast({ t: 'hostLeft', message: 'The host left the game.' });
      for (const p of match.players.values()) { try { p.ws.close(); } catch (e) {} }
      match.players.clear();
    }
    pushLobby();
  };
  ws.on('close', drop);
  ws.on('error', drop);           // unexpected drops take the same path as a clean leave
});

httpServer.on('error', err => {
  if (err && err.code === 'EADDRINUSE') {
    console.error(`\n[!] Port ${PORT} is already in use.`);
    console.error(`    Another copy of the server may be running, or pick a different port:\n`);
    console.error(`        PORT=3001 node server.js        (macOS / Linux)`);
    console.error(`        set PORT=3001 && node server.js (Windows cmd)\n`);
  } else {
    console.error('\n[!] Server failed to start:', err && err.message ? err.message : err, '\n');
  }
  process.exit(1);
});

httpServer.listen(PORT, '0.0.0.0', () => {
  const addrs = lanAddresses();
  const primary = addrs.length ? addrs[0].address : 'localhost';
  console.log('');
  console.log('  ███ NEON STRIKE — LAN server running');
  console.log('  ' + '-'.repeat(52));
  console.log('  On this PC (the host):   http://localhost:' + PORT);
  if (addrs.length) {
    console.log('  Others on your network:  http://' + primary + ':' + PORT);
    if (addrs.length > 1) {
      console.log('');
      console.log('  Other addresses on this machine, if that one does not work:');
      addrs.slice(1).forEach(a => console.log('    http://' + a.address + ':' + PORT + '   (' + a.name + ')'));
    }
  } else {
    console.log('  [!] No LAN address detected — is this machine on a network?');
  }
  console.log('  ' + '-'.repeat(52));
  console.log('  Host: open the URL above, click HOST, then START GAME.');
  console.log('  Others: open http://' + primary + ':' + PORT + ' and click JOIN GAME.');
  console.log('');
});
