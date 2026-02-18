// ═══════════════════════════════════════════════════════════
// PirateIsles — Сервер v0.1 (Redis multi-key)
// ═══════════════════════════════════════════════════════════

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const Redis = require('ioredis');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use('/assets', express.static(path.join(__dirname, 'assets')));
app.use(express.static(path.join(__dirname)));

// ── Redis ─────────────────────────────────────────────────
const redisUrl = process.env.REDIS_URL;
const redisOpts = redisUrl
  ? { enableReadyCheck: true, maxRetriesPerRequest: 3 }
  : {
      host: process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.REDIS_PORT || '6379', 10),
      enableReadyCheck: true,
      maxRetriesPerRequest: 3
    };
const redis = redisUrl ? new Redis(redisUrl, redisOpts) : new Redis(redisOpts);

redis.on('connect', () => console.log('[REDIS] Connected'));
redis.on('error', (err) => console.error('[REDIS] Error:', err.message));

// ── Константы — баланс ───────────────────────────────────
const TICK_RATE = 1000;              // Economy tick (1 sec)
const RAID_CHECK_TICK = 5000;        // Check raid completions (5 sec)
const AUTOSAVE_TICK = 20000;         // Persist to Redis (20 sec)
const STATE_BROADCAST_TICK = 2000;   // Broadcast state (2 sec)

// Ресурсы
const BASE_RUM_PER_SEC = 2;         // Начальный доход рома
const RUM_PER_TAVERN_LVL = 1;       // +1 rum/sec за каждый уровень таверны выше 1

// Стоимости построек (x2 scaling)
const TAVERN_BASE_COST = 400;       // rum
const DOCK_BASE_COST = 300;          // wood
const CANNON_BASE_COST_RUM = 300;    // rum часть
const CANNON_BASE_COST_WOOD = 200;   // wood часть
const COST_MULTIPLIER = 2;

// Лодки
const BASE_BOAT_CAPACITY = 2;       // Начальная вместимость
const BOATS_PER_DOCK_LVL = 1;       // +1 лодка за уровень дока

// Рейды (AI-лодки)
const RAID_MIN_TIME = 1 * 60 * 1000;   // 1 мин (тест; боевое: 8 мин)
const RAID_MAX_TIME = 1 * 60 * 1000;   // 1 мин (тест; боевое: 20 мин)
const RAID_LOOT_MIN = 150;
const RAID_LOOT_MAX = 1200;
const BOAT_LOSS_CHANCE = 0.12;          // 12% базовый шанс потери

// PvP
const PVP_STEAL_MIN = 0.45;
const PVP_STEAL_MAX = 0.65;
const SHIELD_MIN_HOURS = 4;
const SHIELD_MAX_HOURS = 8;
const DEBRIS_RATIO = 0.40;              // 40% от украденного → debris
const DEBRIS_ATTACKER_SHARE = 0.70;     // 70% debris → атакующий сразу
const DEBRIS_DEFENDER_SHARE = 0.30;     // 30% debris → защитник пассивно
const PVP_COOLDOWN = 30 * 60 * 1000;   // 30 мин кулдаун

// Оффлайн
const OFFLINE_RATE = 0.40;              // 40% от онлайн-скорости
const OFFLINE_MAX_HOURS = 12;

// Авто-ремонт
const AUTO_REPAIR_PER_HOUR = 0.015;     // 1.5% уровня острова в час
const AUTO_REPAIR_RUM_PER_LVL = 10;     // стоимость ремонта в rum за тик за уровень
const AUTO_REPAIR_PROGRESS_PER_SEC = AUTO_REPAIR_PER_HOUR / 3600; // доля прогресса за 1 сек на 1 уровень

// Destruction / Legacy
const LEGACY_BONUS_PER_DESTROY = 0.01;  // +1% дохода

// Auth
const NICK_MIN = 3;
const NICK_MAX = 16;

// World map
const MAP_W = 3000;
const MAP_H = 2000;
// Минимальная дистанция между стартовыми островами игроков на карте
const PLAYER_MIN_DISTANCE = 450;
const PVP_SPEED = 70;           // world-units/sec — медленнее, чем за ресурсами (BOAT_SPEED)
const BOAT_SPEED = 120;         // world-units/sec — одинаковая скорость до острова (архипелаг, ресурсные)
const PVP_MISSION_TICK = 100;   // ms — тик движения PvP-лодок

// Архипелаг (острова-спутники у базы)
const ARCHI_WOOD_PER_SEC   = 0.4;            // дерево/сек пассивно с каждого островка
const ARCHI_HARVEST_MIN_MS = 5000;           // минимум 5 сек в пути (даже для близких островов)
const ARCHI_DEPLETION      = 15 * 60 * 1000; // 15 мин до восстановления острова
const ARCHI_LOSS_CHANCE    = 0.05;           // 5% шанс потери лодки
// Лут по типу: [min, max]
const ARCHI_LOOT = {
  wood:  { resource: 'wood',  min: 60,  max: 180 },
  stone: { resource: 'gold',  min: 30,  max: 100 },
  rum:   { resource: 'rum',   min: 80,  max: 250 },
};

// ── In-memory хранилище ──────────────────────────────────
// Острова ресурсов на мировой карте
const RESOURCE_ISLAND_COUNT = 20;
const RESOURCE_HARVEST_MIN_MS = 5000;         // минимум 5 сек в пути до ресурсного острова
const RESOURCE_DEPLETION    = 2 * 60 * 1000;  // 2 мин (тест; боевое: ~30 мин)
const RESOURCE_LOSS_CHANCE  = 0.08;
const RESOURCE_LOOT = {
  wood: { resource: 'wood', min: 100, max: 300 },
  gold: { resource: 'gold', min:  80, max: 200 },
  rum:  { resource: 'rum',  min: 150, max: 400 },
};

const players = {};       // ключ — nick
const activeRaids = [];   // { playerId, startTime, duration, lootRoll }
const pvpMissions = [];   // { id, owner, targetNick, x, y, tx, ty, speed, ownerColor }
const resourceIslands = []; // { id, x, y, type, size, depleted_until }
let resourceIslandsDirty = false; // флаг: нужно ли сохранить острова в Redis после деплеции

// ── Утилиты ──────────────────────────────────────────────
function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function hashPassword(pw) {
  return crypto.createHash('sha256').update(pw).digest('hex');
}

// Подбор свободной позиции на мировой карте с учётом уже существующих игроков
function findFreePlayerPosition() {
  const margin = 300;
  const maxAttempts = 50;
  const minDist = PLAYER_MIN_DISTANCE;

  const existing = Object.values(players);
  // Если игроков ещё нет — можно ставить куда угодно в допустимую зону
  if (existing.length === 0) {
    return {
      x: randInt(margin, MAP_W - margin),
      y: randInt(margin, MAP_H - margin)
    };
  }

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const x = randInt(margin, MAP_W - margin);
    const y = randInt(margin, MAP_H - margin);
    let ok = true;
    for (const p of existing) {
      if (typeof p.pos_x !== 'number' || typeof p.pos_y !== 'number') continue;
      const dx = p.pos_x - x;
      const dy = p.pos_y - y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < minDist) {
        ok = false;
        break;
      }
    }
    if (ok) {
      return { x, y };
    }
  }

  // Фолбэк: не нашли за разумное число попыток — ставим случайно
  return {
    x: randInt(margin, MAP_W - margin),
    y: randInt(margin, MAP_H - margin)
  };
}

// Детерминированный сид по нику (зеркало клиентской функции)
function archSeed(nick, i) {
  let h = 0;
  for (let j = 0; j < nick.length; j++) h = (h * 31 + nick.charCodeAt(j)) & 0xffffffff;
  return Math.abs((h ^ (i * 2654435761)) >>> 0);
}
function getArchipelagoCount(nick) {
  return 2 + (archSeed(nick, 0) % 3); // 2–4 островка
}

// Позиция острова архипелага относительно базы (зеркало клиентской getArchipelago)
function getArchipelagoPosition(nick, idx) {
  const count = getArchipelagoCount(nick);
  if (idx < 0 || idx >= count) return { dx: 0, dy: 0 };
  const s1 = archSeed(nick, idx * 3 + 1);
  const s2 = archSeed(nick, idx * 3 + 2);
  const baseAngleDeg = (360 / count) * idx + (s1 % 40) - 20;
  const angle = (baseAngleDeg * Math.PI) / 180;
  const dist = 190 + (s2 % 50);
  return {
    dx: Math.cos(angle) * dist,
    dy: Math.sin(angle) * dist
  };
}

function generateResourceIslands() {
  const cols = 5, rows = 4;
  const cellW = MAP_W / cols;
  const cellH = MAP_H / rows;
  const types = ['wood', 'wood', 'gold', 'rum'];
  let id = 0;
  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) {
      const x = Math.round(cellW * c + cellW * 0.15 + Math.random() * cellW * 0.7);
      const y = Math.round(cellH * r + cellH * 0.15 + Math.random() * cellH * 0.7);
      resourceIslands.push({
        id: id++, x, y,
        type: types[Math.floor(Math.random() * types.length)],
        size: 18 + Math.round(Math.random() * 14),
        depleted_until: 0
      });
    }
  }
  console.log(`[WORLD] Generated ${resourceIslands.length} resource islands`);
}

function getResourceIslandsPublic() {
  return resourceIslands.map(i => ({
    id: i.id, x: i.x, y: i.y,
    type: i.type, size: i.size,
    depleted_until: i.depleted_until
  }));
}

function boatCapacity(dockLevel) {
  return BASE_BOAT_CAPACITY + (dockLevel - 1) * BOATS_PER_DOCK_LVL;
}

function tavernCost(level) {
  return Math.floor(TAVERN_BASE_COST * Math.pow(COST_MULTIPLIER, level - 1));
}

function dockCost(level) {
  return Math.floor(DOCK_BASE_COST * Math.pow(COST_MULTIPLIER, level - 1));
}

function cannonCostRum(level) {
  return Math.floor(CANNON_BASE_COST_RUM * Math.pow(COST_MULTIPLIER, level));
}

function cannonCostWood(level) {
  return Math.floor(CANNON_BASE_COST_WOOD * Math.pow(COST_MULTIPLIER, level));
}

function rumRate(player) {
  const base = BASE_RUM_PER_SEC + (player.tavern_level - 1) * RUM_PER_TAVERN_LVL;
  const legacyMult = 1 + player.legacy_bonus;
  return Math.floor(base * legacyMult);
}

// ── Redis persistence (multi-key) ────────────────────────
const PLAYERS_SET_KEY = 'players:nicks';
const RESOURCE_ISLANDS_KEY = 'resourceIslands';

function pKey(id) { return `player:${id}`; }
function pResKey(id) { return `player:${id}:resources`; }
function pCdKey(id) { return `player:${id}:cooldowns`; }
function pDestrKey(id) { return `player:${id}:destruction_state`; }
function pLegacyKey(id) { return `player:${id}:legacy_bonus`; }
function pDebrisKey(id) { return `player:${id}:debris`; }
function pArchiDepletedKey(id) { return `player:${id}:archi_depleted`; }

async function persistPlayer(id) {
  const p = players[id];
  if (!p) return;

  const pipe = redis.pipeline();

  // player:<id> hash — основные данные
  pipe.hset(pKey(id), {
    nick: p.nick,
    password_hash: p.password_hash || '',
    island_level: String(p.island_level),
    pos_x: String(p.pos_x),
    pos_y: String(p.pos_y),
    last_active: String(p.last_active),
    tavern_level: String(p.tavern_level),
    dock_level: String(p.dock_level),
    cannon_level: String(p.cannon_level),
    boats: String(p.boats),
    shield_until: String(p.shield_until),
    created_at: String(p.created_at),
    color: p.color
  });

  // player:<id>:resources hash
  pipe.hset(pResKey(id), {
    rum: String(Math.floor(p.rum)),
    gold: String(Math.floor(p.gold)),
    wood: String(Math.floor(p.wood))
  });

  // player:<id>:cooldowns hash
  pipe.hset(pCdKey(id), {
    raid: String(p.raid_cooldown),
    pvp: String(p.pvp_cooldown)
  });

  // Скаляры
  pipe.set(pDestrKey(id), String(p.destruction_state));
  pipe.set(pLegacyKey(id), String(p.legacy_bonus));

  // Debris
  if (p.debris_gold > 0 && p.debris_ttl > Date.now()) {
    pipe.hset(pDebrisKey(id), {
      gold: String(Math.floor(p.debris_gold)),
      ttl: String(p.debris_ttl)
    });
  } else {
    pipe.del(pDebrisKey(id));
  }

  // Архипелаг: деплеция островов (сохраняем только актуальные timestamp)
  const archiDep = p.archi_depleted || {};
  const now = Date.now();
  const archiDepFiltered = {};
  for (const k of Object.keys(archiDep)) {
    if (archiDep[k] > now) archiDepFiltered[k] = archiDep[k];
  }
  pipe.set(pArchiDepletedKey(id), JSON.stringify(archiDepFiltered));

  pipe.sadd(PLAYERS_SET_KEY, id);

  await pipe.exec();
}

function persist(id) {
  if (id) persistPlayer(id).catch(err => console.warn('[REDIS] Persist error:', id, err.message));
}

async function removePlayer(id) {
  const pipe = redis.pipeline();
  pipe.del(pKey(id));
  pipe.del(pResKey(id));
  pipe.del(pCdKey(id));
  pipe.del(pDestrKey(id));
  pipe.del(pLegacyKey(id));
  pipe.del(pDebrisKey(id));
  pipe.del(pArchiDepletedKey(id));
  pipe.srem(PLAYERS_SET_KEY, id);
  await pipe.exec();
}

function removePersist(id) {
  if (id) removePlayer(id).catch(err => console.warn('[REDIS] Remove error:', id, err.message));
}

async function loadPlayersFromRedis() {
  const ids = await redis.smembers(PLAYERS_SET_KEY);
  const out = {};

  for (const id of ids) {
    try {
      const pipe = redis.pipeline();
      pipe.hgetall(pKey(id));          // 0
      pipe.hgetall(pResKey(id));       // 1
      pipe.hgetall(pCdKey(id));        // 2
      pipe.get(pDestrKey(id));         // 3
      pipe.get(pLegacyKey(id));        // 4
      pipe.hgetall(pDebrisKey(id));    // 5
      pipe.get(pArchiDepletedKey(id)); // 6
      const results = await pipe.exec();

      const base = results[0][1] || {};
      const res = results[1][1] || {};
      const cd = results[2][1] || {};
      const destr = results[3][1];
      const legacy = results[4][1];
      const debris = results[5][1] || {};
      const archiDepRaw = results[6][1];

      if (!base.nick) continue; // битая запись

      out[id] = {
        id,
        nick: base.nick,
        password_hash: base.password_hash || null,
        island_level: parseInt(base.island_level) || 1,
        pos_x: parseFloat(base.pos_x) || randInt(300, MAP_W - 300),
        pos_y: parseFloat(base.pos_y) || randInt(300, MAP_H - 300),
        last_active: parseInt(base.last_active) || Date.now(),
        online: false,
        tavern_level: parseInt(base.tavern_level) || 1,
        dock_level: parseInt(base.dock_level) || 1,
        cannon_level: parseInt(base.cannon_level) || 0,
        // boats восстанавливаем до максимума: activeRaids/archi_raids хранятся
        // только в памяти и сбрасываются при рестарте — лодки «возвращаются» автоматически
        boats: boatCapacity(parseInt(base.dock_level) || 1),
        shield_until: parseInt(base.shield_until) || 0,
        created_at: parseInt(base.created_at) || Date.now(),
        color: base.color || `hsl(${randInt(0, 360)}, 60%, 50%)`,
        rum: parseFloat(res.rum) || 0,
        gold: parseFloat(res.gold) || 0,
        wood: parseFloat(res.wood) || 0,
        raid_cooldown: parseInt(cd.raid) || 0,
        pvp_cooldown: parseInt(cd.pvp) || 0,
        destruction_state: parseInt(destr) || 0,
        repair_progress: 0,
        legacy_bonus: parseFloat(legacy) || 0,
        debris_gold: parseFloat(debris.gold) || 0,
        debris_ttl: parseInt(debris.ttl) || 0,
        archi_raids: [],
        resource_raids: [],
        archi_depleted: (() => {
          try {
            const o = archiDepRaw ? JSON.parse(archiDepRaw) : {};
            if (typeof o !== 'object' || o === null) return {};
            const out = {};
            for (const k of Object.keys(o)) out[k] = parseInt(o[k], 10) || 0;
            return out;
          } catch (_) { return {}; }
        })()
      };
    } catch (e) {
      console.warn(`[REDIS] Failed to load player ${id}:`, e.message);
    }
  }

  return out;
}

// ── Ресурсные острова: загрузка/сохранение в Redis ───────
async function loadResourceIslandsFromRedis() {
  try {
    const raw = await redis.get(RESOURCE_ISLANDS_KEY);
    if (!raw) return null;
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr) || arr.length === 0) return null;
    // Валидация: каждый элемент должен иметь id, x, y, type, size, depleted_until
    const valid = arr.filter(
      i => typeof i.id === 'number' && typeof i.x === 'number' && typeof i.y === 'number' &&
          typeof i.type === 'string' && typeof i.size === 'number' && typeof i.depleted_until === 'number'
    );
    if (valid.length === 0) return null;
    console.log(`[REDIS] Loaded ${valid.length} resource islands from ${RESOURCE_ISLANDS_KEY}`);
    return valid;
  } catch (e) {
    console.warn('[REDIS] loadResourceIslands failed:', e.message);
    return null;
  }
}

async function saveResourceIslandsToRedis() {
  try {
    const payload = JSON.stringify(resourceIslands.map(i => ({
      id: i.id,
      x: i.x,
      y: i.y,
      type: i.type,
      size: i.size,
      depleted_until: i.depleted_until
    })));
    await redis.set(RESOURCE_ISLANDS_KEY, payload);
    console.log(`[REDIS] Saved ${resourceIslands.length} resource islands to ${RESOURCE_ISLANDS_KEY}`);
  } catch (e) {
    console.warn('[REDIS] saveResourceIslands failed:', e.message);
  }
}

// ── Создание игрока ──────────────────────────────────────
function createPlayer(nick, passwordHash = null) {
  const pos = findFreePlayerPosition();
  return {
    id: nick,
    nick,
    password_hash: passwordHash,
    island_level: 1,
    pos_x: pos.x,
    pos_y: pos.y,
    last_active: Date.now(),
    online: true,
    rum: 0,
    gold: 0,
    wood: 0,
    tavern_level: 1,
    dock_level: 1,
    cannon_level: 0,
    boats: BASE_BOAT_CAPACITY,
    shield_until: 0,
    destruction_state: 0,
    repair_progress: 0,
    legacy_bonus: 0,
    debris_gold: 0,
    debris_ttl: 0,
    raid_cooldown: 0,
    pvp_cooldown: 0,
    archi_raids: [],     // [{idx, startTime, duration, type}]
    archi_depleted: {},  // {idx: timestamp_until_replenished}
    resource_raids: [],  // [{islandId, startTime, duration}]
    created_at: Date.now(),
    color: `hsl(${randInt(0, 360)}, 60%, 50%)`
  };
}

// ── Оффлайн-прогресс ────────────────────────────────────
function calculateOfflineProgress(player) {
  const now = Date.now();
  const elapsed = Math.min(
    now - player.last_active,
    OFFLINE_MAX_HOURS * 3600 * 1000
  );
  // Меньше 1 минуты — нет прогресса
  if (elapsed < 60000) return { rum: 0, gold: 0, wood: 0, seconds: 0 };

  const seconds = elapsed / 1000;
  const onlineRum = rumRate(player);
  const offlineRum = Math.floor(onlineRum * OFFLINE_RATE);

  const rum = Math.floor(seconds * offlineRum);
  const wood = Math.floor(seconds * ARCHI_WOOD_PER_SEC * getArchipelagoCount(player.nick) * OFFLINE_RATE);
  return { rum, gold: 0, wood, seconds: Math.floor(seconds) };
}

// ── Публичное состояние ──────────────────────────────────
function getPlayerPublic(id) {
  const p = players[id];
  if (!p) return null;
  const myRaidsCount = activeRaids.filter(r => r.playerId === id).length;
  return {
    nick: p.nick,
    island_level: p.island_level,
    pos_x: p.pos_x,
    pos_y: p.pos_y,
    rum: Math.floor(p.rum),
    gold: Math.floor(p.gold),
    wood: Math.floor(p.wood),
    tavern_level: p.tavern_level,
    dock_level: p.dock_level,
    cannon_level: p.cannon_level,
    boats: p.boats,
    boats_max: boatCapacity(p.dock_level),
    shield_until: p.shield_until,
    destruction_state: p.destruction_state,
    legacy_bonus: p.legacy_bonus,
    debris_gold: Math.floor(p.debris_gold),
    online: p.online,
    color: p.color,
    pvp_cooldown: p.pvp_cooldown,
    active_raids: myRaidsCount,
    archi_raids: (p.archi_raids || []).map(r => ({
      idx: r.idx, startTime: r.startTime, duration: r.duration, type: r.type
    })),
    archi_depleted: p.archi_depleted || {},
    resource_raids: (p.resource_raids || []).map(r => ({
      islandId: r.islandId, startTime: r.startTime, duration: r.duration
    }))
  };
}

function getPvpMissionsPublic() {
  return pvpMissions.map(m => ({
    id: m.id,
    owner: m.owner,
    targetNick: m.targetNick,
    x: Math.round(m.x),
    y: Math.round(m.y),
    tx: Math.round(m.tx),
    ty: Math.round(m.ty),
    ownerColor: m.ownerColor
  }));
}

function getStatePayload() {
  const list = [];
  for (const id in players) {
    list.push(getPlayerPublic(id));
  }
  return { players: list, pvpMissions: getPvpMissionsPublic(), resourceIslands: getResourceIslandsPublic(), timestamp: Date.now() };
}

// ── Resolve PvP mission по прилёту ───────────────────────
function resolvePvpAttack(m) {
  const attacker = players[m.owner];
  const target = players[m.targetNick];

  // Возврат лодки атакующему
  if (attacker) {
    attacker.boats = Math.min(attacker.boats + 1, boatCapacity(attacker.dock_level));
  }

  if (!target) {
    if (attacker) io.to(m.owner).emit('attackResult', { ok: false, msg: 'Target not found' });
    return;
  }

  // Если цель получила щит пока лодка летела
  if (target.shield_until > Date.now()) {
    if (attacker) io.to(m.owner).emit('attackResult', { ok: false, msg: 'Target is shielded' });
    return;
  }

  const attackPower = (attacker ? attacker.boats * 10 + attacker.island_level * 5 : 10);
  const defensePower = target.cannon_level * 15 + target.island_level * 3;
  const defenseReduction = Math.min(0.5, defensePower / (attackPower + defensePower + 1));
  const stealPercent = PVP_STEAL_MIN + Math.random() * (PVP_STEAL_MAX - PVP_STEAL_MIN);
  const effectiveSteal = stealPercent * (1 - defenseReduction);

  const stolenRum  = Math.floor(target.rum  * effectiveSteal);
  const stolenGold = Math.floor(target.gold * effectiveSteal);
  const stolenWood = Math.floor(target.wood * effectiveSteal);
  const totalStolen = stolenRum + stolenGold + stolenWood;

  const debrisTotal    = Math.floor(totalStolen * DEBRIS_RATIO);
  const attackerDebris = Math.floor(debrisTotal * DEBRIS_ATTACKER_SHARE);
  const defenderDebris = Math.floor(debrisTotal * DEBRIS_DEFENDER_SHARE);

  if (attacker) {
    const safe = (totalStolen || 1);
    attacker.rum  += stolenRum  - Math.floor(stolenRum  * DEBRIS_RATIO) + Math.floor(attackerDebris * (stolenRum  / safe));
    attacker.gold += stolenGold - Math.floor(stolenGold * DEBRIS_RATIO) + Math.floor(attackerDebris * (stolenGold / safe));
    attacker.wood += stolenWood - Math.floor(stolenWood * DEBRIS_RATIO) + Math.floor(attackerDebris * (stolenWood / safe));
    persist(m.owner);
    io.to(m.owner).emit('attackResult', {
      ok: true,
      target: m.targetNick,
      stolen: { rum: stolenRum, gold: stolenGold, wood: stolenWood },
      totalStolen,
      debris: debrisTotal
    });
  }

  target.rum  = Math.max(0, target.rum  - stolenRum);
  target.gold = Math.max(0, target.gold - stolenGold);
  target.wood = Math.max(0, target.wood - stolenWood);
  target.debris_gold += defenderDebris;
  target.debris_ttl = Date.now() + 24 * 3600 * 1000;

  const shieldHours = SHIELD_MIN_HOURS + Math.random() * (SHIELD_MAX_HOURS - SHIELD_MIN_HOURS);
  target.shield_until = Date.now() + Math.floor(shieldHours * 3600 * 1000);
  target.destruction_state = Math.min(2, target.destruction_state + 1);

  if (target.destruction_state >= 2) {
    target.legacy_bonus += LEGACY_BONUS_PER_DESTROY * target.island_level;
  }

  persist(m.targetNick);

  io.to(m.targetNick).emit('attacked', {
    by: m.owner,
    lost: { rum: stolenRum, gold: stolenGold, wood: stolenWood },
    shieldUntil: target.shield_until,
    destructionState: target.destruction_state,
    debrisGold: defenderDebris
  });

  io.emit('chat', {
    from: 'PvP',
    text: `${m.owner} raided ${m.targetNick}! Stole ${totalStolen} resources`
  });

  console.log(`[PVP] Mission resolved: ${m.owner} → ${m.targetNick}, stole ${totalStolen}`);
}

// ── Economy tick (1 сек) ─────────────────────────────────
setInterval(() => {
  for (const id in players) {
    const p = players[id];
    if (!p.online) continue;

    // Пассивный доход рома
    p.rum += rumRate(p);
    // Пассивный доход дерева от архипелага
    p.wood += ARCHI_WOOD_PER_SEC * getArchipelagoCount(p.nick);
    p.last_active = Date.now();

    // Авто-ремонт (destruction_state > 0): прогресс за тик, при 1.0 — снимаем одну стадию
    if (p.destruction_state > 0) {
      const repairCost = AUTO_REPAIR_RUM_PER_LVL * p.island_level;
      if (p.rum >= repairCost) {
        p.rum -= repairCost;
        const progressGain = AUTO_REPAIR_PROGRESS_PER_SEC * p.island_level;
        p.repair_progress = (p.repair_progress || 0) + progressGain;
        if (p.repair_progress >= 1) {
          p.repair_progress = 0;
          p.destruction_state = Math.max(0, p.destruction_state - 1);
        }
      }
    }

    // Debris пассивная выдача для защитника
    if (p.debris_gold > 0 && p.debris_ttl > Date.now()) {
      // Выдаём 0.1% от debris в gold каждый тик
      const gain = Math.max(1, Math.floor(p.debris_gold * 0.001));
      p.gold += gain;
      p.debris_gold = Math.max(0, p.debris_gold - gain);
    } else if (p.debris_ttl > 0 && p.debris_ttl <= Date.now()) {
      // Debris истёк
      p.debris_gold = 0;
      p.debris_ttl = 0;
    }
  }
}, TICK_RATE);

// ── Raid check tick (5 сек) ──────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (let i = activeRaids.length - 1; i >= 0; i--) {
    const raid = activeRaids[i];
    if (now < raid.startTime + raid.duration) continue;

    // Рейд завершён
    const p = players[raid.playerId];
    if (!p) {
      activeRaids.splice(i, 1);
      continue;
    }

    if (Math.random() < BOAT_LOSS_CHANCE) {
      // Лодка потеряна
      console.log(`[RAID] ${raid.playerId}: boat lost at sea`);
      io.to(raid.playerId).emit('raidResult', {
        success: false,
        boatLost: true,
        loot: { rum: 0, gold: 0, wood: 0 }
      });
      io.to(raid.playerId).emit('chat', {
        from: 'Raid',
        text: `Your boat was lost at sea!`
      });
    } else {
      // Лодка вернулась с лутом
      const total = raid.lootRoll;
      const lootRum = Math.floor(total * 0.70);
      const lootWood = Math.floor(total * 0.20);
      const lootGold = Math.floor(total * 0.10);

      p.rum += lootRum;
      p.wood += lootWood;
      p.gold += lootGold;
      p.boats = Math.min(p.boats + 1, boatCapacity(p.dock_level)); // лодка возвращается

      console.log(`[RAID] ${raid.playerId}: returned, loot=${total} (rum=${lootRum}, wood=${lootWood}, gold=${lootGold})`);
      io.to(raid.playerId).emit('raidResult', {
        success: true,
        boatLost: false,
        loot: { rum: lootRum, gold: lootGold, wood: lootWood }
      });
      io.to(raid.playerId).emit('chat', {
        from: 'Raid',
        text: `Boat returned! +${lootRum} rum, +${lootWood} wood, +${lootGold} gold`
      });
    }

    activeRaids.splice(i, 1);
    persist(raid.playerId);
  }

  // ── Проверка archi-рейдов ──────────────────────────────
  const now2 = Date.now();
  for (const id in players) {
    const p = players[id];
    if (!p.archi_raids || p.archi_raids.length === 0) continue;
    for (let i = p.archi_raids.length - 1; i >= 0; i--) {
      const ar = p.archi_raids[i];
      if (now2 < ar.startTime + ar.duration) continue;

      // Завершился
      const lootDef = ARCHI_LOOT[ar.type] || ARCHI_LOOT.wood;
      const lootAmt = Math.floor(lootDef.min + Math.random() * (lootDef.max - lootDef.min));
      const boatLost = Math.random() < ARCHI_LOSS_CHANCE;

      if (!boatLost) {
        p.boats = Math.min(p.boats + 1, boatCapacity(p.dock_level));
      }
      p[lootDef.resource] = (p[lootDef.resource] || 0) + lootAmt;

      // Деплеция острова
      if (!p.archi_depleted) p.archi_depleted = {};
      p.archi_depleted[ar.idx] = now2 + ARCHI_DEPLETION;

      p.archi_raids.splice(i, 1);

      console.log(`[ARCHI] ${id}: island ${ar.idx} (${ar.type}) → +${lootAmt} ${lootDef.resource}, boatLost=${boatLost}`);
      io.to(id).emit('archiResult', {
        idx: ar.idx,
        type: ar.type,
        resource: lootDef.resource,
        amount: lootAmt,
        boatLost,
        depletedUntil: p.archi_depleted[ar.idx]
      });

      persist(id);
    }
  }

  // ── Проверка resource-рейдов ───────────────────────────
  const now3 = Date.now();
  for (const id in players) {
    const p = players[id];
    if (!p.resource_raids || p.resource_raids.length === 0) continue;
    for (let i = p.resource_raids.length - 1; i >= 0; i--) {
      const rr = p.resource_raids[i];
      if (now3 < rr.startTime + rr.duration) continue;

      const island = resourceIslands.find(isl => isl.id === rr.islandId);
      if (!island) { p.resource_raids.splice(i, 1); continue; }

      const lootDef = RESOURCE_LOOT[island.type] || RESOURCE_LOOT.wood;
      const lootAmt = Math.floor(lootDef.min + Math.random() * (lootDef.max - lootDef.min));
      const boatLost = Math.random() < RESOURCE_LOSS_CHANCE;

      if (!boatLost) {
        p.boats = Math.min(p.boats + 1, boatCapacity(p.dock_level));
      }
      p[lootDef.resource] = (p[lootDef.resource] || 0) + lootAmt;

      // Деплеция острова (общая для всех игроков)
      island.depleted_until = now3 + RESOURCE_DEPLETION;
      resourceIslandsDirty = true;

      p.resource_raids.splice(i, 1);

      console.log(`[RES] ${id}: island ${island.id} (${island.type}) → +${lootAmt} ${lootDef.resource}, boatLost=${boatLost}`);
      io.to(id).emit('resourceRaidResult', {
        islandId: island.id,
        type: island.type,
        resource: lootDef.resource,
        amount: lootAmt,
        boatLost,
        depletedUntil: island.depleted_until
      });

      persist(id);
    }
  }

  // Сохранение ресурсных островов в Redis при изменении depleted_until
  if (resourceIslandsDirty) {
    resourceIslandsDirty = false;
    saveResourceIslandsToRedis().catch(err => console.warn('[REDIS] resourceIslands save error:', err.message));
  }
}, RAID_CHECK_TICK);

// ── PvP missions tick (100 мс) ───────────────────────────
setInterval(() => {
  if (pvpMissions.length === 0) return;
  const dt = PVP_MISSION_TICK / 1000;
  for (let i = pvpMissions.length - 1; i >= 0; i--) {
    const m = pvpMissions[i];
    const dx = m.tx - m.x;
    const dy = m.ty - m.y;
    const d = Math.sqrt(dx * dx + dy * dy);
    if (d < m.speed * dt + 10) {
      resolvePvpAttack(m);
      pvpMissions.splice(i, 1);
    } else {
      m.x += (dx / d) * m.speed * dt;
      m.y += (dy / d) * m.speed * dt;
    }
  }
  io.emit('pvpMissions', getPvpMissionsPublic());
}, PVP_MISSION_TICK);

// ── State broadcast (2 сек) ──────────────────────────────
setInterval(() => {
  io.emit('state', getStatePayload());

  // Рассылка активных рейдов каждому игроку
  const raidsByPlayer = {};
  for (const r of activeRaids) {
    if (!raidsByPlayer[r.playerId]) raidsByPlayer[r.playerId] = [];
    raidsByPlayer[r.playerId].push({
      startTime: r.startTime,
      duration: r.duration,
      remaining: Math.max(0, r.startTime + r.duration - Date.now())
    });
  }
  for (const id in raidsByPlayer) {
    io.to(id).emit('raidsUpdate', raidsByPlayer[id]);
  }
  // Игрокам без рейдов — пустой массив
  for (const id in players) {
    if (!raidsByPlayer[id] && players[id].online) {
      io.to(id).emit('raidsUpdate', []);
    }
  }
}, STATE_BROADCAST_TICK);

// ── Autosave (20 сек) ───────────────────────────────────
setInterval(() => {
  for (const id in players) {
    persist(id);
  }
  // console.log(`[AUTOSAVE] Saved ${Object.keys(players).length} players`);
}, AUTOSAVE_TICK);

// ── Socket.io ────────────────────────────────────────────
io.on('connection', (socket) => {
  let currentNick = null;

  function setNick(nick) {
    currentNick = nick;
    socket._currentNick = nick;
  }

  // ── Регистрация / вход ──────────────────────────────────
  socket.on('join', (data, callback) => {
    if (typeof callback !== 'function') return;
    const nick = typeof data?.nick === 'string' ? data.nick.trim() : '';
    const password = typeof data?.password === 'string' ? data.password : '';

    if (nick.length < NICK_MIN || nick.length > NICK_MAX) {
      return callback({ ok: false, msg: `Nick: ${NICK_MIN}-${NICK_MAX} characters` });
    }

    if (players[nick]) {
      // Существующий игрок
      const p = players[nick];

      // Проверка пароля
      if (p.password_hash && p.password_hash !== '') {
        if (!password) {
          return callback({ ok: false, msg: 'Password required for this nick' });
        }
        if (hashPassword(password) !== p.password_hash) {
          return callback({ ok: false, msg: 'Wrong password' });
        }
      }

      // Оффлайн-прогресс
      const offlineGains = calculateOfflineProgress(p);
      p.rum += offlineGains.rum;
      p.gold += offlineGains.gold;
      p.wood += offlineGains.wood;

      p.online = true;
      p.last_active = Date.now();
      setNick(nick);
      socket.join(nick); // Для адресных уведомлений

      persist(nick);

      console.log(`[AUTH] ${nick} logged in (restored), offline gains: +${offlineGains.rum} rum (${offlineGains.seconds}s away)`);
      callback({
        ok: true,
        restored: true,
        player: getPlayerPublic(nick),
        offlineGains
      });
    } else {
      // Новый игрок
      const pw_hash = password ? hashPassword(password) : null;
      players[nick] = createPlayer(nick, pw_hash);
      setNick(nick);
      socket.join(nick);

      persist(nick);

      console.log(`[AUTH] ${nick} registered (new player, password: ${pw_hash ? 'yes' : 'no'})`);
      callback({
        ok: true,
        restored: false,
        player: getPlayerPublic(nick),
        offlineGains: { rum: 0, gold: 0, wood: 0, seconds: 0 }
      });
    }

    // Отправить текущее состояние
    socket.emit('state', getStatePayload());

    // Уведомить всех
    io.emit('chat', { from: 'System', text: `${nick} joined the seas` });
  });

  // ── Апгрейд таверны ────────────────────────────────────
  socket.on('upgradeTavern', (callback) => {
    if (!currentNick || !players[currentNick]) return callback?.({ ok: false });
    const p = players[currentNick];
    const cost = tavernCost(p.tavern_level);
    if (p.rum < cost) {
      return callback({ ok: false, msg: `Need ${cost} rum (have ${Math.floor(p.rum)})` });
    }
    p.rum -= cost;
    p.tavern_level += 1;
    persist(currentNick);
    console.log(`[UPGRADE] ${currentNick}: tavern → ${p.tavern_level} (cost ${cost} rum)`);
    callback({ ok: true, tavern_level: p.tavern_level, rum: Math.floor(p.rum) });
  });

  // ── Апгрейд дока ──────────────────────────────────────
  socket.on('upgradeDock', (callback) => {
    if (!currentNick || !players[currentNick]) return callback?.({ ok: false });
    const p = players[currentNick];
    const cost = dockCost(p.dock_level);
    if (p.wood < cost) {
      return callback({ ok: false, msg: `Need ${cost} wood (have ${Math.floor(p.wood)})` });
    }
    p.wood -= cost;
    p.dock_level += 1;
    const newCap = boatCapacity(p.dock_level);
    p.boats = newCap; // при апгрейде даём полный набор лодок до нового лимита
    persist(currentNick);
    console.log(`[UPGRADE] ${currentNick}: dock → ${p.dock_level} (cost ${cost} wood), boats ${p.boats}/${newCap}`);
    callback({ ok: true, dock_level: p.dock_level, wood: Math.floor(p.wood), boats: p.boats, boats_max: newCap });
  });

  // ── Апгрейд пушек ─────────────────────────────────────
  socket.on('upgradeCannons', (callback) => {
    if (!currentNick || !players[currentNick]) return callback?.({ ok: false });
    const p = players[currentNick];
    const costRum = cannonCostRum(p.cannon_level);
    const costWood = cannonCostWood(p.cannon_level);
    if (p.rum < costRum || p.wood < costWood) {
      return callback({ ok: false, msg: `Need ${costRum} rum + ${costWood} wood` });
    }
    p.rum -= costRum;
    p.wood -= costWood;
    p.cannon_level += 1;
    persist(currentNick);
    console.log(`[UPGRADE] ${currentNick}: cannons → ${p.cannon_level}`);
    callback({ ok: true, cannon_level: p.cannon_level, rum: Math.floor(p.rum), wood: Math.floor(p.wood) });
  });

  // ── Апгрейд острова ────────────────────────────────────
  socket.on('upgradeIsland', (callback) => {
    if (!currentNick || !players[currentNick]) return callback?.({ ok: false });
    const p = players[currentNick];
    // Стоимость: 1000 * 2^(level-1) rum + 500 * 2^(level-1) wood
    const costRum = Math.floor(1000 * Math.pow(2, p.island_level - 1));
    const costWood = Math.floor(500 * Math.pow(2, p.island_level - 1));
    if (p.rum < costRum || p.wood < costWood) {
      return callback({ ok: false, msg: `Need ${costRum} rum + ${costWood} wood` });
    }
    p.rum -= costRum;
    p.wood -= costWood;
    p.island_level += 1;
    persist(currentNick);
    console.log(`[UPGRADE] ${currentNick}: island → ${p.island_level}`);
    callback({ ok: true, island_level: p.island_level });
  });

  // ── Отправка рейда ─────────────────────────────────────
  socket.on('sendRaid', (callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];

    if (p.boats <= 0) {
      return callback({ ok: false, msg: 'No boats available' });
    }

    p.boats -= 1;
    const duration = RAID_MIN_TIME + Math.random() * (RAID_MAX_TIME - RAID_MIN_TIME);
    const lootRoll = RAID_LOOT_MIN + Math.random() * (RAID_LOOT_MAX - RAID_LOOT_MIN);

    activeRaids.push({
      playerId: currentNick,
      startTime: Date.now(),
      duration: Math.floor(duration),
      lootRoll: Math.floor(lootRoll)
    });

    persist(currentNick);
    console.log(`[RAID] ${currentNick}: sent boat, duration=${Math.round(duration / 60000)}min, potential loot=${Math.floor(lootRoll)}`);
    callback({
      ok: true,
      duration: Math.floor(duration),
      boatsRemaining: p.boats,
      boats_max: boatCapacity(p.dock_level)
    });
  });

  // ── Сбор ресурсов с острова архипелага ────────────────
  socket.on('harvestArchi', (data, callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];

    const idx = parseInt(data?.idx);
    if (isNaN(idx) || idx < 0 || idx >= getArchipelagoCount(currentNick)) {
      return callback({ ok: false, msg: 'Invalid island index' });
    }

    // Проверка: остров не в деплеции
    const depletedUntil = (p.archi_depleted || {})[idx] || 0;
    if (depletedUntil > Date.now()) {
      const secsLeft = Math.ceil((depletedUntil - Date.now()) / 1000);
      return callback({ ok: false, msg: `Island depleted (${secsLeft}s)` });
    }

    // Проверка: остров уже не собирается
    if (!p.archi_raids) p.archi_raids = [];
    if (p.archi_raids.some(r => r.idx === idx)) {
      return callback({ ok: false, msg: 'Already harvesting this island' });
    }

    // Нужна лодка
    if (p.boats <= 0) {
      return callback({ ok: false, msg: 'No boats available' });
    }

    // Определяем тип острова (зеркало клиента)
    const ARCHI_TYPES_SRV = ['wood', 'wood', 'stone', 'rum'];
    const s3 = archSeed(currentNick, idx * 3 + 3);
    const type = ARCHI_TYPES_SRV[s3 % ARCHI_TYPES_SRV.length];

    p.boats -= 1;
    const pos = getArchipelagoPosition(currentNick, idx);
    const distance = Math.sqrt(pos.dx * pos.dx + pos.dy * pos.dy);
    const duration = Math.max(ARCHI_HARVEST_MIN_MS, Math.floor((distance / BOAT_SPEED) * 1000));
    p.archi_raids.push({ idx, startTime: Date.now(), duration, type });

    console.log(`[ARCHI] ${currentNick}: harvesting island ${idx} (${type}), dist=${Math.round(distance)}, eta=${Math.round(duration/1000)}s`);
    callback({ ok: true, duration, idx, type });
  });

  // ── Сбор ресурсов с острова мировой карты ─────────────
  socket.on('harvestResourceIsland', (data, callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];

    const islandId = parseInt(data?.islandId);
    const island = resourceIslands.find(i => i.id === islandId);
    if (!island) return callback({ ok: false, msg: 'Island not found' });

    // Проверка: остров не в деплеции
    if (island.depleted_until > Date.now()) {
      const secsLeft = Math.ceil((island.depleted_until - Date.now()) / 1000);
      return callback({ ok: false, msg: `Island depleted (${secsLeft}s)` });
    }

    // Проверка: игрок уже не отправил лодку на этот остров
    if (!p.resource_raids) p.resource_raids = [];
    if (p.resource_raids.some(r => r.islandId === islandId)) {
      return callback({ ok: false, msg: 'Already harvesting this island' });
    }

    // Нужна лодка
    if (p.boats <= 0) {
      return callback({ ok: false, msg: 'No boats available' });
    }

    p.boats -= 1;
    const dx = island.x - p.pos_x;
    const dy = island.y - p.pos_y;
    const distance = Math.sqrt(dx * dx + dy * dy);
    const duration = Math.max(RESOURCE_HARVEST_MIN_MS, Math.floor((distance / BOAT_SPEED) * 1000));
    p.resource_raids.push({ islandId, startTime: Date.now(), duration });

    console.log(`[RES] ${currentNick}: harvesting island ${islandId} (${island.type}), dist=${Math.round(distance)}, eta=${Math.round(duration/1000)}s`);
    callback({ ok: true, duration, islandId, type: island.type });
  });

  // ── PvP атака — создаёт летящую миссию ────────────────
  socket.on('attackPlayer', (data, callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });

    const targetNick = typeof data?.targetNick === 'string' ? data.targetNick.trim() : '';
    const target = players[targetNick];
    const attacker = players[currentNick];

    if (!target) return callback({ ok: false, msg: 'Player not found' });
    if (targetNick === currentNick) return callback({ ok: false, msg: 'Cannot attack yourself' });

    // Щит — проверяем на старте
    if (target.shield_until > Date.now()) {
      const remainH = Math.ceil((target.shield_until - Date.now()) / 3600000);
      return callback({ ok: false, msg: `Target is shielded (${remainH}h left)` });
    }

    // Кулдаун
    if (attacker.pvp_cooldown > Date.now()) {
      const remainM = Math.ceil((attacker.pvp_cooldown - Date.now()) / 60000);
      return callback({ ok: false, msg: `PvP cooldown: ${remainM} min` });
    }

    // Нужна хотя бы 1 лодка
    if (attacker.boats <= 0) {
      return callback({ ok: false, msg: 'Need at least 1 boat to attack' });
    }

    // Запускаем летящую миссию
    const dx = target.pos_x - attacker.pos_x;
    const dy = target.pos_y - attacker.pos_y;
    const dist = Math.sqrt(dx * dx + dy * dy);
    const eta = Math.ceil(dist / PVP_SPEED);

    const missionId = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    pvpMissions.push({
      id: missionId,
      owner: currentNick,
      targetNick,
      x: attacker.pos_x,
      y: attacker.pos_y,
      tx: target.pos_x,
      ty: target.pos_y,
      speed: PVP_SPEED,
      ownerColor: attacker.color
    });

    // Расходуем лодку и ставим кулдаун сразу
    attacker.boats = Math.max(0, attacker.boats - 1);
    attacker.pvp_cooldown = Date.now() + PVP_COOLDOWN;
    persist(currentNick);

    console.log(`[PVP] ${currentNick} → ${targetNick}: mission launched (dist=${Math.round(dist)}, ETA=${eta}s)`);

    callback({ ok: true, launched: true, eta, targetNick });

    io.emit('chat', {
      from: 'PvP',
      text: `${currentNick} launched attack on ${targetNick}! (ETA ~${eta}s)`
    });
    io.emit('state', getStatePayload());
  });

  // ── Сбор debris ────────────────────────────────────────
  socket.on('collectDebris', (callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false });
    const p = players[currentNick];

    if (p.debris_gold <= 0) return callback({ ok: false, msg: 'No debris to collect' });

    const collected = Math.floor(p.debris_gold);
    p.gold += collected;
    p.debris_gold = 0;
    p.debris_ttl = 0;
    persist(currentNick);

    console.log(`[DEBRIS] ${currentNick} collected ${collected} gold from debris`);
    callback({ ok: true, collected, gold: Math.floor(p.gold) });
  });

  // ── Отключение ─────────────────────────────────────────
  socket.on('disconnect', () => {
    if (currentNick && players[currentNick]) {
      players[currentNick].online = false;
      players[currentNick].last_active = Date.now();
      persist(currentNick);
      console.log(`[AUTH] ${currentNick} disconnected`);
    }
  });
});

// ── Запуск ───────────────────────────────────────────────
async function start() {
  try {
    const loaded = await loadPlayersFromRedis();
    Object.assign(players, loaded);
    console.log(`[SYSTEM] Loaded ${Object.keys(loaded).length} players from Redis`);
  } catch (e) {
    console.warn('[SYSTEM] Redis load failed, starting empty:', e.message);
  }

  // Ресурсные острова: из Redis или генерация с сохранением
  const loadedIslands = await loadResourceIslandsFromRedis();
  if (loadedIslands && loadedIslands.length > 0) {
    resourceIslands.length = 0;
    resourceIslands.push(...loadedIslands);
    console.log(`[WORLD] Using ${resourceIslands.length} resource islands from Redis`);
  } else {
    generateResourceIslands();
    await saveResourceIslandsToRedis();
  }

  const PORT = process.env.PORT || 3000;
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[SYSTEM] PirateIsles v0.1 running on http://localhost:${PORT}`);
  });
}
start();
