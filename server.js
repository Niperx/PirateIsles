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
const BOAT_BUILD_COST = 80;         // Стоимость постройки одной лодки (дерево)

// Рейды (AI-лодки)
const RAID_MIN_TIME = 1 * 60 * 1000;   // 1 мин (тест; боевое: 8 мин)
const RAID_MAX_TIME = 1 * 60 * 1000;   // 1 мин (тест; боевое: 20 мин)
const RAID_LOOT_MIN = 150;
const RAID_LOOT_MAX = 1200;
const BOAT_LOSS_CHANCE = 0.12;          // 12% базовый шанс потери

// PvP v2 — wipe, щиты, пассивки
const PVP_STEAL_BASE_MIN = 0.45;
const PVP_STEAL_BASE_MAX = 0.65;
const PVP_STEAL_PER_WIN_PCT = 2;           // +2% за каждую PvP-победу (passives)
const PVP_BOATS_REQUIRED = 2;              // лодок на одну атаку (риск 20–30% от пушек жертвы)
const PVP_CANNON_LOSS_CHANCE_MIN = 0.20;  // мин шанс потери лодки от пушек
const PVP_CANNON_LOSS_CHANCE_MAX = 0.30;
const PVP_ATTACK_COOLDOWN_MS = 90 * 1000;  // 1–2 мин между атаками на одного игрока
const PVP_ATTACKS_PER_TARGET_PER_24H = 5;
const PVP_FLEET_FATIGUE_AFTER = 3;         // после 3 атак подряд — кулдаун
const PVP_FLEET_FATIGUE_COOLDOWN_MS = 60 * 60 * 1000; // 1 час
const WIPE_ATTACKS_MIN = 3;
const WIPE_ATTACKS_MAX = 5;                // случайное порог 3–5 для каждого острова
const WIPE_LEVEL_DROP_MIN = 1;
const WIPE_LEVEL_DROP_MAX = 2;             // за атаку -1 или -2 уровня
const ISLAND_LEVEL_MAX = 15;
const SHIELD_NEWBIE_HOURS = 0;     // 0 = новичковая защита по времени отключена (для тестирования)
const SHIELD_NEWBIE_MAX_LEVEL = 0; // 0 = новичковая защита по уровню отключена (для тестирования)
const SHIELD_POST_ATTACK_HOURS = 0;  // 0 = щит после атаки отключён (для тестирования)
const SHIELD_POST_ATTACK_BREACH_CHANCE = 0.50; // усиленный рейд пробивает с 50%
const SHIELD_BUY_COST_PCT = 0.10;          // 10% текущих ресурсов
const SHIELD_BUY_DURATION_MS = 60 * 60 * 1000; // 1 час
const SHIELD_BUY_COOLDOWN_MS = 6 * 60 * 60 * 1000; // 6 часов
const ENHANCED_RAID_BOATS = 4;             // ×2 от обычных 2 — усиленный рейд
const DEBRIS_RATIO = 0.40;
const DEBRIS_ATTACKER_SHARE = 0.70;
const DEBRIS_DEFENDER_SHARE = 0.30;
const PVP_COOLDOWN_MS = 2 * 60 * 1000;    // 2 мин после атаки (на любую цель)

// Активная оборона
const ACTIVE_DEFENSE_TIMEOUT_MS = 25 * 1000;       // 25 сек окно выбора
const ACTIVE_DEFENSE_CANNON_RUM_PCT = 0.12;         // 12% рома за залп пушек
const ACTIVE_DEFENSE_CANNON_DEF_MULT = 2.5;         // ×2.5 к defense power
const ACTIVE_DEFENSE_SHIELD_GOLD_COST = 50;         // фиксированная стоимость в золоте
const ACTIVE_DEFENSE_SHIELD_STEAL_REDUCE = 0.20;    // -20% к effectiveSteal (щит)
const ACTIVE_DEFENSE_MERC_RUM_COST = 100;           // ром за наёмников
const ACTIVE_DEFENSE_MERC_DEF_BONUS = 25;           // +25 к defensePower от наёмников

// Оффлайн
const OFFLINE_RATE = 0.40;              // 40% от онлайн-скорости
const OFFLINE_MAX_HOURS = 12;

// Авто-ремонт
const AUTO_REPAIR_PER_HOUR = 0.015;     // 1.5% уровня острова в час
const AUTO_REPAIR_RUM_PER_LVL = 10;     // стоимость ремонта в rum за тик за уровень
const AUTO_REPAIR_PROGRESS_PER_SEC = AUTO_REPAIR_PER_HOUR / 3600; // доля прогресса за 1 сек на 1 уровень

// Пассивки (наследие) — переносятся после wipe
const PASSIVE_RUM_PERCENT_PER_TAVERN_UPGRADE = 0.5;   // +0.5% рома за апгрейд таверны
const PASSIVE_RAID_SPEED_PER_10_RAIDS = 1;           // +1% скорости рейдов за каждые 10 успешных
const PASSIVE_PVP_STEAL_PER_WIN = 2;                 // +2% кражи за PvP-победу
const WIPE_BOOST_MIN = 5;                             // +5–10% буст после wipe за прошлую жизнь
const WIPE_BOOST_MAX = 10;

// Auth
const NICK_MIN = 3;
const NICK_MAX = 16;

// World map
const MAP_W = 3000;
const MAP_H = 2000;
// Минимальная дистанция между стартовыми островами игроков на карте
const PLAYER_MIN_DISTANCE = 450;
const BOAT_SPEED = 58;         // world-units/sec — скорость до острова (архипелаг, ресурсные), естественный темп
const PVP_SPEED = 34;          // world-units/sec — рейд медленнее, чем поход за ресурсами (PVP_SPEED < BOAT_SPEED)
const PVP_MISSION_TICK = 100;   // ms — тик движения PvP-лодок

// Архипелаг (острова-спутники у базы)
const ARCHI_WOOD_PER_SEC   = 0.4;            // дерево/сек пассивно с каждого островка
const ARCHI_HARVEST_MIN_MS = 5000;           // минимум 5 сек в пути (даже для близких островов)
const ARCHI_DEPLETION      = 15 * 60 * 1000; // 15 мин до восстановления острова
const ARCHI_LOSS_CHANCE    = 0.05;           // 5% шанс потери лодки
// Лут по типу: [min, max]
const ARCHI_LOOT = {
  wood:  { resource: 'wood',  min: 60,  max: 180 },
  gold:  { resource: 'gold',  min: 30,  max: 100 },
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

// ── Точки захвата (PvP конкуренция) ─────────────────────
const CAPTURE_POINT_COUNT   = 8;
const CAPTURE_BASE_TIME_MS  = 3 * 60 * 1000;  // 3 мин за 1 лодку; 1 лодка = полный таймер
const CAPTURE_RESPAWN_MS    = 8 * 60 * 1000;  // 8 мин до следующего лута
const CAPTURE_MAX_BOATS     = 3;              // макс лодок на захват
const CAPTURE_LOOT = {
  gold: { min: 120, max: 320 },
  rum:  { min:  80, max: 220 },
};
const INTERCEPT_LOSS_PCT_MIN = 0.40;
const INTERCEPT_LOSS_PCT_MAX = 0.60;

// ── Имперские караваны (движущиеся NPC) ───────────────────
const CARAVAN_COUNT = 4;
const CARAVAN_TICK_MS = 10000;                    // 10 сек — тик движения
const CARAVAN_SPEED = 12;                          // world-units за тик
const CARAVAN_INTERCEPT_MIN_MS = 2 * 60 * 1000;    // 2 мин минимум в пути
const CARAVAN_INTERCEPT_MAX_MS = 5 * 60 * 1000;    // 5 мин максимум
const CARAVAN_NPC_BOATS = 3;                       // сила эскорта каравана (Fleet vs NPC)
const CARAVAN_LOOT = { gold: { min: 180, max: 450 }, rum: { min: 120, max: 320 } };
const CARAVAN_LOSS_CHANCE = 0.15;                  // 15% шанс потери лодки при победе; при поражении — 60–80%

const players = {};       // ключ — nick
const activeRaids = [];   // { playerId, startTime, duration, lootRoll }
const pvpMissions = [];   // { id, owner, targetNick, x, y, tx, ty, speed, ownerColor }
const resourceIslands = []; // { id, x, y, type, size, depleted_until }
let resourceIslandsDirty = false; // флаг: нужно ли сохранить острова в Redis после деплеции
// capturePoints: { id, x, y, capturer, capturerBoats, captureEta, respawnAt }
const capturePoints = [];
// caravans: { id, x, y, routeIndex, route: [{x,y},...] } — движутся по маршруту
const caravans = [];
// caravanIntercepts: { id, owner, caravanId, boats, startTime, duration }
const caravanIntercepts = [];

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

// Сколько лодок сейчас в миссиях (PvE, архипелаг, ресурсы, PvP, захват, караваны)
function boatsInMissions(nick) {
  const p = players[nick];
  if (!p) return 0;
  const pve    = activeRaids.filter(r => r.playerId === nick).length;
  const archi  = (p.archi_raids    || []).length;
  const res    = (p.resource_raids || []).length;
  const pvp    = pvpMissions.filter(m => m.owner === nick).reduce((s, m) => s + (m.boatsUsed || 1), 0);
  const cap_pt = capturePoints.filter(cp => cp.capturer === nick).reduce((s, cp) => s + (cp.capturerBoats || 0), 0);
  const car    = caravanIntercepts.filter(ci => ci.owner === nick).reduce((s, ci) => s + ci.boats, 0);
  return pve + archi + res + pvp + cap_pt + car;
}

function generateCapturePoints() {
  const types = ['gold', 'rum', 'gold', 'rum', 'gold', 'rum', 'gold', 'rum'];
  const margin = 200;
  for (let i = 0; i < CAPTURE_POINT_COUNT; i++) {
    capturePoints.push({
      id: i,
      x: randInt(margin, MAP_W - margin),
      y: randInt(margin, MAP_H - margin),
      type: types[i % types.length],
      capturer: null,
      capturerBoats: 0,
      captureEta: null,
      respawnAt: 0     // 0 = доступно сейчас
    });
  }
  console.log(`[WORLD] Generated ${capturePoints.length} capture points`);
}

function getCapturePointsPublic() {
  return capturePoints.map(cp => ({
    id: cp.id, x: cp.x, y: cp.y, type: cp.type,
    capturer: cp.capturer,
    capturerBoats: cp.capturerBoats,
    captureEta: cp.captureEta,
    respawnAt: cp.respawnAt
  }));
}

// Генерация караванов: маршрут — прямоугольник по карте
function generateCaravans() {
  const margin = 250;
  for (let i = 0; i < CARAVAN_COUNT; i++) {
    const w = margin + (i % 3) * (MAP_W - 2 * margin) / 3;
    const h = margin + (Math.floor(i / 2) % 2) * (MAP_H - 2 * margin) / 2;
    const route = [
      { x: w, y: h },
      { x: Math.min(MAP_W - margin, w + 400), y: h },
      { x: Math.min(MAP_W - margin, w + 400), y: Math.min(MAP_H - margin, h + 350) },
      { x: w, y: Math.min(MAP_H - margin, h + 350) }
    ];
    caravans.push({
      id: i,
      x: route[0].x,
      y: route[0].y,
      routeIndex: 0,
      route
    });
  }
  console.log(`[WORLD] Generated ${caravans.length} imperial caravans`);
}

function getCaravansPublic() {
  return caravans.map(c => ({ id: c.id, x: Math.round(c.x), y: Math.round(c.y) }));
}

// Fleet vs NPC (караван): игрок vs фиксированная сила эскорта
function resolveCaravanBattle(playerBoats, npcBoats) {
  const pPower = playerBoats * (0.7 + Math.random() * 0.6);
  const nPower = npcBoats * (0.7 + Math.random() * 0.6);
  if (pPower >= nPower) {
    const playerLost = Math.min(playerBoats - 1, Math.round(playerBoats * (1 - pPower / (pPower + nPower)) * 0.9));
    return { winner: 'player', playerLost: Math.max(0, playerLost) };
  } else {
    const lossPct = 0.60 + Math.random() * 0.25;
    const playerLost = Math.min(playerBoats, Math.max(1, Math.round(playerBoats * lossPct)));
    return { winner: 'npc', playerLost };
  }
}

// Fleet vs Fleet симуляция (для захватов/перехватов)
function resolveFleetBattle(aBoats, bBoats) {
  // Возвращает { winner: 'a'|'b', aLost, bLost }
  const aPower = aBoats * (0.7 + Math.random() * 0.6);
  const bPower = bBoats * (0.7 + Math.random() * 0.6);
  if (aPower >= bPower) {
    const bLost = Math.min(bBoats, Math.round(bBoats * (INTERCEPT_LOSS_PCT_MIN + Math.random() * (INTERCEPT_LOSS_PCT_MAX - INTERCEPT_LOSS_PCT_MIN))));
    const aLost = Math.min(aBoats - 1, Math.round(aBoats * (1 - aPower / (aPower + bPower)) * 0.8));
    return { winner: 'a', aLost: Math.max(0, aLost), bLost };
  } else {
    const aLost = Math.min(aBoats, Math.round(aBoats * (INTERCEPT_LOSS_PCT_MIN + Math.random() * (INTERCEPT_LOSS_PCT_MAX - INTERCEPT_LOSS_PCT_MIN))));
    const bLost = Math.min(bBoats - 1, Math.round(bBoats * (1 - bPower / (aPower + bPower)) * 0.8));
    return { winner: 'b', aLost, bLost: Math.max(0, bLost) };
  }
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
  const passives = player.passives || defaultPassives();
  const passiveMult = 1 + (passives.rum_bonus + passives.wipe_boost) / 100;
  const legacyMult = 1 + (player.legacy_bonus || 0);
  return Math.floor(base * passiveMult * legacyMult);
}

// Щит новичка: первые 2 ч ИЛИ до lvl 5 ИЛИ до первой атаки на другого (что раньше)
function isNewbieShield(player) {
  if (!player) return false;
  if (SHIELD_NEWBIE_HOURS === 0 && SHIELD_NEWBIE_MAX_LEVEL === 0) return false;
  const twoHours = SHIELD_NEWBIE_HOURS * 3600 * 1000;
  if (Date.now() - player.created_at < twoHours) return true;
  if ((player.island_level || 0) < SHIELD_NEWBIE_MAX_LEVEL) return true;
  return false;
}

// Цель под щитом (новичок — полный; остальные — обычный shield_until, усиленный рейд может пробить)
function isTargetShielded(target, enhancedRaid) {
  if (!target) return true;
  if (isNewbieShield(target)) return true;
  if (SHIELD_POST_ATTACK_HOURS === 0) return false;
  if (target.shield_until <= Date.now()) return false;
  if (enhancedRaid && Math.random() < SHIELD_POST_ATTACK_BREACH_CHANCE) return false;
  return true;
}

// ── Redis persistence (multi-key) ────────────────────────
const PLAYERS_SET_KEY = 'players:nicks';
const RESOURCE_ISLANDS_KEY = 'resourceIslands';

function pKey(id) { return `player:${id}`; }
function pResKey(id) { return `player:${id}:resources`; }
function pCdKey(id) { return `player:${id}:cooldowns`; }
function pDestrKey(id) { return `player:${id}:destruction_state`; }
function pLegacyKey(id) { return `player:${id}:legacy_bonus`; }
function pPassivesKey(id) { return `player:${id}:passives`; }
function pPvpAttacksKey(id) { return `player:${id}:pvp_attacks`; }
function pWipeStateKey(id) { return `player:${id}:wipe_state`; }
function pDebrisKey(id) { return `player:${id}:debris`; }
function pArchiDepletedKey(id) { return `player:${id}:archi_depleted`; }

function defaultPassives() {
  return { rum_bonus: 0, raid_speed: 0, pvp_steal: 0, wipe_boost: 0, successful_raids: 0, pvp_wins: 0 };
}

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
    shield_cooldown_until: String(p.shield_cooldown_until || 0),
    has_attacked_anyone: p.has_attacked_anyone ? '1' : '0',
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
  pipe.set(pPassivesKey(id), JSON.stringify(p.passives || defaultPassives()));
  const pvpAttacks = p.pvp_attacks || {};
  const dayAgo = Date.now() - 24 * 3600 * 1000;
  const filtered = {};
  for (const k of Object.keys(pvpAttacks)) {
    const v = pvpAttacks[k];
    if (v && typeof v.t === 'number' && v.t > dayAgo) filtered[k] = v;
  }
  pipe.set(pPvpAttacksKey(id), JSON.stringify(filtered));
  const ws = p.wipe_state || { threshold: WIPE_ATTACKS_MIN, count: 0 };
  pipe.set(pWipeStateKey(id), JSON.stringify({ threshold: ws.threshold, count: ws.count }));

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
  pipe.del(pPassivesKey(id));
  pipe.del(pPvpAttacksKey(id));
  pipe.del(pWipeStateKey(id));
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
      pipe.get(pPassivesKey(id));      // 7
      pipe.get(pPvpAttacksKey(id));    // 8
      pipe.get(pWipeStateKey(id));     // 9
      const results = await pipe.exec();

      const base = results[0][1] || {};
      const res = results[1][1] || {};
      const cd = results[2][1] || {};
      const destr = results[3][1];
      const legacy = results[4][1];
      const debris = results[5][1] || {};
      const archiDepRaw = results[6][1];
      const passivesRaw = results[7][1];
      const pvpAttacksRaw = results[8][1];

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
        shield_cooldown_until: parseInt(base.shield_cooldown_until) || 0,
        has_attacked_anyone: base.has_attacked_anyone === '1',
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
        })(),
        passives: (() => {
          try {
            const o = passivesRaw ? JSON.parse(passivesRaw) : null;
            if (typeof o !== 'object' || o === null) return defaultPassives();
            return { ...defaultPassives(), ...o };
          } catch (_) { return defaultPassives(); }
        })(),
        pvp_attacks: (() => {
          try {
            const o = pvpAttacksRaw ? JSON.parse(pvpAttacksRaw) : null;
            return typeof o === 'object' && o !== null ? o : {};
          } catch (_) { return {}; }
        })(),
        wipe_state: (() => {
          try {
            const raw = results[9] && results[9][1];
            const o = raw ? JSON.parse(raw) : null;
            if (typeof o === 'object' && o !== null && typeof o.threshold === 'number' && typeof o.count === 'number') return o;
            return { threshold: WIPE_ATTACKS_MIN + randInt(0, WIPE_ATTACKS_MAX - WIPE_ATTACKS_MIN), count: 0 };
          } catch (_) { return { threshold: WIPE_ATTACKS_MIN + randInt(0, WIPE_ATTACKS_MAX - WIPE_ATTACKS_MIN), count: 0 }; }
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
    shield_cooldown_until: 0,
    has_attacked_anyone: false,
    destruction_state: 0,
    repair_progress: 0,
    legacy_bonus: 0,
    passives: defaultPassives(),
    pvp_attacks: {},
    wipe_state: { threshold: WIPE_ATTACKS_MIN + randInt(0, WIPE_ATTACKS_MAX - WIPE_ATTACKS_MIN), count: 0 },
    debris_gold: 0,
    debris_ttl: 0,
    raid_cooldown: 0,
    pvp_cooldown: 0,
    archi_raids: [],
    archi_depleted: {},
    resource_raids: [],
    created_at: Date.now(),
    color: `hsl(${randInt(0, 360)}, 60%, 50%)`
  };
}

// Щит новичка: первые 2ч ИЛИ до lvl 5 ИЛИ до первой атаки на другого (что раньше)
function isNewbieShielded(p) {
  if (!p) return false;
  if (SHIELD_NEWBIE_HOURS === 0 && SHIELD_NEWBIE_MAX_LEVEL === 0) return false;
  const now = Date.now();
  if (now < p.created_at + SHIELD_NEWBIE_HOURS * 3600 * 1000) return true;
  if ((p.island_level || 0) < SHIELD_NEWBIE_MAX_LEVEL) return true;
  return false;
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
    boats_deployed: boatsInMissions(id),
    shield_until: p.shield_until,
    shield_cooldown_until: p.shield_cooldown_until || 0,
    has_attacked_anyone: p.has_attacked_anyone,
    is_newbie_shield: isNewbieShielded(p),
    destruction_state: p.destruction_state,
    legacy_bonus: p.legacy_bonus,
    passives: p.passives || defaultPassives(),
    debris_gold: Math.floor(p.debris_gold),
    online: p.online,
    color: p.color,
    pvp_cooldown: p.pvp_cooldown,
    created_at: p.created_at,
    active_raids: myRaidsCount,
    archi_raids: (p.archi_raids || []).map(r => ({
      idx: r.idx, startTime: r.startTime, duration: r.duration, type: r.type
    })),
    archi_depleted: p.archi_depleted || {},
    resource_raids: (p.resource_raids || []).map(r => ({
      islandId: r.islandId, startTime: r.startTime, duration: r.duration
    })),
    caravan_intercepts: caravanIntercepts.filter(ci => ci.owner === id).map(ci => ({
      caravanId: ci.caravanId, startTime: ci.startTime, duration: ci.duration, boats: ci.boats
    })),
    wipe_state: p.wipe_state || { threshold: WIPE_ATTACKS_MIN, count: 0 }
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
  return {
    players: list,
    pvpMissions: getPvpMissionsPublic(),
    resourceIslands: getResourceIslandsPublic(),
    capturePoints: getCapturePointsPublic(),
    caravans: getCaravansPublic(),
    timestamp: Date.now()
  };
}

// ── Wipe: сброс острова, пассивки переносятся, +5–10% wipe_boost ─────────
function wipePlayer(target, attackerNick) {
  const id = target.nick;
  const passives = target.passives || defaultPassives();
  const wipeBoostGain = WIPE_BOOST_MIN + Math.random() * (WIPE_BOOST_MAX - WIPE_BOOST_MIN);
  passives.wipe_boost = (passives.wipe_boost || 0) + wipeBoostGain;
  target.passives = passives;

  const pos = findFreePlayerPosition();
  target.pos_x = pos.x;
  target.pos_y = pos.y;
  target.island_level = 1;
  target.tavern_level = 1;
  target.dock_level = 1;
  target.cannon_level = 0;
  target.boats = BASE_BOAT_CAPACITY;
  target.rum = 0;
  target.gold = 0;
  target.wood = 0;
  target.destruction_state = 0;
  target.repair_progress = 0;
  target.shield_until = Date.now() + SHIELD_POST_ATTACK_HOURS * 3600 * 1000;
  target.wipe_state = { threshold: WIPE_ATTACKS_MIN + randInt(0, WIPE_ATTACKS_MAX - WIPE_ATTACKS_MIN), count: 0 };
  target.debris_gold = 0;
  target.debris_ttl = 0;
  target.archi_raids = [];
  target.resource_raids = [];
  target.legacy_bonus = 0;

  persist(id);
  io.to(id).emit('wiped', {
    by: attackerNick,
    passives: target.passives,
    wipe_boost_gained: wipeBoostGain,
    newShieldUntil: target.shield_until,
    msg: 'Island destroyed. Your legacy carries on — start again with bonus!'
  });
  io.emit('chat', { from: 'PvP', text: `${id}'s island was wiped by ${attackerNick}! +${wipeBoostGain.toFixed(0)}% legacy boost.` });
  console.log(`[WIPE] ${id} wiped by ${attackerNick}, wipe_boost +${wipeBoostGain.toFixed(1)}%`);
}

// ── Resolve PvP mission по прилёту ───────────────────────
function resolvePvpAttack(m) {
  const attacker = players[m.owner];
  const target   = players[m.targetNick];
  const boatsUsed = m.boatsUsed || PVP_BOATS_REQUIRED;

  // Цель отключилась — вернуть весь флот
  if (!target) {
    if (attacker) {
      attacker.boats = Math.min(attacker.boats + boatsUsed, boatCapacity(attacker.dock_level));
      io.to(m.owner).emit('attackResult', { ok: false, msg: 'Target not found' });
    }
    return;
  }

  // Щит: усиленный флот (4+ лодок) пробивает с 50%
  const isEnhanced = boatsUsed >= ENHANCED_RAID_BOATS;
  if (isTargetShielded(target, isEnhanced)) {
    if (attacker) {
      attacker.boats = Math.min(attacker.boats + boatsUsed, boatCapacity(attacker.dock_level));
      io.to(m.owner).emit('attackResult', { ok: false, msg: 'Target is shielded' });
    }
    return;
  }

  // ── Симуляция боя (survivorship) ────────────────────────
  // attackPower зависит от кол-ва отправленных лодок, не от текущего fleet
  const attackPower = boatsUsed * 10 + ((attacker ? attacker.island_level : 1) * 5);
  let defensePower  = target.cannon_level * 15 + target.island_level * 3;

  // Активная оборона
  const dm = m.defenseModifier;
  if (dm) {
    if (dm.type === 'cannon')    defensePower *= dm.defMult;
    if (dm.type === 'mercenary') defensePower += dm.defBonus;
  }

  // % выживших = (1 − defence_ratio) ± шум 15%
  const baseRatio   = defensePower / (attackPower + defensePower + 1);
  const noise       = (Math.random() - 0.5) * 0.30;
  const survivorPct = Math.max(0, Math.min(1, (1 - baseRatio) + noise));

  // Три порога исхода
  let stealPercent, boatLossPct, tier;
  if (survivorPct >= 0.70) {
    tier         = 'victory';
    stealPercent = 0.30 + Math.random() * 0.20;   // 30–50%
    boatLossPct  = 1 - survivorPct;
  } else if (survivorPct >= 0.40) {
    tier         = 'pyrrhic';
    stealPercent = 0.10 + Math.random() * 0.20;   // 10–30%
    boatLossPct  = 1 - survivorPct;
  } else {
    tier         = 'defeat';
    stealPercent = 0;
    boatLossPct  = 0.70 + Math.random() * 0.30;   // 70–100%
  }

  // Пассивный бонус pvp_steal (только при победе/пирровой)
  if (tier !== 'defeat') {
    const pvpStealBonus = (attacker && (attacker.passives || {}).pvp_steal) ? attacker.passives.pvp_steal / 100 : 0;
    stealPercent = Math.min(0.95, stealPercent + pvpStealBonus);
    if (dm && dm.type === 'shield') stealPercent = Math.max(0, stealPercent - dm.stealReduce);
  }

  // Возврат лодок
  const boatsLost     = Math.min(boatsUsed, Math.round(boatsUsed * boatLossPct));
  const boatsReturned = boatsUsed - boatsLost;
  if (attacker) {
    attacker.boats = Math.min(attacker.boats + boatsReturned, boatCapacity(attacker.dock_level));
  }

  const stolenRum  = Math.floor(target.rum  * stealPercent);
  const stolenGold = Math.floor(target.gold * stealPercent);
  const stolenWood = Math.floor(target.wood * stealPercent);
  const totalStolen = stolenRum + stolenGold + stolenWood;

  const debrisTotal    = Math.floor(totalStolen * DEBRIS_RATIO);
  const attackerDebris = Math.floor(debrisTotal * DEBRIS_ATTACKER_SHARE);
  const defenderDebris = Math.floor(debrisTotal * DEBRIS_DEFENDER_SHARE);

  if (attacker && tier !== 'defeat') {
    const safe = (totalStolen || 1);
    attacker.rum  += stolenRum  - Math.floor(stolenRum  * DEBRIS_RATIO) + Math.floor(attackerDebris * (stolenRum  / safe));
    attacker.gold += stolenGold - Math.floor(stolenGold * DEBRIS_RATIO) + Math.floor(attackerDebris * (stolenGold / safe));
    attacker.wood += stolenWood - Math.floor(stolenWood * DEBRIS_RATIO) + Math.floor(attackerDebris * (stolenWood / safe));
    const ap = attacker.passives || defaultPassives();
    ap.pvp_steal = (ap.pvp_steal || 0) + PVP_STEAL_PER_WIN_PCT;
    ap.pvp_wins  = (ap.pvp_wins  || 0) + 1;
    attacker.passives = ap;
    persist(m.owner);
  } else if (attacker) {
    persist(m.owner);
  }

  target.rum  = Math.max(0, target.rum  - stolenRum);
  target.gold = Math.max(0, target.gold - stolenGold);
  target.wood = Math.max(0, target.wood - stolenWood);
  target.debris_gold += defenderDebris;
  target.debris_ttl = Date.now() + 24 * 3600 * 1000;

  target.shield_until = Date.now() + SHIELD_POST_ATTACK_HOURS * 3600 * 1000;
  target.destruction_state = Math.min(2, (target.destruction_state || 0) + 1);

  const ws = target.wipe_state || { threshold: WIPE_ATTACKS_MIN + randInt(0, WIPE_ATTACKS_MAX - WIPE_ATTACKS_MIN), count: 0 };
  ws.count = (ws.count || 0) + 1;
  const levelDrop = WIPE_LEVEL_DROP_MIN + randInt(0, WIPE_LEVEL_DROP_MAX - WIPE_LEVEL_DROP_MIN);
  const oldLevel = target.island_level || 1;
  target.island_level = Math.max(1, oldLevel - levelDrop);
  const levelDropped = oldLevel - target.island_level;
  const didWipe = ws.count >= ws.threshold;
  if (didWipe) wipePlayer(target, m.owner);

  if (attacker) {
    io.to(m.owner).emit('attackResult', {
      ok: true,
      target: m.targetNick,
      tier,
      boatsLost,
      boatsReturned,
      stolen: { rum: stolenRum, gold: stolenGold, wood: stolenWood },
      totalStolen,
      debris: debrisTotal,
      levelDropped,
      targetWiped: didWipe
    });
  }

  persist(m.targetNick);

  io.to(m.targetNick).emit('attacked', {
    by: m.owner,
    lost: { rum: stolenRum, gold: stolenGold, wood: stolenWood },
    shieldUntil: target.shield_until,
    destructionState: target.destruction_state,
    debrisGold: defenderDebris,
    levelDropped,
    islandLevel: target.island_level,
    wiped: didWipe,
    defenseUsed: dm ? dm.type : null
  });

  io.emit('chat', {
    from: 'PvP',
    text: `${m.owner} raided ${m.targetNick}! Stole ${totalStolen} resources${levelDropped ? `, island -${levelDropped} lvl` : ''}`
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
      p.boats = Math.min(p.boats + 1, boatCapacity(p.dock_level));

      const pa = p.passives || defaultPassives();
      pa.successful_raids = (pa.successful_raids || 0) + 1;
      pa.raid_speed = Math.floor((pa.successful_raids || 0) / 10) * PASSIVE_RAID_SPEED_PER_10_RAIDS;
      p.passives = pa;

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
        const pa = p.passives || defaultPassives();
        pa.successful_raids = (pa.successful_raids || 0) + 1;
        pa.raid_speed = Math.floor((pa.successful_raids || 0) / 10) * PASSIVE_RAID_SPEED_PER_10_RAIDS;
        p.passives = pa;
      }
      p[lootDef.resource] = (p[lootDef.resource] || 0) + lootAmt;

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
        const pa = p.passives || defaultPassives();
        pa.successful_raids = (pa.successful_raids || 0) + 1;
        pa.raid_speed = Math.floor((pa.successful_raids || 0) / 10) * PASSIVE_RAID_SPEED_PER_10_RAIDS;
        p.passives = pa;
      }
      p[lootDef.resource] = (p[lootDef.resource] || 0) + lootAmt;

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

  // ── Проверка завершения захватов ───────────────────────
  const nowC = Date.now();
  let captureChanged = false;
  for (const cp of capturePoints) {
    if (!cp.capturer || !cp.captureEta) continue;
    if (nowC < cp.captureEta) continue;

    // Захват завершён — выдать лут
    const winner = players[cp.capturer];
    const lootGold = randInt(CAPTURE_LOOT.gold.min, CAPTURE_LOOT.gold.max);
    const lootRum  = randInt(CAPTURE_LOOT.rum.min,  CAPTURE_LOOT.rum.max);

    if (winner) {
      winner.gold += lootGold;
      winner.rum  += lootRum;
      persist(cp.capturer);
      io.to(cp.capturer).emit('captureResult', {
        pointId: cp.id,
        gold: lootGold,
        rum: lootRum
      });
    }

    io.emit('chat', {
      from: 'Capture',
      text: `${cp.capturer} captured a plunder spot! +${lootGold} gold, +${lootRum} rum`
    });
    console.log(`[CAPTURE] Point ${cp.id} captured by ${cp.capturer}: +${lootGold}g +${lootRum}r`);

    cp.capturer = null;
    cp.capturerBoats = 0;
    cp.captureEta = null;
    cp.respawnAt = nowC + CAPTURE_RESPAWN_MS;
    captureChanged = true;
  }
  if (captureChanged) io.emit('capturePoints', getCapturePointsPublic());

  // ── Проверка перехватов караванов ──────────────────────
  const nowCar = Date.now();
  for (let i = caravanIntercepts.length - 1; i >= 0; i--) {
    const ci = caravanIntercepts[i];
    if (nowCar < ci.startTime + ci.duration) continue;

    const p = players[ci.owner];
    const battle = resolveCaravanBattle(ci.boats, CARAVAN_NPC_BOATS);
    const boatsReturned = ci.boats - battle.playerLost;

    if (battle.winner === 'player') {
      const lootGold = randInt(CARAVAN_LOOT.gold.min, CARAVAN_LOOT.gold.max);
      const lootRum = randInt(CARAVAN_LOOT.rum.min, CARAVAN_LOOT.rum.max);
      const extraLoss = Math.random() < CARAVAN_LOSS_CHANCE ? 1 : 0;
      const totalLost = Math.min(ci.boats, battle.playerLost + extraLoss);
      const actualReturned = ci.boats - totalLost;

      if (p) {
        p.gold += lootGold;
        p.rum += lootRum;
        p.boats = Math.min(p.boats + actualReturned, boatCapacity(p.dock_level));
        const pa = p.passives || defaultPassives();
        pa.successful_raids = (pa.successful_raids || 0) + 1;
        pa.raid_speed = Math.floor((pa.successful_raids || 0) / 10) * PASSIVE_RAID_SPEED_PER_10_RAIDS;
        p.passives = pa;
        persist(ci.owner);
      }
      io.to(ci.owner).emit('caravanInterceptResult', {
        ok: true,
        won: true,
        gold: lootGold,
        rum: lootRum,
        boatsLost: totalLost,
        boatsReturned: actualReturned
      });
      if (p) io.to(ci.owner).emit('chat', { from: 'Caravan', text: `Caravan intercepted! +${lootGold} gold, +${lootRum} rum. Lost ${totalLost} boat(s).` });
    } else {
      if (p) {
        p.boats = Math.min(p.boats + boatsReturned, boatCapacity(p.dock_level));
        persist(ci.owner);
      }
      io.to(ci.owner).emit('caravanInterceptResult', {
        ok: true,
        won: false,
        boatsLost: battle.playerLost,
        boatsReturned: boatsReturned
      });
      if (p) io.to(ci.owner).emit('chat', { from: 'Caravan', text: `Caravan escort repelled you! Lost ${battle.playerLost} boat(s).` });
    }
    caravanIntercepts.splice(i, 1);
  }
}, RAID_CHECK_TICK);

// ── Движение караванов по маршруту (10 сек) ──────────────
setInterval(() => {
  for (const c of caravans) {
    const target = c.route[(c.routeIndex + 1) % c.route.length];
    const dx = target.x - c.x;
    const dy = target.y - c.y;
    const d = Math.sqrt(dx * dx + dy * dy) || 1;
    const step = Math.min(CARAVAN_SPEED, d);
    c.x += (dx / d) * step;
    c.y += (dy / d) * step;
    if (step >= d - 1) c.routeIndex = (c.routeIndex + 1) % c.route.length;
  }
}, CARAVAN_TICK_MS);

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
    const pa = p.passives || defaultPassives();
    pa.rum_bonus = (pa.rum_bonus || 0) + PASSIVE_RUM_PERCENT_PER_TAVERN_UPGRADE;
    p.passives = pa;
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
    const newCap = boatCapacity(p.dock_level); // только лимит; лодки покупаются отдельно
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

  // ── Постройка лодки ────────────────────────────────────
  socket.on('buyBoat', (callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];
    const cap = boatCapacity(p.dock_level);
    const deployed = boatsInMissions(currentNick);
    if (p.boats + deployed >= cap) {
      return callback({ ok: false, msg: 'Boats at maximum capacity (including deployed)' });
    }
    if (p.wood < BOAT_BUILD_COST) {
      return callback({ ok: false, msg: `Need ${BOAT_BUILD_COST} wood (have ${Math.floor(p.wood)})` });
    }
    p.wood -= BOAT_BUILD_COST;
    p.boats += 1;
    persist(currentNick);
    callback({ ok: true, boats: p.boats, boats_max: cap, wood: Math.floor(p.wood) });
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
    const ARCHI_TYPES_SRV = ['wood', 'wood', 'gold', 'rum'];
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

  // ── Захват нейтральной точки (PvP-конкуренция) ─────────
  socket.on('launchCapture', (data, callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];

    const pointId = parseInt(data?.pointId);
    const cp = capturePoints.find(c => c.id === pointId);
    if (!cp) return callback({ ok: false, msg: 'Invalid point' });
    if (cp.respawnAt > Date.now()) return callback({ ok: false, msg: 'Point not available yet' });
    if (cp.capturer === currentNick) return callback({ ok: false, msg: 'Already capturing this point' });
    if (cp.capturer) return callback({ ok: false, msg: 'Point is being captured — use launchIntercept' });

    const boats = Math.max(1, Math.min(CAPTURE_MAX_BOATS, parseInt(data?.boats) || 1));
    if (p.boats < boats) return callback({ ok: false, msg: `Need ${boats} boats (have ${p.boats})` });

    p.boats -= boats;
    const duration = Math.round(CAPTURE_BASE_TIME_MS / boats);
    cp.capturer = currentNick;
    cp.capturerBoats = boats;
    cp.captureEta = Date.now() + duration;

    persist(currentNick);
    io.emit('capturePoints', getCapturePointsPublic());
    io.emit('chat', { from: 'Capture', text: `${currentNick} is capturing a plunder spot! (~${Math.round(duration / 1000)}s)` });
    console.log(`[CAPTURE] ${currentNick}: point ${pointId} (${boats} boats, ${Math.round(duration / 1000)}s)`);
    callback({ ok: true, eta: Math.round(duration / 1000), pointId });
  });

  // ── Перехват захвата точки ──────────────────────────────
  socket.on('launchIntercept', (data, callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];

    const pointId = parseInt(data?.pointId);
    const cp = capturePoints.find(c => c.id === pointId);
    if (!cp) return callback({ ok: false, msg: 'Invalid point' });
    if (!cp.capturer) return callback({ ok: false, msg: 'Nothing to intercept' });
    if (cp.capturer === currentNick) return callback({ ok: false, msg: 'Cannot intercept your own capture' });

    const boats = Math.max(1, Math.min(CAPTURE_MAX_BOATS, parseInt(data?.boats) || 1));
    if (p.boats < boats) return callback({ ok: false, msg: `Need ${boats} boats (have ${p.boats})` });

    const defenderNick  = cp.capturer;
    const defenderBoats = cp.capturerBoats;
    const defender = players[defenderNick];

    p.boats -= boats;

    const battle = resolveFleetBattle(boats, defenderBoats);
    const interceptorWon = battle.winner === 'a';

    p.boats = Math.min(p.boats + (boats - battle.aLost), boatCapacity(p.dock_level));
    if (defender) {
      defender.boats = Math.min(defender.boats + (defenderBoats - battle.bLost), boatCapacity(defender.dock_level));
    }

    const loserDebris = randInt(30, 80);
    if (interceptorWon) {
      const remaining = cp.captureEta ? Math.max(30000, cp.captureEta - Date.now()) : CAPTURE_BASE_TIME_MS;
      cp.capturer = currentNick;
      cp.capturerBoats = Math.max(1, boats - battle.aLost);
      cp.captureEta = Date.now() + remaining;
      if (defender) { defender.debris_gold += loserDebris; defender.debris_ttl = Date.now() + 24 * 3600 * 1000; }
      io.to(defenderNick).emit('intercepted', { by: currentNick, pointId, boatsLost: battle.bLost, debris: loserDebris });
    } else {
      p.debris_gold += loserDebris;
      p.debris_ttl = Date.now() + 24 * 3600 * 1000;
      io.to(defenderNick).emit('intercepted', { by: currentNick, pointId, result: 'defended', boatsLost: battle.bLost });
    }

    persist(currentNick);
    if (defender) persist(defenderNick);

    const chatText = interceptorWon
      ? `${currentNick} seized the plunder spot from ${defenderNick}!`
      : `${currentNick} failed to intercept ${defenderNick}!`;
    io.emit('chat', { from: 'Capture', text: chatText });
    io.emit('capturePoints', getCapturePointsPublic());
    console.log(`[INTERCEPT] ${currentNick} vs ${defenderNick} at point ${pointId}: winner=${interceptorWon ? 'interceptor' : 'defender'}`);

    callback({ ok: true, won: interceptorWon, boatsLost: battle.aLost, defenderBoatsLost: battle.bLost });
  });

  // ── Перехват имперского каравана ───────────────────────
  socket.on('interceptCaravan', (data, callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];

    const caravanId = parseInt(data?.caravanId);
    const caravan = caravans.find(c => c.id === caravanId);
    if (!caravan) return callback({ ok: false, msg: 'Caravan not found' });

    const boats = Math.max(1, Math.min(5, parseInt(data?.boats) || 1));
    if (p.boats < boats) return callback({ ok: false, msg: `Need ${boats} boats (have ${p.boats})` });

    const existing = caravanIntercepts.find(ci => ci.caravanId === caravanId);
    const now = Date.now();

    if (!existing) {
      // Первый перехват этого каравана — обычная миссия против NPC
      const duration = CARAVAN_INTERCEPT_MIN_MS + Math.random() * (CARAVAN_INTERCEPT_MAX_MS - CARAVAN_INTERCEPT_MIN_MS);
      const interceptId = now.toString(36) + Math.random().toString(36).slice(2, 6);
      p.boats -= boats;
      caravanIntercepts.push({ id: interceptId, owner: currentNick, caravanId, boats, startTime: now, duration });

      persist(currentNick);
      io.emit('state', getStatePayload());
      console.log(`[CARAVAN] ${currentNick}: intercepting caravan ${caravanId} with ${boats} boats, ETA ${Math.round(duration / 60000)}min`);
      callback({ ok: true, duration: Math.round(duration), eta: Math.ceil(duration / 1000) });
      return;
    }

    // Конкуренция: другой игрок уже перехватывает этот караван.
    // Новый перехватчик пытается «сбить» старого: Fleet vs Fleet за право продолжать перехват.
    const defenderOwner = existing.owner;
    const defender = players[defenderOwner];
    const battle = resolveFleetBattle(boats, existing.boats);

    // Списываем лодки атакующего из дока
    p.boats -= boats;

    if (battle.winner === 'a') {
      // Новый игрок выигрывает схватку и перехватывает миссию
      const attackerRemaining = Math.max(1, boats - battle.aLost);
      const defenderRemaining = Math.max(0, existing.boats - battle.bLost);

      // Защитнику возвращаем выжившие лодки на базу
      if (defender && defenderRemaining > 0) {
        defender.boats = Math.min(defender.boats + defenderRemaining, boatCapacity(defender.dock_level));
        persist(defenderOwner);
      }

      // Обновляем запись перехвата под нового владельца
      existing.owner = currentNick;
      existing.boats = attackerRemaining;
      existing.startTime = now; // можно перезапустить таймер
      // duration оставляем как было, чтобы общий ETA не рос бесконечно

      io.to(defenderOwner).emit('chat', {
        from: 'Caravan',
        text: `${currentNick} seized your caravan intercept! Lost ${battle.bLost} boat(s).`
      });

      persist(currentNick);
      io.emit('state', getStatePayload());
      console.log(`[CARAVAN] ${currentNick} seized caravan ${caravanId} from ${defenderOwner}: boats ${boats}→${attackerRemaining}, defenderLost=${battle.bLost}`);
      callback({ ok: true, duration: Math.round(existing.duration), eta: Math.ceil((existing.startTime + existing.duration - now) / 1000) });
    } else {
      // Защитник удержал перехват, атакующий теряет часть флота и возвращает выживших на базу
      const attackerRemaining = Math.max(0, boats - battle.aLost);
      if (attackerRemaining > 0) {
        p.boats = Math.min(p.boats + attackerRemaining, boatCapacity(p.dock_level));
      }
      // Лёгкий урон по защитнику: уменьшаем его боевой состав в миссии
      existing.boats = Math.max(1, existing.boats - battle.bLost);
      if (defender) persist(defenderOwner);
      persist(currentNick);

      io.to(defenderOwner).emit('chat', {
        from: 'Caravan',
        text: `${currentNick} tried to steal your caravan but failed! Lost ${battle.aLost} boat(s).`
      });

      console.log(`[CARAVAN] ${currentNick} failed to seize caravan ${caravanId} from ${defenderOwner}: lost ${battle.aLost}, defenderLost=${battle.bLost}`);
      callback({ ok: false, msg: `Another pirate is already intercepting this caravan (you lost ${battle.aLost} boat(s) in the skirmish)` });
    }
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
    if (SHIELD_POST_ATTACK_HOURS > 0 && target.shield_until > Date.now()) {
      const remainH = Math.ceil((target.shield_until - Date.now()) / 3600000);
      return callback({ ok: false, msg: `Target is shielded (${remainH}h left)` });
    }
    if (isNewbieShielded(target)) {
      return callback({ ok: false, msg: 'Target is under newbie protection' });
    }

    // Кулдаун
    if (attacker.pvp_cooldown > Date.now()) {
      const remainM = Math.ceil((attacker.pvp_cooldown - Date.now()) / 60000);
      return callback({ ok: false, msg: `PvP cooldown: ${remainM} min` });
    }

    const maxFleet  = Math.min(5, boatCapacity(attacker.dock_level));
    const fleetSize = Math.max(1, Math.min(maxFleet, parseInt(data.fleetSize) || 1));
    if (attacker.boats < fleetSize) {
      return callback({ ok: false, msg: `Need ${fleetSize} boats (have ${attacker.boats})` });
    }
    const boatsNeeded = fleetSize;

    const pvpAttacks = attacker.pvp_attacks || {};
    const rec = pvpAttacks[targetNick] || { c: 0, t: 0 };
    const now = Date.now();
    const dayAgo = now - 24 * 3600 * 1000;
    if (rec.t > dayAgo && rec.c >= PVP_ATTACKS_PER_TARGET_PER_24H) {
      return callback({ ok: false, msg: 'Max 5 attacks per target per day' });
    }
    if (rec.c >= PVP_FLEET_FATIGUE_AFTER && (now - rec.t) < PVP_FLEET_FATIGUE_COOLDOWN_MS) {
      return callback({ ok: false, msg: 'Fleet fatigue: wait 1h before attacking this player again' });
    }
    if (rec.t > 0 && (now - rec.t) < PVP_ATTACK_COOLDOWN_MS) {
      return callback({ ok: false, msg: 'Wait 1–2 min before attacking same player again' });
    }

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
      ownerColor: attacker.color,
      boatsUsed: boatsNeeded,
      defenseModifier: null   // заполняется через activeDefenseChoice
    });

    // Уведомить цель об атаке — даёт время выбрать активную оборону
    io.to(targetNick).emit('incomingAttack', {
      missionId,
      by: currentNick,
      eta,
      boatsUsed: boatsNeeded,
      defenseTimeout: ACTIVE_DEFENSE_TIMEOUT_MS
    });

    attacker.has_attacked_anyone = true;
    attacker.boats = Math.max(0, attacker.boats - boatsNeeded);
    attacker.pvp_cooldown = now + PVP_COOLDOWN_MS;
    pvpAttacks[targetNick] = { c: rec.c + 1, t: now };
    attacker.pvp_attacks = pvpAttacks;
    persist(currentNick);

    console.log(`[PVP] ${currentNick} → ${targetNick}: mission launched (dist=${Math.round(dist)}, ETA=${eta}s)`);

    callback({ ok: true, launched: true, eta, targetNick, fleetSize: boatsNeeded });

    io.emit('chat', {
      from: 'PvP',
      text: `${currentNick} launched attack on ${targetNick}! (ETA ~${eta}s)`
    });
    io.emit('state', getStatePayload());
  });

  // ── Покупка щита (10% ресурсов, 1 ч, кулдаун 6 ч) ─────
  socket.on('buyShield', (callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });
    const p = players[currentNick];
    const now = Date.now();
    if (p.shield_cooldown_until > now) {
      const remainM = Math.ceil((p.shield_cooldown_until - now) / 60000);
      return callback({ ok: false, msg: `Shield cooldown: ${remainM} min` });
    }
    const costRum = Math.max(0, Math.floor(p.rum * SHIELD_BUY_COST_PCT));
    const costGold = Math.max(0, Math.floor(p.gold * SHIELD_BUY_COST_PCT));
    const costWood = Math.max(0, Math.floor(p.wood * SHIELD_BUY_COST_PCT));
    if (p.rum < costRum || p.gold < costGold || p.wood < costWood) {
      return callback({ ok: false, msg: `Need 10% resources (${costRum} rum, ${costGold} gold, ${costWood} wood)` });
    }
    p.rum -= costRum;
    p.gold -= costGold;
    p.wood -= costWood;
    p.shield_until = now + SHIELD_BUY_DURATION_MS;
    p.shield_cooldown_until = now + SHIELD_BUY_COOLDOWN_MS;
    persist(currentNick);
    console.log(`[SHIELD] ${currentNick} bought shield (1h), cooldown 6h`);
    callback({ ok: true, shieldUntil: p.shield_until, shieldCooldownUntil: p.shield_cooldown_until });
  });

  // ── Активная оборона (выбор при входящей атаке) ────────
  socket.on('activeDefenseChoice', (data, callback) => {
    if (typeof callback !== 'function') return;
    if (!currentNick || !players[currentNick]) return callback({ ok: false, msg: 'Not logged in' });

    const { missionId, choice } = data || {};
    const m = pvpMissions.find(m => m.id === missionId && m.targetNick === currentNick);
    if (!m) return callback({ ok: false, msg: 'Mission not found or already resolved' });
    if (m.defenseModifier) return callback({ ok: false, msg: 'Defense already chosen' });

    const p = players[currentNick];

    if (choice === 'cannon') {
      const cost = Math.max(10, Math.floor(p.rum * ACTIVE_DEFENSE_CANNON_RUM_PCT));
      if (p.rum < cost) return callback({ ok: false, msg: `Need ${cost} rum` });
      p.rum -= cost;
      m.defenseModifier = { type: 'cannon', defMult: ACTIVE_DEFENSE_CANNON_DEF_MULT };
      console.log(`[DEFENSE] ${currentNick}: cannon volley (cost ${cost} rum)`);
      callback({ ok: true, type: 'cannon', cost });

    } else if (choice === 'shield') {
      const cost = ACTIVE_DEFENSE_SHIELD_GOLD_COST;
      if (p.gold < cost) return callback({ ok: false, msg: `Need ${cost} gold` });
      p.gold -= cost;
      m.defenseModifier = { type: 'shield', stealReduce: ACTIVE_DEFENSE_SHIELD_STEAL_REDUCE };
      console.log(`[DEFENSE] ${currentNick}: shield boost (cost ${cost} gold)`);
      callback({ ok: true, type: 'shield', cost });

    } else if (choice === 'mercenary') {
      const cost = ACTIVE_DEFENSE_MERC_RUM_COST;
      if (p.rum < cost) return callback({ ok: false, msg: `Need ${cost} rum` });
      p.rum -= cost;
      m.defenseModifier = { type: 'mercenary', defBonus: ACTIVE_DEFENSE_MERC_DEF_BONUS };
      console.log(`[DEFENSE] ${currentNick}: mercenaries hired (cost ${cost} rum)`);
      callback({ ok: true, type: 'mercenary', cost });

    } else {
      return callback({ ok: false, msg: 'Unknown choice' });
    }

    persist(currentNick);
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

  generateCapturePoints();
  generateCaravans();

  const PORT = process.env.PORT || 3000;
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[SYSTEM] PirateIsles v0.1 running on http://localhost:${PORT}`);
  });
}
start();
