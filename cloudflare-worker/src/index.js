const DEFAULT_WEBHOOK_SECRET = "hook-123";
const DEFAULT_TARGET_CHAT = "-1003167239288";
const DEFAULT_REPOSITORY = "znamteam-max/HOH_NHL_Daily_Results";
const DEFAULT_GITHUB_REF = "main";
const NHL_BASE = "https://api-web.nhle.com/v1";

const TEAM_RU = {
  ANA: "Анахайм",
  ARI: "Аризона",
  BOS: "Бостон",
  BUF: "Баффало",
  CGY: "Калгари",
  CAR: "Каролина",
  CHI: "Чикаго",
  COL: "Колорадо",
  CBJ: "Коламбус",
  DAL: "Даллас",
  DET: "Детройт",
  EDM: "Эдмонтон",
  FLA: "Флорида",
  LAK: "Лос-Анджелес",
  MIN: "Миннесота",
  MTL: "Монреаль",
  NSH: "Нэшвилл",
  NJD: "Нью-Джерси",
  NYI: "Айлендерс",
  NYR: "Рейнджерс",
  OTT: "Оттава",
  PHI: "Филадельфия",
  PIT: "Питтсбург",
  SJS: "Сан-Хосе",
  SEA: "Сиэтл",
  STL: "Сент-Луис",
  TBL: "Тампа-Бэй",
  TOR: "Торонто",
  VAN: "Ванкувер",
  VGK: "Вегас",
  WSH: "Вашингтон",
  WPG: "Виннипег",
  UTA: "Юта",
};

const TEAM_EMOJI = {
  ANA: "🦆",
  ARI: "🦊",
  BOS: "🐻",
  BUF: "🦬",
  CGY: "🔥",
  CAR: "🌪️",
  CHI: "🦅",
  COL: "⛰️",
  CBJ: "💣",
  DAL: "⭐",
  DET: "🛡️",
  EDM: "🛢️",
  FLA: "🐆",
  LAK: "👑",
  MIN: "🌲",
  MTL: "🇨🇦",
  NSH: "🐯",
  NJD: "😈",
  NYI: "🏝️",
  NYR: "🗽",
  OTT: "🛡",
  PHI: "🛩",
  PIT: "🐧",
  SJS: "🦈",
  SEA: "🦑",
  STL: "🎵",
  TBL: "⚡",
  TOR: "🍁",
  VAN: "🐳",
  VGK: "🎰",
  WSH: "🦅",
  WPG: "✈️",
  UTA: "🧊",
};

const WEEKDAYS_RU = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"];

export default {
  async fetch(request, env) {
    return handleRequest(request, env);
  },

  async scheduled(controller, env, ctx) {
    if (!envFlag(env.CLOUDFLARE_CRON_ENABLED, false)) {
      return;
    }

    ctx.waitUntil(
      triggerRepositoryDispatch(env, eventName(env, "GITHUB_DISPATCH_EVENT_POLL", "nhl_poll"), {
        source: "cloudflare_cron",
        scheduled_time: controller.scheduledTime,
      }),
    );
  },
};

async function handleRequest(request, env) {
  const url = new URL(request.url);
  const path = stripTrailingSlash(url.pathname);

  if (path === "") {
    return jsonResponse({ ok: true, service: "hoh-nhl-daily-results", runtime: "cloudflare-workers" });
  }

  if (["/api/setup-webhook", "/setup-webhook"].includes(path)) {
    return setupWebhook(request, env);
  }

  if (["/api/menu", "/menu"].includes(path)) {
    return sendMenuRoute(request, env);
  }

  if (["/api/setup-commands", "/setup-commands"].includes(path)) {
    return setupCommandsRoute(request, env);
  }

  if (["/api/telegram", "/telegram"].includes(path)) {
    return telegramWebhook(request, env);
  }

  if (["/api/cron", "/cron"].includes(path)) {
    return cronRoute(request, env);
  }

  return jsonResponse({ ok: false, error: "not_found" }, 404);
}

async function setupWebhook(request, env) {
  if (!isManagementAuthorized(request, env)) {
    return jsonResponse({ ok: false, error: "unauthorized" }, 401);
  }

  const url = new URL(request.url);
  const webhookUrl = `${publicBaseUrl(request, env)}/api/telegram`;
  const telegram = await telegramRequest(env, "setWebhook", {
    url: webhookUrl,
    secret_token: webhookSecret(env),
    allowed_updates: ["message", "channel_post", "callback_query"],
  });
  const commands = await setBotCommands(env);

  let menu = null;
  if (queryBool(url, "send_menu", true) && telegram.ok) {
    menu = await sendMenu(env, menuChatId(env));
  }

  return jsonResponse(
    {
      ok: telegram.ok,
      webhook_url: webhookUrl,
      webhook_secret: "configured",
      telegram,
      commands,
      menu,
    },
    telegram.ok ? 200 : 500,
  );
}

async function sendMenuRoute(request, env) {
  if (!isManagementAuthorized(request, env)) {
    return jsonResponse({ ok: false, error: "unauthorized" }, 401);
  }

  const url = new URL(request.url);
  const chatId = (url.searchParams.get("chat") || menuChatId(env)).trim();
  if (!chatId) {
    return jsonResponse({ ok: false, error: "missing_chat" }, 500);
  }

  const telegram = await sendMenu(env, chatId);
  return jsonResponse({ ok: telegram.ok, chat_id: chatId, telegram }, telegram.ok ? 200 : 500);
}

async function setupCommandsRoute(request, env) {
  if (!isManagementAuthorized(request, env)) {
    return jsonResponse({ ok: false, error: "unauthorized" }, 401);
  }

  const commands = await setBotCommands(env);
  return jsonResponse({ ok: commands.ok, commands }, commands.ok ? 200 : 500);
}

async function setBotCommands(env) {
  return telegramRequest(env, "setMyCommands", {
    commands: [
      { command: "menu", description: "Показать меню" },
      { command: "latest", description: "Показать последние матчи" },
      { command: "schedule", description: "Расписание по дням" },
      { command: "reload", description: "Загрузить заново последний игровой день" },
      { command: "resend", description: "Повторить отправку последнего игрового дня" },
    ],
  });
}

async function telegramWebhook(request, env) {
  if (request.method !== "POST") {
    return jsonResponse({ ok: false, error: "method_not_allowed" }, 405);
  }

  const providedSecret = request.headers.get("x-telegram-bot-api-secret-token") || "";
  if (providedSecret !== webhookSecret(env)) {
    return jsonResponse({ ok: false, error: "unauthorized" }, 401);
  }

  const update = await request.json();
  const callback = update.callback_query || null;
  if (callback) {
    return handleCallback(callback, env);
  }

  const message = update.message || update.channel_post || {};
  const chatId = message.chat?.id;
  if (!isAllowedChat(env, chatId)) {
    return jsonResponse({ ok: true, skipped: "chat_not_allowed" });
  }

  const command = commandName(message.text || "");
  if (["/start", "/menu", "/help"].includes(command)) {
    await sendMenu(env, chatId);
  } else if (command === "/latest") {
    await sendLatestMatches(env, chatId);
  } else if (command === "/schedule") {
    await sendScheduleOverview(env, chatId);
  } else if (["/reload", "/resend"].includes(command)) {
    await resendLatestDay(env, chatId);
  }

  return jsonResponse({ ok: true });
}

async function cronRoute(request, env) {
  if (!isManagementAuthorized(request, env)) {
    return jsonResponse({ ok: false, error: "unauthorized" }, 401);
  }

  const result = await triggerRepositoryDispatch(env, eventName(env, "GITHUB_DISPATCH_EVENT_POLL", "nhl_poll"), {
    source: "cloudflare_http_cron",
  });
  return jsonResponse({ ok: true, dispatch: result });
}

async function handleCallback(callback, env) {
  const callbackId = callback.id;
  const data = callback.data || "";
  const chatId = callback.message?.chat?.id;

  if (!isAllowedChat(env, chatId)) {
    await answerCallback(env, callbackId, "Эта кнопка доступна только в группе HOH NHL Results.");
    return jsonResponse({ ok: true, skipped: "chat_not_allowed" });
  }

  if (data === "latest_matches") {
    await answerCallback(env, callbackId, "Показываю последние матчи...");
    await sendLatestMatches(env, chatId);
    return jsonResponse({ ok: true, action: data });
  }

  if (data === "schedule_overview") {
    await answerCallback(env, callbackId, "Показываю расписание...");
    await sendScheduleOverview(env, chatId);
    return jsonResponse({ ok: true, action: data });
  }

  if (data === "resend_last_day") {
    await answerCallback(env, callbackId, "Запускаю повторную отправку...");
    const result = await resendLatestDay(env, chatId);
    return jsonResponse({ ok: result.ok, action: data, dispatch: result }, result.ok ? 200 : 500);
  }

  await answerCallback(env, callbackId, "Неизвестная команда");
  return jsonResponse({ ok: false, error: "unknown_callback" }, 400);
}

async function sendLatestMatches(env, chatId) {
  try {
    await sendText(env, chatId, await latestMatchesText(env));
    return { ok: true };
  } catch (error) {
    await sendText(env, chatId, `Не получилось загрузить последние матчи: ${error.message}`);
    return { ok: false, error: error.message };
  }
}

async function sendScheduleOverview(env, chatId) {
  try {
    await sendText(env, chatId, await scheduleOverviewText(env));
    return { ok: true };
  } catch (error) {
    await sendText(env, chatId, `Не получилось загрузить расписание: ${error.message}`);
    return { ok: false, error: error.message };
  }
}

async function resendLatestDay(env, chatId) {
  await sendText(env, chatId, "Запускаю повторную отправку последнего игрового дня.");

  try {
    const result = await triggerRepositoryDispatch(env, eventName(env, "GITHUB_DISPATCH_EVENT_RESEND", "resend_last_day"), {
      source: "telegram_menu",
      resend_last_day: "true",
      target_chat_id: menuChatId(env),
    });
    await sendText(env, chatId, "Готово: GitHub Actions запущен, последний игровой день будет отправлен повторно.");
    return { ok: true, ...result };
  } catch (error) {
    await sendText(env, chatId, `Не получилось запустить повторную отправку: ${error.message}`);
    return { ok: false, error: error.message };
  }
}

async function latestMatchesText(env) {
  const baseDay = currentHockeyDayPT();
  const daysBack = envInt(env.MENU_LATEST_DAYS_BACK, 6, 1, 14);
  const limit = envInt(env.MENU_LATEST_LIMIT, 12, 3, 25);

  const metas = [];
  for (const day of dateRange(baseDay, -daysBack, 1)) {
    metas.push(...(await metasForDay(day)));
  }

  const seen = new Set();
  const finals = metas
    .filter((meta) => {
      if (seen.has(meta.gamePk) || !isFinalState(meta.state)) {
        return false;
      }
      seen.add(meta.gamePk);
      return true;
    })
    .sort((a, b) => b.gameDateUTC.getTime() - a.gameDateUTC.getTime());

  if (!finals.length) {
    return "Последние завершённые матчи не найдены.";
  }

  return ["Последние завершённые матчи", "", ...finals.slice(0, limit).map(matchLine)].join("\n");
}

async function scheduleOverviewText(env) {
  const baseDay = currentHockeyDayPT();
  const daysBack = envInt(env.MENU_SCHEDULE_DAYS_BACK, 2, 0, 7);
  const daysForward = envInt(env.MENU_SCHEDULE_DAYS_FORWARD, 10, 1, 21);
  const lines = ["Расписание NHL по игровым дням", ""];

  for (const day of dateRange(baseDay, -daysBack, daysForward)) {
    const metas = await metasForDay(day);
    const total = metas.length;
    const finalCount = metas.filter((meta) => isFinalState(meta.state)).length;
    const liveCount = metas.filter((meta) => isLiveishState(meta.state)).length;
    const upcomingCount = Math.max(0, total - finalCount - liveCount);
    const weekday = WEEKDAYS_RU[weekdayIndex(day)];
    const status = `${finalCount} завершено, ${liveCount} в игре, ${upcomingCount} впереди`;
    lines.push(`${formatDay(day)} ${weekday} — ${total} ${pluralRu(total, "матч", "матча", "матчей")}: ${status}`);
  }

  return lines.join("\n");
}

async function metasForDay(day) {
  const games = await fetchGamesForDay(day);
  return games.map(gameToMeta).filter(Boolean);
}

async function fetchGamesForDay(day) {
  const response = await fetch(`${NHL_BASE}/schedule/${day}`, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`NHL schedule failed: HTTP ${response.status}`);
  }

  const payload = await response.json();
  let games = payload.games || [];
  if (!games.length && Array.isArray(payload.gameWeek)) {
    games = payload.gameWeek.flatMap((weekDay) => weekDay.games || []);
  }

  if (games.some((game) => Object.prototype.hasOwnProperty.call(game, "gameDate"))) {
    games = games.filter((game) => String(game.gameDate || "") === day);
  }

  const seen = new Set();
  return games.filter((game) => {
    const id = firstInt(game.id, game.gameId, game.gamePk);
    if (!id || seen.has(id)) {
      return false;
    }
    seen.add(id);
    return true;
  });
}

function gameToMeta(game) {
  const gamePk = firstInt(game.id, game.gameId, game.gamePk);
  if (!gamePk) {
    return null;
  }

  const home = game.homeTeam || {};
  const away = game.awayTeam || {};
  const homeTri = upper(home.abbrev || home.triCode || home.teamAbbrev);
  const awayTri = upper(away.abbrev || away.triCode || away.teamAbbrev);
  const homeScore = firstInt(home.score);
  const awayScore = firstInt(away.score);
  const state = upper(game.gameState || game.gameStatus);
  const gameDateUTC = parseGameDate(game.startTimeUTC || game.gameDate);

  const series = game.seriesStatus || {};
  const seriesGame = firstInt(series.gameNumberOfSeries) || null;
  let homeSeriesWins = null;
  let awaySeriesWins = null;
  const top = upper(series.topSeedTeamAbbrev);
  const bottom = upper(series.bottomSeedTeamAbbrev);
  const topWins = firstInt(series.topSeedWins);
  const bottomWins = firstInt(series.bottomSeedWins);

  if (homeTri === top) {
    homeSeriesWins = topWins;
  } else if (homeTri === bottom) {
    homeSeriesWins = bottomWins;
  }
  if (awayTri === top) {
    awaySeriesWins = topWins;
  } else if (awayTri === bottom) {
    awaySeriesWins = bottomWins;
  }

  if (
    seriesGame &&
    isFinalState(state) &&
    homeSeriesWins !== null &&
    awaySeriesWins !== null &&
    homeScore !== awayScore &&
    homeSeriesWins + awaySeriesWins === seriesGame - 1
  ) {
    if (homeScore > awayScore) {
      homeSeriesWins += 1;
    } else {
      awaySeriesWins += 1;
    }
  }

  return {
    gamePk,
    gameDateUTC,
    state,
    homeTri,
    awayTri,
    homeScore,
    awayScore,
    seriesGame,
    homeSeriesWins,
    awaySeriesWins,
  };
}

function matchLine(meta) {
  const details = seriesText(meta);
  const suffix = details ? ` · ${details}` : "";
  return `${formatDay(toPTDate(meta.gameDateUTC))} · ${teamLabel(meta.homeTri)} ${meta.homeScore}:${meta.awayScore} ${teamLabel(meta.awayTri)}${suffix}`;
}

function seriesText(meta) {
  const pieces = [];
  if (meta.seriesGame) {
    pieces.push(`Матч №${meta.seriesGame}`);
  }
  if (meta.homeSeriesWins !== null && meta.awaySeriesWins !== null) {
    pieces.push(`серия ${meta.homeSeriesWins}-${meta.awaySeriesWins}`);
  }
  return pieces.join(", ");
}

async function sendMenu(env, chatId) {
  return sendText(env, chatId, "Меню HOH NHL Results", {
    inline_keyboard: [
      [{ text: "Показать последние матчи", callback_data: "latest_matches" }],
      [{ text: "Загрузить заново последний игровой день", callback_data: "resend_last_day" }],
      [{ text: "Расписание по дням", callback_data: "schedule_overview" }],
    ],
  });
}

async function sendText(env, chatId, text, replyMarkup = null) {
  const payload = {
    chat_id: chatId,
    text,
    disable_web_page_preview: true,
  };

  if (env.TELEGRAM_THREAD_ID) {
    payload.message_thread_id = Number(env.TELEGRAM_THREAD_ID);
  }
  if (replyMarkup) {
    payload.reply_markup = replyMarkup;
  }

  return telegramRequest(env, "sendMessage", payload);
}

async function answerCallback(env, callbackId, text) {
  if (!callbackId) {
    return null;
  }
  return telegramRequest(env, "answerCallbackQuery", {
    callback_query_id: callbackId,
    text,
  });
}

async function telegramRequest(env, method, payload) {
  if (!env.TELEGRAM_BOT_TOKEN) {
    return { ok: false, error: "missing_TELEGRAM_BOT_TOKEN" };
  }

  const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await response.json().catch(() => ({}));
  return {
    ok: response.ok && data.ok === true,
    status_code: response.status,
    response: data,
  };
}

async function triggerRepositoryDispatch(env, eventType, clientPayload) {
  const token = env.GITHUB_DISPATCH_TOKEN || env.GITHUB_STATE_TOKEN || env.GITHUB_TOKEN;
  if (!token) {
    throw new Error("missing_GITHUB_DISPATCH_TOKEN");
  }

  const repository = env.GITHUB_REPOSITORY || DEFAULT_REPOSITORY;
  const payload = {
    event_type: eventType,
    client_payload: {
      ref: env.GITHUB_REF || DEFAULT_GITHUB_REF,
      ...clientPayload,
    },
  };

  const response = await fetch(`https://api.github.com/repos/${repository}/dispatches`, {
    method: "POST",
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "User-Agent": "hoh-nhl-cloudflare-worker",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify(payload),
  });

  if (response.status !== 204) {
    const body = await response.text();
    throw new Error(`GitHub dispatch failed: HTTP ${response.status} ${body.slice(0, 300)}`);
  }

  return { ok: true, status_code: response.status, event_type: eventType };
}

function isManagementAuthorized(request, env) {
  const url = new URL(request.url);
  const provided = (url.searchParams.get("secret") || "").trim();
  const authorization = request.headers.get("authorization") || "";
  const expected = managementSecret(env);
  return provided === expected || authorization === `Bearer ${expected}`;
}

function isAllowedChat(env, chatId) {
  if (!chatId) {
    return false;
  }
  if (envFlag(env.TELEGRAM_ALLOW_ANY_CHAT, false)) {
    return true;
  }
  return String(chatId) === menuChatId(env);
}

function menuChatId(env) {
  return String(env.TELEGRAM_MENU_CHAT_ID || env.TELEGRAM_CHAT_ID || DEFAULT_TARGET_CHAT).trim();
}

function webhookSecret(env) {
  return String(env.TELEGRAM_WEBHOOK_SECRET || DEFAULT_WEBHOOK_SECRET).trim() || DEFAULT_WEBHOOK_SECRET;
}

function managementSecret(env) {
  return String(env.WEBHOOK_SETUP_SECRET || webhookSecret(env)).trim() || webhookSecret(env);
}

function publicBaseUrl(request, env) {
  const configured = String(env.PUBLIC_BASE_URL || "").trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }
  const url = new URL(request.url);
  return `${url.protocol}//${url.host}`;
}

function commandName(text) {
  if (!text) {
    return "";
  }
  return text.trim().split(/\s+/)[0].toLowerCase().split("@", 1)[0];
}

function currentHockeyDayPT(now = new Date()) {
  const pt = partsInTimeZone(now, "America/Los_Angeles");
  return pt.hour >= 6 ? pt.date : addDays(pt.date, -1);
}

function toPTDate(date) {
  return partsInTimeZone(date, "America/Los_Angeles").date;
}

function partsInTimeZone(date, timeZone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const value = (type) => parts.find((part) => part.type === type)?.value || "";
  return {
    date: `${value("year")}-${value("month")}-${value("day")}`,
    hour: Number(value("hour")),
  };
}

function dateRange(baseDay, startOffset, endOffset) {
  const days = [];
  for (let offset = startOffset; offset <= endOffset; offset += 1) {
    days.push(addDays(baseDay, offset));
  }
  return days;
}

function addDays(day, offset) {
  const date = new Date(`${day}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + offset);
  return date.toISOString().slice(0, 10);
}

function parseGameDate(value) {
  if (!value) {
    return new Date();
  }
  const raw = String(value);
  return new Date(raw.includes("T") ? raw : `${raw}T12:00:00Z`);
}

function formatDay(day) {
  const date = typeof day === "string" ? day : day.toISOString().slice(0, 10);
  const [, month, dom] = date.split("-");
  return `${dom}.${month}`;
}

function weekdayIndex(day) {
  const jsDay = new Date(`${day}T12:00:00Z`).getUTCDay();
  return (jsDay + 6) % 7;
}

function teamLabel(tricode) {
  return `${TEAM_EMOJI[tricode] || ""} ${TEAM_RU[tricode] || tricode}`.trim();
}

function isFinalState(state) {
  return ["FINAL", "OFF"].includes(upper(state));
}

function isLiveishState(state) {
  return ["LIVE", "CRIT"].includes(upper(state));
}

function firstInt(...values) {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.trunc(value);
    }
    if (typeof value === "string" && value.trim()) {
      const parsed = Number.parseInt(value, 10);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return 0;
}

function upper(value) {
  return String(value || "").trim().toUpperCase();
}

function pluralRu(n, one, few, many) {
  const abs = Math.abs(n);
  if (abs % 100 >= 11 && abs % 100 <= 14) {
    return many;
  }
  if (abs % 10 === 1) {
    return one;
  }
  if (abs % 10 >= 2 && abs % 10 <= 4) {
    return few;
  }
  return many;
}

function envInt(value, fallback, minimum, maximum) {
  const parsed = Number.parseInt(String(value || ""), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(minimum, Math.min(maximum, parsed));
}

function envFlag(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  return ["1", "true", "yes", "y", "on"].includes(String(value).trim().toLowerCase());
}

function eventName(env, key, fallback) {
  return String(env[key] || fallback).trim() || fallback;
}

function queryBool(url, name, fallback) {
  if (!url.searchParams.has(name)) {
    return fallback;
  }
  return envFlag(url.searchParams.get(name), fallback);
}

function stripTrailingSlash(path) {
  if (path === "/") {
    return "";
  }
  return path.replace(/\/+$/, "");
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}
