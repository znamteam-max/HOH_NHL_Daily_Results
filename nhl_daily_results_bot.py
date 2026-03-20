import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from zoneinfo import ZoneInfo


API_BASE = "https://api-web.nhle.com"
PT_TZ = ZoneInfo("America/Los_Angeles")

STATE_DIR = Path("state")
STATE_PATH = STATE_DIR / "posted_games.json"

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

GAME_PK = (os.getenv("GAME_PK") or "").strip()
GAME_QUERY = (os.getenv("GAME_QUERY") or "").strip()
DEBUG_VERBOSE = (os.getenv("DEBUG_VERBOSE") or "0").strip() == "1"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; NHLSingleResultBot/1.0)",
        "Accept": "application/json",
    }
)


def log(*args: Any) -> None:
    print(*args, flush=True)


def dlog(*args: Any) -> None:
    if DEBUG_VERBOSE:
        print("[DBG]", *args, flush=True)


def api_get(path: str, timeout: int = 30) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def load_posted_state() -> Dict[str, Any]:
    ensure_state_dir()
    if not STATE_PATH.exists():
        return {"posted_games": {}}

    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"posted_games": {}}


def save_posted_state(state: Dict[str, Any]) -> None:
    ensure_state_dir()
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def current_hockey_day_pt() -> date:
    now_pt = datetime.now(PT_TZ)
    return now_pt.date() if now_pt.hour >= 6 else (now_pt.date() - timedelta(days=1))


def fetch_schedule_for_date(d: date) -> List[Dict[str, Any]]:
    js = api_get(f"/v1/schedule/{d.isoformat()}")
    games = js.get("games")
    if games is None:
        weeks = js.get("gameWeek") or []
        games = []
        for w in weeks:
            games.extend(w.get("games") or [])
    return games or []


def game_state(game: Dict[str, Any]) -> str:
    return (game.get("gameState") or game.get("gameStatus") or "").upper().strip()


def is_completed(game: Dict[str, Any]) -> bool:
    return game_state(game) in {"FINAL", "OFF", "FINAL_OT", "FINAL_SO"}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def team_abbrev(team_obj: Dict[str, Any]) -> str:
    abbr = team_obj.get("abbrev")
    if isinstance(abbr, str) and abbr.strip():
        return abbr.strip().upper()

    common = ((team_obj.get("commonName") or {}).get("default") or "").strip()
    place = ((team_obj.get("placeName") or {}).get("default") or "").strip()
    if place and common:
        return f"{place} {common}"
    return "TEAM"


def team_name_ru_fallback(team_obj: Dict[str, Any]) -> str:
    return team_abbrev(team_obj)


def get_game_id(game: Dict[str, Any]) -> str:
    for key in ("id", "gamePk", "gameId"):
        if key in game and str(game[key]).strip():
            return str(game[key]).strip()
    return ""


def get_boxscore(game_id: str) -> Dict[str, Any]:
    return api_get(f"/v1/gamecenter/{game_id}/boxscore")


def get_play_by_play(game_id: str) -> Dict[str, Any]:
    return api_get(f"/v1/gamecenter/{game_id}/play-by-play")


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip())


def player_name_from_obj(obj: Any) -> str:
    if isinstance(obj, str):
        return normalize_name(obj)

    if not isinstance(obj, dict):
        return ""

    for key in ("name", "fullName", "default"):
        val = obj.get(key)
        if isinstance(val, str) and val.strip():
            return normalize_name(val)

    first = obj.get("firstName")
    last = obj.get("lastName")
    if isinstance(first, dict):
        first = first.get("default")
    if isinstance(last, dict):
        last = last.get("default")
    full = f"{first or ''} {last or ''}".strip()
    if full:
        return normalize_name(full)

    return ""


def build_player_map(boxscore: Dict[str, Any]) -> Dict[str, str]:
    result: Dict[str, str] = {}

    def ingest(side_key: str) -> None:
        side = boxscore.get(side_key) or {}
        for group_key in ("forwards", "defense", "goalies", "scratches"):
            for p in side.get(group_key) or []:
                pid = p.get("playerId") or p.get("id")
                name = player_name_from_obj(p.get("name")) or player_name_from_obj(p)
                if pid and name:
                    result[str(pid)] = name

    ingest("awayTeam")
    ingest("homeTeam")
    return result


def display_team_name(team_obj: Dict[str, Any]) -> str:
    place = ((team_obj.get("placeName") or {}).get("default") or "").strip()
    common = ((team_obj.get("commonName") or {}).get("default") or "").strip()
    if place and common:
        return f"«{place}»"
    return f"«{team_abbrev(team_obj)}»"


def final_record_text(boxscore: Dict[str, Any], side_key: str) -> str:
    side = boxscore.get(side_key) or {}
    record = side.get("record") or side.get("teamRecord") or {}
    wins = record.get("wins")
    losses = record.get("losses")
    otl = record.get("otLosses")
    if wins is None or losses is None or otl is None:
        return ""
    return f"({wins}-{losses}-{otl})"


def final_header(boxscore: Dict[str, Any]) -> List[str]:
    away = boxscore.get("awayTeam") or {}
    home = boxscore.get("homeTeam") or {}

    away_name = display_team_name(away)
    home_name = display_team_name(home)

    away_score = safe_int(away.get("score"))
    home_score = safe_int(home.get("score"))

    away_record = final_record_text(boxscore, "awayTeam")
    home_record = final_record_text(boxscore, "homeTeam")

    return [
        f"{home_name}: {home_score} {home_record}".rstrip(),
        f"{away_name}: {away_score} {away_record}".rstrip(),
    ]


def period_label(num: int) -> str:
    mapping = {
        1: "1-й период",
        2: "2-й период",
        3: "3-й период",
        4: "Овертайм",
    }
    return mapping.get(num, f"{num}-й период")


def format_mmss(raw: Any) -> str:
    s = str(raw or "").strip()
    if not s:
        return "00.00"
    return s.replace(":", ".")


def extract_assists(details: Dict[str, Any], player_map: Dict[str, str]) -> List[str]:
    assists: List[str] = []

    # Частые варианты
    for key in ("assist1PlayerId", "assist2PlayerId", "assist3PlayerId"):
        pid = details.get(key)
        if pid:
            name = player_map.get(str(pid), "")
            if name:
                assists.append(name)

    for key in ("assist1Name", "assist2Name", "assist3Name"):
        name = details.get(key)
        if isinstance(name, str) and name.strip():
            assists.append(normalize_name(name))

    # Иногда NHL отдаёт массив
    for arr_key in ("assists", "assistors"):
        arr = details.get(arr_key)
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, dict):
                    pid = item.get("playerId") or item.get("id")
                    name = (
                        player_name_from_obj(item.get("name"))
                        or player_name_from_obj(item)
                        or player_map.get(str(pid), "")
                    )
                    if name:
                        assists.append(name)
                elif isinstance(item, str) and item.strip():
                    assists.append(normalize_name(item))

    deduped: List[str] = []
    seen = set()
    for a in assists:
        aa = normalize_name(a)
        if aa and aa not in seen:
            seen.add(aa)
            deduped.append(aa)
    return deduped[:2]


def format_assists(assists: List[str]) -> str:
    clean = [normalize_name(a) for a in assists if normalize_name(a)]
    if not clean:
        return ""
    return f" ({', '.join(clean)})"


def find_scorer(details: Dict[str, Any], player_map: Dict[str, str]) -> str:
    for key in ("scoringPlayerId", "goalScorerPlayerId", "shootingPlayerId", "playerId"):
        pid = details.get(key)
        if pid and str(pid) in player_map:
            return player_map[str(pid)]

    for key in ("scoringPlayerName", "goalScorerName", "shootingPlayerName", "playerName"):
        val = details.get(key)
        if isinstance(val, str) and val.strip():
            return normalize_name(val)

    return ""


def event_type_name(play: Dict[str, Any]) -> str:
    return (
        str(play.get("typeDescKey") or play.get("eventTypeId") or play.get("eventType") or "")
        .upper()
        .strip()
    )


def parse_goal_events(
    pbp: Dict[str, Any], player_map: Dict[str, str]
) -> Tuple[List[Tuple[int, str]], Optional[Tuple[str, str]]]:
    plays = pbp.get("plays") or pbp.get("gameEvents") or []
    score_events: List[Tuple[int, str]] = []
    winning_shootout: Optional[Tuple[str, str]] = None

    current_home = 0
    current_away = 0

    home_id = str(((pbp.get("homeTeam") or {}).get("id")) or "")
    away_id = str(((pbp.get("awayTeam") or {}).get("id")) or "")

    for play in plays:
        et = event_type_name(play)
        details = play.get("details") or {}
        period = safe_int(play.get("periodDescriptor", {}).get("number") or play.get("period"))

        # Пытаемся понять, это гол
        is_goal = et in {"GOAL", "SHOT_ON_GOAL"} and (
            details.get("reason") == "goal"
            or details.get("isGoal")
            or et == "GOAL"
        )

        # У shootout goal свои нюансы
        is_shootout = (
            play.get("periodDescriptor", {}).get("periodType") == "SO"
            or period >= 5
            or et in {"SHOOTOUT_COMPLETE", "SHOOTOUT"}  # запасной вариант
            or bool(details.get("isShootout"))
        )

        if is_goal and not is_shootout:
            owner_team_id = str(details.get("eventOwnerTeamId") or details.get("teamId") or "")
            if owner_team_id and owner_team_id == home_id:
                current_home += 1
            elif owner_team_id and owner_team_id == away_id:
                current_away += 1
            else:
                # fallback по встроенному счёту события
                current_home = safe_int(details.get("homeScore"), current_home)
                current_away = safe_int(details.get("awayScore"), current_away)

            clock = format_mmss(
                play.get("timeInPeriod")
                or play.get("timeRemaining")
                or details.get("timeInPeriod")
                or "00:00"
            )
            scorer = find_scorer(details, player_map) or "Игрок"
            assists = extract_assists(details, player_map)
            line = f"{current_home}:{current_away} – {clock} {scorer}{format_assists(assists)}"
            score_events.append((period, line))
            continue

        # Победный буллит
        if is_shootout:
            flag = (
                details.get("isGameWinningGoal")
                or details.get("isWinningGoal")
                or details.get("isGameDecidingGoal")
                or details.get("gameWinningGoal")
            )
            scored = (
                details.get("scored")
                or details.get("isGoal")
                or details.get("result") == "goal"
                or details.get("shotResult") == "goal"
            )
            if flag or (scored and details.get("decidingGoal")):
                scorer = find_scorer(details, player_map) or "Игрок"
                # Итоговый счёт уже финальный, возьмём из pbp
                home_final = safe_int(((pbp.get("homeTeam") or {}).get("score")))
                away_final = safe_int(((pbp.get("awayTeam") or {}).get("score")))
                winning_shootout = (f"{home_final}:{away_final}", scorer)

    return score_events, winning_shootout


def build_match_text(game_id: str) -> str:
    boxscore = get_boxscore(game_id)
    pbp = get_play_by_play(game_id)
    player_map = build_player_map(boxscore)

    lines: List[str] = []
    lines.extend(final_header(boxscore))
    lines.append("")

    goal_events, winning_shootout = parse_goal_events(pbp, player_map)

    grouped: Dict[int, List[str]] = {}
    for period_num, line in goal_events:
        grouped.setdefault(period_num, []).append(line)

    for period_num in sorted(grouped.keys()):
        lines.append(period_label(period_num))
        for item in grouped[period_num]:
            lines.append(item)
        lines.append("")

    if winning_shootout:
        final_score, scorer = winning_shootout
        lines.append("Победный буллит")
        lines.append(f"{final_score} – {scorer}")

    # Уберём хвостовые пустые строки
    while lines and not lines[-1].strip():
        lines.pop()

    return "\n".join(lines).strip()


def tg_send_message(text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    r = SESSION.post(url, json=payload, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"Telegram sendMessage failed: {js}")


def normalize_query_match(s: str) -> str:
    return re.sub(r"\s+", "", s.upper())


def game_matches_query(game: Dict[str, Any], query: str) -> bool:
    q = query.strip().upper()
    if not q:
        return False

    game_id = get_game_id(game)
    if game_id and q == game_id:
        return True

    away = team_abbrev(game.get("awayTeam") or {})
    home = team_abbrev(game.get("homeTeam") or {})
    game_date = str(game.get("gameDate") or "")

    compact = normalize_query_match(q)
    candidates = {
        normalize_query_match(f"{game_date}{home}-{away}"),
        normalize_query_match(f"{game_date}{away}@{home}"),
        normalize_query_match(f"{home}-{away}"),
        normalize_query_match(f"{away}@{home}"),
        normalize_query_match(f"{away}-{home}"),
        normalize_query_match(f"{home}@{away}"),
    }
    return compact in candidates


def resolve_manual_game() -> Optional[Dict[str, Any]]:
    if GAME_PK:
        target = GAME_PK
        dlog("Resolving manual GAME_PK:", target)
        d = current_hockey_day_pt()
        # ищем сначала в текущем дне, потом рядом
        for delta in range(-2, 3):
            games = fetch_schedule_for_date(d + timedelta(days=delta))
            for g in games:
                if get_game_id(g) == target:
                    return g
        return None

    if GAME_QUERY:
        dlog("Resolving manual GAME_QUERY:", GAME_QUERY)
        m = re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s+(.+?)\s*-\s*(.+?)\s*$", GAME_QUERY)
        if m:
            qdate = date.fromisoformat(m.group(1))
            games = fetch_schedule_for_date(qdate)
            for g in games:
                if game_matches_query(g, GAME_QUERY):
                    return g
        else:
            d = current_hockey_day_pt()
            for delta in range(-2, 3):
                games = fetch_schedule_for_date(d + timedelta(days=delta))
                for g in games:
                    if game_matches_query(g, GAME_QUERY):
                        return g
        return None

    return None


def post_manual_game(game: Dict[str, Any]) -> int:
    gid = get_game_id(game)
    if not gid:
        log("Manual game found but without game id")
        return 1

    st = game_state(game)
    if not is_completed(game):
        away = team_abbrev(game.get("awayTeam") or {})
        home = team_abbrev(game.get("homeTeam") or {})
        if st in {"PRE", "FUT", "SCHED"}:
            msg = f"{away} - {home} ещё не началась"
        else:
            msg = f"{away} - {home} ещё не завершилась"
        tg_send_message(msg)
        log("Posted pending status for manual game:", gid, st)
        return 0

    text = build_match_text(gid)
    tg_send_message(text)
    log("Posted manual completed game:", gid)
    return 0


def auto_post_new_completed_games() -> int:
    state = load_posted_state()
    posted_games = state.setdefault("posted_games", {})

    hockey_day = current_hockey_day_pt()
    games = fetch_schedule_for_date(hockey_day)

    final_games = [g for g in games if is_completed(g)]
    final_ids = [get_game_id(g) for g in final_games]
    dlog("PT hockey day:", hockey_day.isoformat())
    dlog("FINAL games:", final_ids)

    need_to_post: List[Dict[str, Any]] = []
    for g in final_games:
        gid = get_game_id(g)
        if gid and gid not in posted_games:
            need_to_post.append(g)

    log("FINAL games:", final_ids)
    log("Need to post:", [get_game_id(g) for g in need_to_post])

    changed = False
    for g in need_to_post:
        gid = get_game_id(g)
        if not gid:
            continue

        try:
            text = build_match_text(gid)
            tg_send_message(text)
            posted_games[gid] = {
                "posted_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                "hockey_day_pt": hockey_day.isoformat(),
            }
            changed = True
            log("Posted:", gid)
        except Exception as e:
            log(f"Failed to post game {gid}: {e}")

    if changed:
        save_posted_state(state)

    return 0


def main() -> int:
    if GAME_PK or GAME_QUERY:
        game = resolve_manual_game()
        if not game:
            tg_send_message("Матч не найден.")
            log("Manual game not found")
            return 1
        return post_manual_game(game)

    return auto_post_new_completed_games()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.HTTPError as e:
        log("HTTP error:", e)
        if e.response is not None:
            try:
                log("Response:", e.response.text[:2000])
            except Exception:
                pass
        raise
    except Exception as e:
        log("Fatal error:", e)
        raise
