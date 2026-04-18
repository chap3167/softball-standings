#!/usr/bin/env python3
"""Scrape SBMSA 6U schedules, recompute standings + analytics, rewrite DATA/GAMES/ANALYTICS in index.html."""

import datetime
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict

INDEX_HTML = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "index.html")

LEAGUES = [
    ("Alo", "https://sbmsa.net/sites/sbmsa/schedule/674695/6U-Alo"),
    ("Finch", "https://sbmsa.net/sites/sbmsa/schedule/674696/6U-Finch"),
    ("Chamberlain", "https://sbmsa.net/sites/sbmsa/schedule/674697/6U-Chamberlain"),
]

SEASON_YEAR = 2026

_today_override = os.environ.get("TODAY_OVERRIDE")
if _today_override:
    TODAY = datetime.date.fromisoformat(_today_override)
else:
    TODAY = datetime.date.today()


def fetch(url, tries=3, backoff=2.0):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (softball-scraper)"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last = e
            if i < tries - 1:
                time.sleep(backoff * (i + 1))
    raise RuntimeError(f"fetch failed for {url}: {last}")


# Regex that extracts rows from the desktop ScheduleGrid (NOT the MobileScheduleGrid).
# We anchor on span IDs that contain StandingsResultsControl_ScheduleGrid_ctl00_ctlNN_<Field>.
# Each game has a consistent ctlNN across its fields.
ROW_FIELD_RE = re.compile(
    r'id="ctl00_ContentPlaceHolder1_StandingsResultsControl_ScheduleGrid_ctl00_ctl(\d+)_'
    r'(DateLabel|TimeLabel|HomeLabel|HomeScoreLabel|AwayLabel|AwayScoreLabel|ScheduleLabel)"'
    r'[^>]*>([^<]*)</span>',
    re.DOTALL,
)


def parse_schedule(html):
    """Return list of game dicts in source order.

    Each game: {"date": str, "time": str, "home": str, "away": str,
                "hs": str, "as": str, "loc": str}.
    Byes are preserved (time="Bye", away="", etc).
    """
    # Restrict to the desktop grid: everything before the MobileScheduleGrid anchor.
    mobile_idx = html.find("StandingsResultsControl_MobileScheduleGrid")
    if mobile_idx == -1:
        segment = html
    else:
        segment = html[:mobile_idx]

    rows = defaultdict(dict)  # ctlNN -> {field: text}
    order = []
    for m in ROW_FIELD_RE.finditer(segment):
        ctl, field, text = m.group(1), m.group(2), m.group(3)
        if ctl not in rows:
            order.append(ctl)
        rows[ctl][field] = _html_unescape(text.strip())

    games = []
    for ctl in order:
        r = rows[ctl]
        g = {
            "date": r.get("DateLabel", ""),
            "time": r.get("TimeLabel", ""),
            "home": r.get("HomeLabel", ""),
            "away": r.get("AwayLabel", ""),
            "hs": r.get("HomeScoreLabel", ""),
            "as": r.get("AwayScoreLabel", ""),
            "loc": r.get("ScheduleLabel", ""),
        }
        games.append(g)

    # Byes on sbmsa.net come with no date, grouped at the end of each week's
    # section. Assign each bye the date of the next dated game in source order;
    # trailing byes inherit the most recently seen date.
    next_dated = [None] * len(games)
    last_known = None
    for i in range(len(games) - 1, -1, -1):
        if games[i]["date"]:
            last_known = games[i]["date"]
        next_dated[i] = last_known
    prev_dated = None
    for i, g in enumerate(games):
        if g["date"]:
            prev_dated = g["date"]
        elif g["time"] == "Bye":
            g["date"] = next_dated[i] or prev_dated or ""
    return games


def _html_unescape(s):
    # Minimal HTML entity unescape
    return (s.replace("&amp;", "&")
             .replace("&lt;", "<")
             .replace("&gt;", ">")
             .replace("&quot;", '"')
             .replace("&#39;", "'")
             .replace("&nbsp;", " "))


# ---------- date handling ----------

DAY_PREFIX_RE = re.compile(r"^(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)\s+(\d{1,2})/(\d{1,2})$")


def parse_date_to_dt(date_str):
    """'Sat 4/18' -> datetime.date(2026, 4, 18). Returns None if unparseable."""
    if not date_str:
        return None
    m = DAY_PREFIX_RE.match(date_str.strip())
    if not m:
        return None
    mo, day = int(m.group(1)), int(m.group(2))
    try:
        return datetime.date(SEASON_YEAR, mo, day)
    except ValueError:
        return None


# ---------- game classification ----------

def is_bye(g):
    return (g.get("time", "").strip().lower() == "bye") or (not g.get("away") and not g.get("date"))


def is_played(g):
    """Played = both scores present and non-empty (ties allowed)."""
    return g["hs"] != "" and g["as"] != "" and not is_bye(g)


def is_future(g):
    return (not is_played(g)) and (not is_bye(g))


# ---------- per-team aggregation ----------

def compute_team_stats(league, games):
    """Build base DATA stats per team within a single league."""
    # Preserve deterministic order: first-seen order across home/away.
    seen = []
    seen_set = set()
    for g in games:
        for t in (g["home"], g["away"]):
            if t and t not in seen_set:
                seen_set.add(t)
                seen.append(t)
    teams = seen

    stats = {}
    for t in teams:
        stats[t] = {
            "W": 0, "L": 0, "T": 0, "RF": 0, "RA": 0, "GP": 0, "REM": 0,
            "HW": 0, "HL": 0, "HT": 0, "AW": 0, "AL": 0, "AT": 0,
            "H_RF": 0, "H_RA": 0, "A_RF": 0, "A_RA": 0,
            "_played": [],   # list of (date_obj, date_str, time, loc, opp, self_score, opp_score, is_home, result)
            "_remaining": [],  # (date_obj, date_str, time, loc, opp, is_home) — only real games, not byes
            "NEXT": None,
        }

    for g in games:
        if is_bye(g):
            continue
        home, away = g["home"], g["away"]
        if not home or not away:
            continue
        d = parse_date_to_dt(g["date"])

        if is_played(g):
            try:
                hs, as_ = int(g["hs"]), int(g["as"])
            except ValueError:
                continue
            # Home team
            stats[home]["GP"] += 1
            stats[home]["RF"] += hs
            stats[home]["RA"] += as_
            stats[home]["H_RF"] += hs
            stats[home]["H_RA"] += as_
            if hs > as_:
                stats[home]["W"] += 1; stats[home]["HW"] += 1; res_h = "W"
                stats[away]["L"] += 1; stats[away]["AL"] += 1; res_a = "L"
            elif hs < as_:
                stats[home]["L"] += 1; stats[home]["HL"] += 1; res_h = "L"
                stats[away]["W"] += 1; stats[away]["AW"] += 1; res_a = "W"
            else:
                stats[home]["T"] += 1; stats[home]["HT"] += 1; res_h = "T"
                stats[away]["T"] += 1; stats[away]["AT"] += 1; res_a = "T"
            # Away team
            stats[away]["GP"] += 1
            stats[away]["RF"] += as_
            stats[away]["RA"] += hs
            stats[away]["A_RF"] += as_
            stats[away]["A_RA"] += hs
            stats[home]["_played"].append((d, g["date"], g["time"], g["loc"], away, hs, as_, True, res_h))
            stats[away]["_played"].append((d, g["date"], g["time"], g["loc"], home, as_, hs, False, res_a))
        else:
            # Future game (not played, not bye)
            stats[home]["REM"] += 1
            stats[away]["REM"] += 1
            stats[home]["_remaining"].append((d, g["date"], g["time"], g["loc"], away, True))
            stats[away]["_remaining"].append((d, g["date"], g["time"], g["loc"], home, False))

    return stats


def compute_next(team_stats, today):
    """Pick the next unplayed game from today forward; fallback to earliest remaining."""
    rem = [r for r in team_stats["_remaining"] if r[0] is not None]
    # Prefer games on/after today, chronologically.
    future = [r for r in rem if r[0] >= today]
    chosen = None
    if future:
        future.sort(key=lambda r: (r[0], r[2]))
        chosen = future[0]
    elif rem:
        rem.sort(key=lambda r: (r[0], r[2]))
        chosen = rem[0]
    if not chosen:
        return None
    _, date_s, time_s, loc, opp, is_home = chosen
    return {
        "opp": opp,
        "ha": "vs" if is_home else "@",
        "date": date_s,
        "time": time_s,
        "loc": loc,
    }


def build_data(parsed):
    """Build the DATA dict matching existing shape."""
    data = {}
    for league, games in parsed.items():
        stats = compute_team_stats(league, games)
        data[league] = {}
        for team, s in stats.items():
            nxt = compute_next(s, TODAY)
            data[league][team] = {
                "W": s["W"], "L": s["L"], "T": s["T"],
                "RF": s["RF"], "RA": s["RA"],
                "GP": s["GP"], "REM": s["REM"],
                "NEXT": nxt,
            }
    return data


# ---------- analytics ----------

def round4(x):
    return round(x + 0.0, 4)


def round2(x):
    return round(x + 0.0, 2)


def pythag(rf, ra, exp=1.83):
    if rf == 0 and ra == 0:
        return 0.0
    num = rf ** exp
    den = num + (ra ** exp)
    return num / den if den else 0.0


def streak(played_sorted):
    """Given list sorted newest first, return 'W3' / 'L2' / 'T1'."""
    if not played_sorted:
        return ""
    last = played_sorted[0][8]  # result
    n = 0
    for rec in played_sorted:
        if rec[8] == last:
            n += 1
        else:
            break
    return f"{last}{n}"


def fmt_rec(w, l, t):
    return f"{w}-{l}" if t == 0 else f"{w}-{l}-{t}"


def build_analytics(parsed, data):
    # Build per-team stats objects and team PCT map.
    per_league_stats = {}
    for league, games in parsed.items():
        per_league_stats[league] = compute_team_stats(league, games)

    team_pct = {}  # (league, team) -> final PCT
    for league, stats in per_league_stats.items():
        for t, s in stats.items():
            gp = s["GP"]
            pct = (s["W"] + 0.5 * s["T"]) / gp if gp else 0.0
            team_pct[(league, t)] = round4(pct)

    teams_out = {}
    for league, stats in per_league_stats.items():
        for t, s in stats.items():
            gp = s["GP"]
            pct = (s["W"] + 0.5 * s["T"]) / gp if gp else 0.0
            pct_r = round4(pct)
            rfpg = round2(s["RF"] / gp) if gp else 0.0
            rapg = round2(s["RA"] / gp) if gp else 0.0
            pyth_r = round4(pythag(s["RF"], s["RA"]))

            # Streak (newest first)
            played_sorted = sorted(
                s["_played"],
                key=lambda r: (r[0] or datetime.date.min, r[2]),
                reverse=True,
            )
            sk = streak(played_sorted)
            last_date_s = played_sorted[0][1] if played_sorted else ""
            last_d = played_sorted[0][0] if played_sorted else None
            days_since = (TODAY - last_d).days if last_d else None

            # Next unplayed game from today forward
            nxt = compute_next(s, TODAY)
            next_date_s = nxt["date"] if nxt else ""
            next_d = parse_date_to_dt(next_date_s) if next_date_s else None
            days_until = (next_d - TODAY).days if next_d else None

            # Games next 7 days (today through today+7 inclusive-of-7? use strict < +7 days)
            window_end = TODAY + datetime.timedelta(days=7)
            games_next_7 = 0
            for r in s["_remaining"]:
                if r[0] and TODAY <= r[0] <= window_end:
                    games_next_7 += 1

            # Strength of schedule
            opp_pcts = [team_pct[(league, r[4])] for r in s["_played"] if (league, r[4]) in team_pct]
            sos = round4(sum(opp_pcts) / len(opp_pcts)) if opp_pcts else 0.0
            rem_opp_pcts = [team_pct[(league, r[4])] for r in s["_remaining"] if (league, r[4]) in team_pct]
            sos_r = round4(sum(rem_opp_pcts) / len(rem_opp_pcts)) if rem_opp_pcts else 0.0

            # POWER
            diffpg = (s["RF"] - s["RA"]) / gp if gp else 0.0
            # clamp (diffpg+10)/20 to [0,1]
            diff_norm = (diffpg + 10) / 20
            if diff_norm < 0:
                diff_norm = 0.0
            elif diff_norm > 1:
                diff_norm = 1.0
            power = 0.45 * pct + 0.20 * diff_norm + 0.20 * sos + 0.15 * pythag(s["RF"], s["RA"])
            power_r = round4(power)

            # VS_ABOVE / VS_BELOW — opponents above/at-or-below .500
            vs_above = {"W": 0, "L": 0, "T": 0}
            vs_below = {"W": 0, "L": 0, "T": 0}
            for rec in s["_played"]:
                opp = rec[4]; result = rec[8]
                opct = team_pct.get((league, opp), 0.0)
                bucket = vs_above if opct > 0.5 else vs_below
                bucket[result] += 1

            # BEST_WIN / WORST_LOSS
            wins = [r for r in s["_played"] if r[8] == "W"]
            losses = [r for r in s["_played"] if r[8] == "L"]
            best_win = None
            if wins:
                # Highest opp pct; tie-break on latest date
                wins_sorted = sorted(
                    wins,
                    key=lambda r: (team_pct.get((league, r[4]), 0.0), r[0] or datetime.date.min),
                    reverse=True,
                )
                bw = wins_sorted[0]
                best_win = {
                    "opp": bw[4],
                    "opp_pct": team_pct.get((league, bw[4]), 0.0),
                    "score": f"{bw[5]}-{bw[6]}",
                    "date": bw[1],
                }
            worst_loss = None
            if losses:
                losses_sorted = sorted(
                    losses,
                    key=lambda r: (team_pct.get((league, r[4]), 0.0), -(r[0].toordinal() if r[0] else 0)),
                )
                wl = losses_sorted[0]
                worst_loss = {
                    "opp": wl[4],
                    "opp_pct": team_pct.get((league, wl[4]), 0.0),
                    "score": f"{wl[5]}-{wl[6]}",
                    "date": wl[1],
                }

            teams_out[f"{league}|{t}"] = {
                "W": s["W"], "L": s["L"], "T": s["T"],
                "RF": s["RF"], "RA": s["RA"], "GP": s["GP"],
                "HW": s["HW"], "HL": s["HL"], "HT": s["HT"],
                "AW": s["AW"], "AL": s["AL"], "AT": s["AT"],
                "H_RF": s["H_RF"], "H_RA": s["H_RA"],
                "A_RF": s["A_RF"], "A_RA": s["A_RA"],
                "PCT": pct_r,
                "RFPG": rfpg, "RAPG": rapg,
                "PYTH": pyth_r,
                "STREAK": sk,
                "LAST_DATE": last_date_s,
                "DAYS_SINCE": days_since,
                "NEXT_DATE": next_date_s,
                "DAYS_UNTIL": days_until,
                "NEXT": nxt,
                "GAMES_NEXT_7": games_next_7,
                "REM": s["REM"],
                "SOS": sos,
                "SOS_R": sos_r,
                "POWER": power_r,
                "VS_ABOVE": vs_above,
                "VS_BELOW": vs_below,
                "BEST_WIN": best_win,
                "WORST_LOSS": worst_loss,
                "league": league,
                "team": t,
            }

    # Highlights: closest / blowout / highest (lists of 3)
    played_all = []
    for league, games in parsed.items():
        for g in games:
            if not is_played(g):
                continue
            try:
                hs, as_ = int(g["hs"]), int(g["as"])
            except ValueError:
                continue
            d = parse_date_to_dt(g["date"])
            played_all.append({
                "league": league,
                "date": g["date"],
                "time": g["time"],
                "loc": g["loc"],
                "home": g["home"],
                "away": g["away"],
                "hs": hs,
                "as": as_,
                "margin": abs(hs - as_),
                "total": hs + as_,
                "scoreboard": f"{g['away']} {as_} @ {g['home']} {hs}",
                "_d": d,
            })

    def strip_d(lst):
        out = []
        for x in lst:
            y = dict(x); y.pop("_d", None); out.append(y)
        return out

    closest = sorted(played_all, key=lambda x: (x["margin"], -x["total"], -(x["_d"].toordinal() if x["_d"] else 0)))[:3]
    blowout = sorted(played_all, key=lambda x: (-x["margin"], x["_d"].toordinal() if x["_d"] else 0))[:3]
    highest = sorted(played_all, key=lambda x: (-x["total"], -(x["_d"].toordinal() if x["_d"] else 0)))[:3]

    highlights = {
        "closest": strip_d(closest),
        "blowout": strip_d(blowout),
        "highest": strip_d(highest),
    }

    # Hero.dominant: best POWER across all leagues
    all_teams = list(teams_out.values())
    all_teams_sorted = sorted(all_teams, key=lambda t: t["POWER"], reverse=True)
    top_dom_list = [
        {
            "team": t["team"],
            "league": t["league"],
            "dom": t["POWER"],
            "rec": fmt_rec(t["W"], t["L"], t["T"]),
        }
        for t in all_teams_sorted[:5]
    ]
    if all_teams_sorted:
        top = all_teams_sorted[0]
        diffpg = (top["RF"] - top["RA"]) / top["GP"] if top["GP"] else 0.0
        dominant = {
            "league": top["league"],
            "team": top["team"],
            "rec": fmt_rec(top["W"], top["L"], top["T"]),
            "dom": top["POWER"],
            "rfpg": top["RFPG"],
            "diffpg": round2(diffpg),
            "sos": top["SOS"],
        }
    else:
        dominant = None

    # Hero.upset: played game where winner's final PCT is lowest vs loser
    best_upset = None
    for g in played_all:
        home, away, hs, as_ = g["home"], g["away"], g["hs"], g["as"]
        if hs == as_:
            continue
        if hs > as_:
            winner, loser, wscore, lscore = home, away, hs, as_
        else:
            winner, loser, wscore, lscore = away, home, as_, hs
        wpct = team_pct.get((g["league"], winner), 0.0)
        lpct = team_pct.get((g["league"], loser), 0.0)
        up = lpct - wpct
        if best_upset is None or up > best_upset["upset_score"]:
            best_upset = {
                "league": g["league"],
                "date": g["date"],
                "time": g["time"],
                "loc": g["loc"],
                "winner": winner,
                "loser": loser,
                "score": f"{wscore}-{lscore}",
                "winner_pct": wpct,
                "loser_pct": lpct,
                "upset_score": round4(up),
            }

    # Hero.game_of_week: filter upcoming games in [TODAY, next Sunday]
    # next Sunday = TODAY + (6 - TODAY.weekday()) days using Mon=0..Sun=6
    days_to_sun = (6 - TODAY.weekday()) % 7
    # If today is already Sunday, window is [today, today]
    window_end = TODAY + datetime.timedelta(days=days_to_sun)

    # Division leader counts per league by PCT
    div_rank = {}  # league -> sorted list of (pct, team)
    for league in parsed:
        ranks = sorted(
            [(teams_out[f"{league}|{t}"]["PCT"], t) for t in per_league_stats[league].keys()],
            reverse=True,
        )
        div_rank[league] = ranks

    candidates = []
    for league, games in parsed.items():
        for g in games:
            if not is_future(g):
                continue
            d = parse_date_to_dt(g["date"])
            if not d:
                continue
            if not (TODAY <= d <= window_end):
                continue
            home, away = g["home"], g["away"]
            if not home or not away:
                continue
            ht = teams_out.get(f"{league}|{home}")
            at = teams_out.get(f"{league}|{away}")
            if not ht or not at:
                continue
            hp = ht["POWER"]; ap = at["POWER"]
            avg_power = (hp + ap) / 2
            gap = abs(hp - ap)
            # division leader bonus: both in top 2 of their division
            ranks = div_rank[league]
            top2_teams = {name for _, name in ranks[:2]}
            both_top2 = (home in top2_teams) and (away in top2_teams)
            bonus = 0.10 if both_top2 else 0.0
            score_val = avg_power - 0.6 * gap + bonus
            candidates.append({
                "league": league,
                "date": g["date"],
                "time": g["time"],
                "loc": g["loc"],
                "home": home,
                "away": away,
                "home_power": hp,
                "away_power": ap,
                "home_rec": fmt_rec(ht["W"], ht["L"], ht["T"]),
                "away_rec": fmt_rec(at["W"], at["L"], at["T"]),
                "home_streak": ht["STREAK"],
                "away_streak": at["STREAK"],
                "score_val": round4(score_val),
                "division_lead": both_top2,
            })

    game_of_week = None
    if candidates:
        candidates.sort(key=lambda x: x["score_val"], reverse=True)
        game_of_week = candidates[0]

    # Hero.spotlight: prefer Aggies, else best POWER
    spotlight = None
    aggies_key = None
    for k in teams_out:
        if k.endswith("|Aggies"):
            aggies_key = k; break
    pick = teams_out.get(aggies_key) if aggies_key else None
    if pick is None and all_teams_sorted:
        pick = all_teams_sorted[0]
    if pick:
        diffpg = (pick["RF"] - pick["RA"]) / pick["GP"] if pick["GP"] else 0.0
        spotlight = {
            "league": pick["league"],
            "team": pick["team"],
            "rec": fmt_rec(pick["W"], pick["L"], pick["T"]),
            "streak": pick["STREAK"],
            "rfpg": pick["RFPG"],
            "diffpg": round2(diffpg),
            "power": pick["POWER"],
            "pct": pick["PCT"],
            "sos": pick["SOS"],
            "next_date": pick["NEXT_DATE"],
            "days_until": pick["DAYS_UNTIL"],
            "best_win": pick["BEST_WIN"],
        }

    return {
        "teams": teams_out,
        "highlights": highlights,
        "hero": {
            "dominant": dominant,
            "top_dom_list": top_dom_list,
            "upset": best_upset,
            "game_of_week": game_of_week,
            "spotlight": spotlight,
        },
        "rest_and_quality": {},
    }


# ---------- GAMES output ----------

def build_games(parsed):
    """Produce GAMES dict matching existing shape (string scores, include byes)."""
    out = {}
    for league, games in parsed.items():
        out[league] = [
            {
                "date": g["date"],
                "time": g["time"],
                "home": g["home"],
                "away": g["away"],
                "hs": g["hs"],
                "as": g["as"],
                "loc": g["loc"],
            }
            for g in games
        ]
    return out


# ---------- replacement ----------

def replace_const(html, name, payload_dict):
    pattern = rf'const {name}\s*=\s*\{{.*?\}};'
    payload = json.dumps(payload_dict, ensure_ascii=False)
    new_html, n = re.subn(pattern, lambda m: f'const {name} = {payload};', html, count=1, flags=re.DOTALL)
    if n == 0:
        raise RuntimeError(f"could not find 'const {name}' block to replace")
    return new_html


# ---------- main ----------

def main():
    try:
        parsed = {}
        for league, url in LEAGUES:
            html = fetch(url)
            games = parse_schedule(html)
            if not games:
                print(f"warning: zero games parsed for {league}", file=sys.stderr)
                sys.exit(1)
            parsed[league] = games

        data = build_data(parsed)
        games_out = build_games(parsed)
        analytics = build_analytics(parsed, data)
    except Exception as e:
        print(f"parse/compute error: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(2)

    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        original = f.read()

    try:
        updated = original
        updated = replace_const(updated, "DATA", data)
        updated = replace_const(updated, "GAMES", games_out)
        updated = replace_const(updated, "ANALYTICS", analytics)
    except Exception as e:
        print(f"replace error: {e}", file=sys.stderr)
        sys.exit(2)

    if updated == original:
        print("no changes")
        return 0

    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(updated)
    print("updated")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
