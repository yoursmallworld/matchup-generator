"""
MaxPreps Schedule Scraper
Fetches upcoming varsity game schedules for specified schools by parsing
the __NEXT_DATA__ JSON embedded in MaxPreps schedule pages.
"""
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Cache of school addresses keyed by slug
_address_cache = {}

# Schools config: slug, display name, gender filter
SCHOOLS = [
    {"slug": "concord-bears", "name": "Concord", "genders": ["boys", "girls"]},
    {"slug": "ygnacio-valley-warriors", "name": "Ygnacio Valley", "genders": ["boys", "girls"]},
    {"slug": "mt-diablo-red-devils", "name": "Mt. Diablo", "genders": ["boys", "girls"]},
    {"slug": "clayton-valley-charter-ugly-eagles", "name": "Clayton Valley Charter", "genders": ["boys", "girls"]},
    {"slug": "de-la-salle-spartans", "name": "De La Salle", "genders": ["boys"]},
    {"slug": "carondelet-cougars", "name": "Carondelet", "genders": ["girls"]},
]

# Spring sports to check (sport_slug, gender, url_suffix)
SPRING_SPORTS = [
    ("baseball", "boys", "baseball/schedule/"),
    ("softball", "girls", "softball/schedule/"),
    ("volleyball", "boys", "volleyball/boys/schedule/"),
    ("lacrosse", "boys", "lacrosse/boys/schedule/"),
    ("lacrosse", "girls", "lacrosse/girls/schedule/"),
]

# Fall sports (for when the season changes)
FALL_SPORTS = [
    ("football", "boys", "football/schedule/"),
    ("volleyball", "girls", "volleyball/girls/schedule/"),
    ("water-polo", "boys", "water-polo/boys/schedule/"),
    ("water-polo", "girls", "water-polo/girls/schedule/"),
]

# Winter sports
WINTER_SPORTS = [
    ("basketball", "boys", "basketball/boys/schedule/"),
    ("basketball", "girls", "basketball/girls/schedule/"),
    ("soccer", "boys", "soccer/boys/schedule/"),
    ("soccer", "girls", "soccer/girls/schedule/"),
    ("wrestling", "boys", "wrestling/schedule/"),
]


def get_current_sports():
    """Return the sports list based on current month."""
    month = datetime.now().month
    if month in [8, 9, 10, 11]:  # Aug-Nov
        return FALL_SPORTS + SPRING_SPORTS  # Include overlap
    elif month in [12, 1, 2, 3]:  # Dec-Mar
        return WINTER_SPORTS + SPRING_SPORTS
    else:  # Apr-Jul
        return SPRING_SPORTS


def parse_opponent_from_url(url, school_slug):
    """Extract opponent name from the game URL.
    URL format: /games/MM-DD-YYYY/sport/team1-vs-team2.htm
    """
    try:
        match = re.search(r'/games/[\d-]+/[\w-]+/([\w-]+)-vs-([\w-]+)\.htm', url)
        if match:
            team1 = match.group(1)
            team2 = match.group(2)
            # Figure out which is opponent
            school_base = school_slug.split('-')[0]  # e.g., "concord" from "concord-bears"
            if school_base in team1:
                return team2.replace('-', ' ').title()
            else:
                return team1.replace('-', ' ').title()
    except Exception:
        pass
    return None


def parse_opponent_from_description(desc, school_name):
    """Extract opponent from the description field.
    e.g., 'The Concord varsity baseball team has an away conference game @ Berean Christian (Walnut Creek, CA)...'
    """
    if not desc:
        return None
    # Match "@ TeamName (City" for away or "vs. TeamName (City" / "vs TeamName" for home
    away_match = re.search(r'@\s+(.+?)\s*\(', desc)
    if away_match:
        return away_match.group(1).strip()
    # For home games: "The OPPONENT varsity SPORT team has an away"
    home_match = re.search(r'^The\s+(.+?)\s+varsity', desc)
    if home_match:
        opp = home_match.group(1).strip()
        if opp.lower() != school_name.lower():
            return opp
    return None


def extract_address_from_next_data(data):
    """Extract school address from __NEXT_DATA__ JSON.
    Looks in teamContext.data and schoolContext.schoolInfo.
    Returns formatted address string or None.
    """
    props = data.get("props", {}).get("pageProps", {})

    # Try teamContext.data (schedule pages)
    team_ctx = props.get("teamContext", {})
    if isinstance(team_ctx, dict):
        team_data = team_ctx.get("data", {})
        if isinstance(team_data, dict):
            addr = team_data.get("schoolAddress", "")
            city = team_data.get("schoolCity", "")
            state = team_data.get("schoolState", "CA")
            zipcode = team_data.get("schoolZipCode", "")
            if addr and city:
                return f"{addr}, {city}, {state} {zipcode}".strip()

    # Try schoolContext.schoolInfo (school main pages)
    school_ctx = props.get("schoolContext", {})
    if isinstance(school_ctx, dict):
        school_info = school_ctx.get("schoolInfo", {})
        if isinstance(school_info, dict):
            addr = school_info.get("address", "")
            city = school_info.get("city", "")
            state = school_info.get("state", "CA")
            zipcode = school_info.get("zip", "")
            if addr and city:
                return f"{addr}, {city}, {state} {zipcode}".strip()

    return None


def fetch_school_address(school_url):
    """Fetch a school's address from their MaxPreps page.
    school_url should be like: https://www.maxpreps.com/ca/concord/concord-bears/baseball/
    We'll derive the school main page from it.
    Returns address string or None.
    """
    # Derive school main page URL from sport URL
    # e.g., /ca/concord/concord-bears/baseball/ -> /ca/concord/concord-bears/
    try:
        # Extract the slug from the URL
        match = re.search(r'maxpreps\.com(/[a-z]{2}/[\w-]+/[\w-]+)', school_url)
        if not match:
            return None
        base_path = match.group(1)
        slug = base_path.split('/')[-1]

        # Check cache first
        if slug in _address_cache:
            return _address_cache[slug]

        # Try the schedule page URL first (already fetched, address available)
        schedule_url = f"https://www.maxpreps.com{base_path}/football/schedule/"
        resp = requests.get(schedule_url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            script = soup.find("script", id="__NEXT_DATA__")
            if script:
                data = json.loads(script.string)
                address = extract_address_from_next_data(data)
                if address:
                    _address_cache[slug] = address
                    return address

        # Fallback: try the main school page
        main_url = f"https://www.maxpreps.com{base_path}/"
        resp = requests.get(main_url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            script = soup.find("script", id="__NEXT_DATA__")
            if script:
                data = json.loads(script.string)
                address = extract_address_from_next_data(data)
                if address:
                    _address_cache[slug] = address
                    return address

    except Exception:
        pass

    return None


def get_opponent_url_from_contest(contest, school_slug):
    """Extract the opponent's MaxPreps URL from a contest entry.
    Contest field 0 contains array of 2 teams, each with URLs.
    Fields 37 and 38 may also contain individual team URLs.
    """
    # Try fields 37 and 38 first (simpler)
    for field_idx in [37, 38]:
        url = safe_get(contest, field_idx)
        if url and isinstance(url, str) and "maxpreps.com" in url:
            if school_slug not in url:
                return url

    # Try field 0 (array of 2 teams)
    teams = safe_get(contest, 0)
    if isinstance(teams, list):
        for team in teams:
            if isinstance(team, list):
                # Look for a URL string in the team data
                for item in team:
                    if isinstance(item, str) and "maxpreps.com" in item and school_slug not in item:
                        return item
            elif isinstance(team, dict):
                for val in team.values():
                    if isinstance(val, str) and "maxpreps.com" in val and school_slug not in val:
                        return val

    return None


def fetch_schedule(school_slug, sport_url):
    """Fetch and parse a single schedule page, returning game data."""
    url = f"https://www.maxpreps.com/ca/concord/{school_slug}/{sport_url}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return None, "not_found"
        if resp.status_code != 200:
            return None, f"http_{resp.status_code}"

        soup = BeautifulSoup(resp.text, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        if not script:
            return None, "no_next_data"

        data = json.loads(script.string)
        page_props = data.get("props", {}).get("pageProps", {})
        contests = page_props.get("contests", [])
        tracking = page_props.get("tracking", {})

        gender = tracking.get("gender", "")
        sport = tracking.get("sportName", "")

        # Extract this school's address from the page
        school_address = extract_address_from_next_data(data)
        if school_address and school_slug not in _address_cache:
            _address_cache[school_slug] = school_address

        return {
            "contests": contests,
            "gender": gender,
            "sport": sport,
            "url": url,
            "school_address": school_address,
        }, "ok"

    except requests.exceptions.Timeout:
        return None, "timeout"
    except requests.exceptions.ConnectionError:
        return None, "connection_error"
    except json.JSONDecodeError:
        return None, "json_error"
    except Exception as e:
        return None, f"error: {str(e)}"


def safe_get(obj, key, default=None):
    """Get a value from a list or dict by index/key."""
    try:
        if isinstance(obj, list):
            if isinstance(key, int) and 0 <= key < len(obj):
                val = obj[key]
                return val if val is not None else default
            return default
        elif isinstance(obj, dict):
            return obj.get(key, obj.get(str(key), default))
        return default
    except (IndexError, KeyError, TypeError):
        return default


def extract_games(schedule_data, school_name, school_slug):
    """Extract future games from parsed schedule data."""
    if not schedule_data:
        return []

    contests = schedule_data["contests"]
    gender = schedule_data["gender"]
    sport = schedule_data["sport"]
    school_address = schedule_data.get("school_address", "")
    now = datetime.now()
    games = []

    for c in contests:
        # Field mapping from __NEXT_DATA__:
        # c[11] = datetime (ISO), c[15] = home(1)/away(2)
        # c[17] = isLeague, c[18] = game URL, c[21] = type
        # c[28] = state description, c[29] = human description

        game_dt_str = safe_get(c, 11)
        if not game_dt_str:
            continue

        try:
            game_dt = datetime.fromisoformat(str(game_dt_str))
        except (ValueError, TypeError):
            continue

        # Skip past games
        if game_dt < now - timedelta(hours=3):
            continue

        # Skip non-game entries
        state = str(safe_get(c, 28, ""))
        if "Pregame" not in state and "InProgress" not in state:
            # Check if it's a future game without state info
            if game_dt < now:
                continue

        home_away_code = safe_get(c, 15, 0)
        is_home = home_away_code == 1
        is_away = home_away_code == 2
        home_away = "Home" if is_home else ("Away" if is_away else "")

        game_url = safe_get(c, 18, "")
        description = safe_get(c, 29, "")

        # Get opponent name
        opponent = parse_opponent_from_url(game_url, school_slug)
        if not opponent:
            opponent = parse_opponent_from_description(description, school_name)
        if not opponent:
            opponent = "TBA"

        # Determine venue address (address of the hosting school)
        venue = ""
        if is_home:
            # We're hosting — use our school's address
            venue = school_address or _address_cache.get(school_slug, "")
        elif is_away:
            # Opponent is hosting — try to get their address
            opp_url = get_opponent_url_from_contest(c, school_slug)
            if opp_url:
                venue = fetch_school_address(opp_url) or ""

        # Build title in the format the matchup generator expects
        if is_away:
            title = f"{gender} {sport}: {school_name} at {opponent}"
        else:
            title = f"{gender} {sport}: {opponent} at {school_name}"

        try:
            time_str = game_dt.strftime("%-I:%M%p").lower()
        except ValueError:
            time_str = game_dt.strftime("%I:%M%p").lstrip("0").lower()

        try:
            date_display = game_dt.strftime("%-m/%-d/%Y")
        except ValueError:
            date_display = f"{game_dt.month}/{game_dt.day}/{game_dt.year}"

        games.append({
            "title": title,
            "school": school_name,
            "gender": gender,
            "sport": sport,
            "date": date_display,
            "date_sort": game_dt.strftime("%Y-%m-%d"),
            "time": time_str,
            "home_away": home_away,
            "opponent": opponent,
            "game_url": game_url,
            "is_league": bool(safe_get(c, 17)),
            "venue": venue,
        })

    return games


def scrape_all_schools(progress_callback=None):
    """Scrape schedules for all configured schools.
    progress_callback(school_name, sport, status) if provided.
    Returns (games_list, verification_log).
    """
    all_games = []
    log_entries = []
    sports_to_check = get_current_sports()
    errors = []

    total_checks = sum(
        len([s for s in sports_to_check if s[1] in school["genders"]])
        for school in SCHOOLS
    )
    current = 0

    for school in SCHOOLS:
        school_games = []
        for sport_slug, sport_gender, sport_url in sports_to_check:
            if sport_gender not in school["genders"]:
                continue

            current += 1
            if progress_callback:
                progress_callback(current, total_checks, school["name"], sport_slug)

            schedule, status = fetch_schedule(school["slug"], sport_url)

            if status == "not_found":
                log_entries.append(f"  {school['name']} {sport_slug} ({sport_gender}): No team page (404)")
                continue
            elif status != "ok":
                log_entries.append(f"  {school['name']} {sport_slug} ({sport_gender}): ERROR - {status}")
                errors.append(f"{school['name']} {sport_slug}: {status}")
                continue

            games = extract_games(schedule, school["name"], school["slug"])
            school_games.extend(games)
            log_entries.append(
                f"  {school['name']} {schedule['gender']} {schedule['sport']}: "
                f"{len(games)} upcoming games"
            )

        all_games.extend(school_games)

    # Sort by date
    all_games.sort(key=lambda g: g["date_sort"])

    # Build verification log
    verification = run_verification(all_games, errors, log_entries)

    return all_games, verification


def run_verification(games, scrape_errors, log_entries):
    """Run fact-check and verification on scraped data."""
    now = datetime.now()
    issues = []

    # 1. Cross-reference check: games between our schools should match
    our_schools = {s["name"].lower() for s in SCHOOLS}
    cross_ref = {}
    for g in games:
        opp_lower = g["opponent"].lower()
        # Check if opponent is one of our schools
        if any(s in opp_lower for s in our_schools):
            key = tuple(sorted([g["school"].lower(), opp_lower]))
            date_key = (key, g["date"], g["sport"].lower())
            if date_key not in cross_ref:
                cross_ref[date_key] = []
            cross_ref[date_key].append(g)

    for key, matches in cross_ref.items():
        if len(matches) == 2:
            g1, g2 = matches
            if g1["time"] != g2["time"]:
                issues.append(
                    f"TIME MISMATCH: {g1['school']} vs {g2['school']} on {g1['date']} "
                    f"({g1['sport']}): {g1['time']} vs {g2['time']}"
                )
            if g1["home_away"] == g2["home_away"]:
                issues.append(
                    f"HOME/AWAY CONFLICT: {g1['school']} and {g2['school']} on {g1['date']} "
                    f"({g1['sport']}): both listed as {g1['home_away']}"
                )

    # 2. Duplicate check
    seen = {}
    for g in games:
        dup_key = (g["school"], g["opponent"], g["date"], g["sport"])
        if dup_key in seen:
            issues.append(
                f"DUPLICATE: {g['school']} vs {g['opponent']} on {g['date']} ({g['sport']})"
            )
        seen[dup_key] = True

    # 3. Date sanity check
    for g in games:
        try:
            gd = datetime.strptime(g["date_sort"], "%Y-%m-%d")
            if gd < now - timedelta(days=1):
                issues.append(f"PAST DATE: {g['title']} on {g['date']}")
            if gd > now + timedelta(days=120):
                issues.append(f"FAR FUTURE: {g['title']} on {g['date']} (>120 days out)")
        except ValueError:
            issues.append(f"BAD DATE: {g['title']} - '{g['date']}'")

    # 4. Completeness check
    schools_with_games = {g["school"] for g in games}
    for s in SCHOOLS:
        if s["name"] not in schools_with_games:
            issues.append(f"NO GAMES: {s['name']} has zero upcoming games")

    # 5. Count check
    counts = {}
    for g in games:
        counts[g["school"]] = counts.get(g["school"], 0) + 1
    avg_count = sum(counts.values()) / max(len(counts), 1)
    for school, count in counts.items():
        if count < avg_count * 0.3:
            issues.append(
                f"LOW COUNT: {school} has only {count} games (avg is {avg_count:.0f})"
            )

    # Build verification report
    report = {
        "scrape_time": now.isoformat(),
        "total_games": len(games),
        "schools_scraped": len(SCHOOLS),
        "sports_found": list({f"{g['gender']} {g['sport']}" for g in games}),
        "games_per_school": counts,
        "scrape_errors": scrape_errors,
        "verification_issues": issues,
        "scrape_log": log_entries,
        "status": "CLEAN" if not issues else f"{len(issues)} ISSUE(S) FOUND",
    }
    return report


def load_cached_data(cache_path):
    """Load previously scraped data from cache file."""
    global _address_cache
    try:
        with open(cache_path, "r") as f:
            data = json.load(f)
        # Restore address cache from saved data
        saved_addrs = data.get("address_cache", {})
        if saved_addrs:
            _address_cache.update(saved_addrs)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_cached_data(cache_path, games, verification):
    """Save scraped data to cache file."""
    data = {
        "games": games,
        "verification": verification,
        "cached_at": datetime.now().isoformat(),
        "address_cache": _address_cache,
    }
    Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(data, f, indent=2)


def get_cache_age_hours(cache_path):
    """Return how many hours old the cache is, or None if no cache."""
    cached = load_cached_data(cache_path)
    if not cached or "cached_at" not in cached:
        return None
    try:
        cached_dt = datetime.fromisoformat(cached["cached_at"])
        age = datetime.now() - cached_dt
        return age.total_seconds() / 3600
    except (ValueError, TypeError):
        return None
