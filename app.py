import streamlit as st
from PIL import Image, ImageDraw, ImageFont
from pathlib import Path
from datetime import datetime, timedelta
import io
import base64
import zipfile
import re
import csv
import json
import threading
import time as time_module

# Configuration
BASE_DIR = Path(__file__).parent
LOGOS_DIR = BASE_DIR / "logos"
FONTS_DIR = BASE_DIR / "fonts"
CACHE_DIR = BASE_DIR / "cache"
CACHE_FILE = CACHE_DIR / "schedule_cache.json"
AUTO_REFRESH_HOURS = 24  # Re-scrape if cache is older than this

# Image dimensions (16:9)
IMAGE_WIDTH = 1200
IMAGE_HEIGHT = 675

WHITE = (255, 255, 255)
BLACK = (0, 0, 0)

SPORTS = [
    "basketball", "football", "soccer", "baseball", "volleyball",
    "softball", "tennis", "wrestling", "track", "swimming"
]

# Known school addresses for venue lookup
KNOWN_ADDRESSES = {
    "Concord": "4200 Concord Blvd, Concord, CA 94521",
    "Ygnacio Valley": "755 Oak Grove Rd, Concord, CA 94518",
    "Mt. Diablo": "2450 Grant St, Concord, CA 94520",
    "Clayton Valley Charter": "1101 Alberta Way, Concord, CA 94521",
    "Clayton Valley": "1101 Alberta Way, Concord, CA 94521",
    "De La Salle": "1130 Winton Dr, Concord, CA 94518",
    "Carondelet": "1133 Winton Dr, Concord, CA 94518",
}


def get_title_font(size):
    """Get Chunk Five font if available, else fallback."""
    chunk_paths = [
        FONTS_DIR / "ChunkFive-Regular.otf",
        FONTS_DIR / "ChunkFive.otf",
    ]
    for font_path in chunk_paths:
        if font_path.exists():
            try:
                return ImageFont.truetype(str(font_path), size)
            except:
                pass
    # Fallback
    fallback = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ]
    for f in fallback:
        if Path(f).exists():
            try:
                return ImageFont.truetype(f, size)
            except:
                pass
    return ImageFont.load_default()


def get_body_font(size):
    """Get font for body text."""
    fonts = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ]
    for f in fonts:
        if Path(f).exists():
            try:
                return ImageFont.truetype(f, size)
            except:
                pass
    return ImageFont.load_default()


def format_team_name(team_id):
    """Convert team_id to display name with special formatting."""
    name = team_id.replace("_", " ").replace("-", " ").title()
    name = name.replace("Mt ", "Mt. ")
    name = name.replace("Sicp", "SICP")
    name = name.replace("'S", "'s")
    name = name.replace("Justin Siena", "Justin-Siena")
    name = name.replace("St Patrick St Vincent", "St. Patrick-St. Vincent")
    # Shorten display names
    name = name.replace("Clayton Valley Charter", "Clayton Valley")
    return name


def get_available_teams():
    """Get list of teams that have logos."""
    teams = []
    if LOGOS_DIR.exists():
        for f in sorted(LOGOS_DIR.glob("*")):
            if f.suffix.lower() in [".png", ".jpg", ".jpeg"]:
                teams.append(f.stem)
    return teams


def find_logo(team_name):
    """Find a logo file for a team name."""
    if not LOGOS_DIR.exists():
        return None
    normalized = team_name.lower().replace(" ", "_").replace("-", "_")
    for logo_file in LOGOS_DIR.glob("*"):
        if logo_file.suffix.lower() in [".png", ".jpg", ".jpeg"]:
            file_normalized = logo_file.stem.lower().replace(" ", "_").replace("-", "_")
            if file_normalized == normalized or normalized in file_normalized or file_normalized in normalized:
                return logo_file
    return None


def load_and_resize_logo(logo_path, target_size=(175, 175)):
    """Load a logo and resize maintaining aspect ratio."""
    try:
        img = Image.open(logo_path)
        if img.mode != "RGBA":
            img = img.convert("RGBA")
        img.thumbnail(target_size, Image.Resampling.LANCZOS)
        result = Image.new("RGBA", target_size, (0, 0, 0, 0))
        x = (target_size[0] - img.width) // 2
        y = (target_size[1] - img.height) // 2
        result.paste(img, (x, y), img if img.mode == "RGBA" else None)
        return result
    except Exception as e:
        return create_placeholder_logo(logo_path.stem if hasattr(logo_path, 'stem') else "Team", target_size)


def create_placeholder_logo(team_name, size=(175, 175)):
    """Create a placeholder logo with team initials."""
    img = Image.new("RGBA", size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    padding = 15
    draw.ellipse(
        [padding, padding, size[0] - padding, size[1] - padding],
        fill=(70, 70, 80), outline=(100, 100, 110), width=4
    )
    words = team_name.replace("_", " ").replace("-", " ").split()
    initials = "".join(word[0].upper() for word in words[:3]) if words else "?"
    font = get_body_font(70)
    bbox = draw.textbbox((0, 0), initials, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    x = (size[0] - text_width) // 2
    y = (size[1] - text_height) // 2 - 10
    draw.text((x, y), initials, fill=WHITE, font=font)
    return img


def generate_matchup_graphic(home_team, away_team, sport, game_date, gender="Boys"):
    """Generate the matchup graphic."""
    img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), WHITE)
    draw = ImageDraw.Draw(img)

    # Square-safe zone
    SAFE_ZONE_SIZE = IMAGE_HEIGHT
    SAFE_ZONE_LEFT = (IMAGE_WIDTH - SAFE_ZONE_SIZE) // 2
    SAFE_ZONE_RIGHT = SAFE_ZONE_LEFT + SAFE_ZONE_SIZE
    SAFE_PADDING = 40
    CONTENT_LEFT = SAFE_ZONE_LEFT + SAFE_PADDING
    CONTENT_RIGHT = SAFE_ZONE_RIGHT - SAFE_PADDING
    CONTENT_WIDTH = CONTENT_RIGHT - CONTENT_LEFT

    # Title
    title_font = get_title_font(70)
    title_text = f"{gender} {sport.title()}"
    title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (IMAGE_WIDTH - title_width) // 2
    draw.text((title_x, 50), title_text, font=title_font, fill=BLACK)

    # Logos
    home_logo_path = find_logo(home_team)
    away_logo_path = find_logo(away_team)
    logo_size = (175, 175)
    home_logo = load_and_resize_logo(home_logo_path, logo_size) if home_logo_path else create_placeholder_logo(home_team, logo_size)
    away_logo = load_and_resize_logo(away_logo_path, logo_size) if away_logo_path else create_placeholder_logo(away_team, logo_size)

    logo_y = 155
    home_x = CONTENT_LEFT + (CONTENT_WIDTH // 5) - (logo_size[0] // 2)
    away_x = CONTENT_LEFT + (4 * CONTENT_WIDTH // 5) - (logo_size[0] // 2)

    img.paste(home_logo, (home_x, logo_y), home_logo)
    img.paste(away_logo, (away_x, logo_y), away_logo)

    # vs.
    vs_font = get_title_font(55)
    vs_bbox = draw.textbbox((0, 0), "vs.", font=vs_font)
    vs_width = vs_bbox[2] - vs_bbox[0]
    vs_height = vs_bbox[3] - vs_bbox[1]
    vs_x = IMAGE_WIDTH // 2 - vs_width // 2
    vs_y = logo_y + logo_size[1] // 2 - vs_height // 2
    draw.text((vs_x, vs_y), "vs.", font=vs_font, fill=BLACK)

    # Team names — auto-shrink to fit, both names use the same font size
    max_name_width = CONTENT_WIDTH // 2 - 20

    home_name = format_team_name(home_team)
    away_name = format_team_name(away_team)

    # Find the smallest font size needed for either name
    font_size = 42
    while font_size > 20:
        team_font = get_title_font(font_size)
        home_w = draw.textbbox((0, 0), home_name, font=team_font)[2] - draw.textbbox((0, 0), home_name, font=team_font)[0]
        away_w = draw.textbbox((0, 0), away_name, font=team_font)[2] - draw.textbbox((0, 0), away_name, font=team_font)[0]
        if home_w <= max_name_width and away_w <= max_name_width:
            break
        font_size -= 2

    team_font = get_title_font(font_size)

    home_bbox = draw.textbbox((0, 0), home_name, font=team_font)
    home_text_width = home_bbox[2] - home_bbox[0]
    home_logo_center = home_x + logo_size[0] // 2
    home_text_x = home_logo_center - home_text_width // 2
    draw.text((home_text_x, logo_y + logo_size[1] + 15), home_name, font=team_font, fill=BLACK)

    away_bbox = draw.textbbox((0, 0), away_name, font=team_font)
    away_text_width = away_bbox[2] - away_bbox[0]
    away_logo_center = away_x + logo_size[0] // 2
    away_text_x = away_logo_center - away_text_width // 2
    draw.text((away_text_x, logo_y + logo_size[1] + 15), away_name, font=team_font, fill=BLACK)

    # Date
    date_font = get_title_font(40)
    try:
        parsed_date = datetime.strptime(game_date, "%Y-%m-%d")
        formatted_date = parsed_date.strftime("%A, %B %-d")
    except:
        try:
            parsed_date = datetime.strptime(game_date, "%Y-%m-%d")
            formatted_date = parsed_date.strftime("%A, %B %d").replace(" 0", " ")
        except:
            formatted_date = game_date

    date_bbox = draw.textbbox((0, 0), formatted_date, font=date_font)
    date_width = date_bbox[2] - date_bbox[0]
    date_height = date_bbox[3] - date_bbox[1]
    date_x = IMAGE_WIDTH // 2 - date_width // 2
    draw.text((date_x, IMAGE_HEIGHT - 140 - date_height), formatted_date, font=date_font, fill=BLACK)

    return img


def parse_title(title):
    """Parse a title like 'Boys Basketball: Concord at Ygnacio Valley' into components.
    Returns (gender, sport, away_team, home_team) or None if can't parse.

    Title format: '{Gender} {Sport}: {Away Team} at {Home Team}'
    Also handles: 'Girls Soccer: Mt. Diablo at Concord'
    """
    # Match pattern: Gender Sport: Team1 at Team2
    match = re.match(
        r"^(Boys|Girls)\s+(.+?):\s+(.+?)\s+at\s+(.+?)$",
        title.strip(),
        re.IGNORECASE
    )
    if match:
        gender = match.group(1).title()
        sport = match.group(2).strip()
        away_team = match.group(3).strip()
        home_team = match.group(4).strip()
        return gender, sport, away_team, home_team
    return None


def team_name_to_id(name):
    """Convert a display team name to a logo-friendly ID.
    e.g., 'Ygnacio Valley' -> 'ygnacio_valley'
    """
    return name.lower().strip().replace(" ", "_").replace(".", "").replace("'", "")


def parse_date_from_string(date_str):
    """Parse various date formats from the spreadsheet."""
    if not date_str or str(date_str).strip() == "":
        return None
    date_str = str(date_str).strip()
    # Try common formats
    formats = [
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # Try pandas-style if it looks like a timestamp
    try:
        # Handle "1/26/2026 22:00:00" style
        parts = date_str.split()
        if len(parts) >= 1:
            date_part = parts[0]
            for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]:
                try:
                    return datetime.strptime(date_part, fmt)
                except ValueError:
                    continue
    except:
        pass
    return None


def parse_csv_data(text):
    """Parse CSV/TSV text into rows of dicts."""
    lines = text.strip().split("\n")
    if not lines:
        return []

    # Detect delimiter
    first_line = lines[0]
    if "\t" in first_line:
        delimiter = "\t"
    elif "," in first_line:
        delimiter = ","
    else:
        delimiter = ","

    reader = csv.DictReader(lines, delimiter=delimiter)
    rows = []
    for row in reader:
        rows.append(row)
    return rows


# ========== STREAMLIT APP ==========

st.set_page_config(page_title="Matchup Generator", page_icon="🏆", layout="centered")

st.title("🏆 Matchup Generator")
st.caption("Generate sports matchup graphics for Smallworld Concord")

# Get available teams from logos folder
available_teams = get_available_teams()
team_display = {t: format_team_name(t) for t in available_teams}

# Add option for custom team name
team_options = sorted(team_display.values())

tab1, tab2, tab4, tab3 = st.tabs(["Generate Graphic", "Batch Generate", "Upcoming Games", "Manage Logos"])

with tab1:
    col1, col2 = st.columns(2)

    with col1:
        gender = st.radio("Gender", ["Boys", "Girls"], horizontal=True)
    with col2:
        sport = st.selectbox("Sport", [s.title() for s in SPORTS])

    # Team selection: text input that doubles as search + free text
    # Users can type any name — if it matches a known team, the logo is used
    col3, col4 = st.columns(2)

    with col3:
        home_input = st.selectbox(
            "Home Team",
            [""] + team_options,
            index=1 if team_options else 0,
            key="home",
            format_func=lambda x: "Select a team or type below..." if x == "" else x
        )
        home_custom = st.text_input(
            "Or type any team name",
            key="custom_home",
            placeholder="e.g. Liberty High"
        )

    with col4:
        away_input = st.selectbox(
            "Away Team",
            [""] + team_options,
            index=min(2, len(team_options)) if team_options else 0,
            key="away",
            format_func=lambda x: "Select a team or type below..." if x == "" else x
        )
        away_custom = st.text_input(
            "Or type any team name",
            key="custom_away",
            placeholder="e.g. Liberty High"
        )

    st.caption("Typed names override the dropdown. Leave the text field blank to use the dropdown selection.")

    game_date = st.date_input("Game Date", value=datetime.now())

    # Determine final team names — typed name takes priority over dropdown
    if home_custom.strip():
        home_team = home_custom.strip().lower().replace(" ", "_")
    elif home_input:
        home_team = [k for k, v in team_display.items() if v == home_input][0] if team_options else ""
    else:
        home_team = ""

    if away_custom.strip():
        away_team = away_custom.strip().lower().replace(" ", "_")
    elif away_input:
        away_team = [k for k, v in team_display.items() if v == away_input][0] if team_options else ""
    else:
        away_team = ""

    # Check for missing logos
    home_logo_exists = find_logo(home_team) is not None if home_team else False
    away_logo_exists = find_logo(away_team) is not None if away_team else False
    missing_logos = []
    if home_team and not home_logo_exists:
        missing_logos.append(format_team_name(home_team))
    if away_team and not away_logo_exists:
        missing_logos.append(format_team_name(away_team))

    # Show missing logo warning
    if missing_logos:
        st.warning(
            f"**Missing logo{'s' if len(missing_logos) > 1 else ''}:** {', '.join(missing_logos)}. "
            f"These teams will use a placeholder (initials in a circle). "
            f"You can add logos in the **Manage Logos** tab."
        )
        proceed_single = st.checkbox("Generate anyway with placeholder logos", key="proceed_single")
    else:
        proceed_single = True

    if st.button("Generate Matchup Graphic", type="primary", use_container_width=True, disabled=bool(missing_logos and not proceed_single)):
        date_str = game_date.strftime("%Y-%m-%d")

        with st.spinner("Generating..."):
            img = generate_matchup_graphic(
                home_team, away_team, sport.lower(), date_str, gender
            )

        st.image(img, use_container_width=True)

        # Download button
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)

        home_display = format_team_name(home_team)
        away_display = format_team_name(away_team)
        filename = f"{date_str}_{away_display}_vs_{home_display}_{gender}_{sport}.png"

        st.download_button(
            label="⬇️ Download Image",
            data=buf,
            file_name=filename,
            mime="image/png",
            use_container_width=True
        )

with tab2:
    st.subheader("Batch Generate from Spreadsheet")
    st.markdown("""
    Upload a CSV or paste spreadsheet data to generate multiple matchup graphics at once.

    **Required columns:** **Title** and **Date/Time (EST) Start**

    The Title column should follow the format: `Boys Basketball: Concord at Ygnacio Valley`

    All other columns (Number, CMS, Topic, Priority, Hosted By, etc.) are kept for your reference but aren't needed for image generation.
    """)

    input_method = st.radio(
        "Input method",
        ["Paste from spreadsheet", "Upload CSV file"],
        horizontal=True,
        key="batch_input_method"
    )

    raw_data = None

    if input_method == "Upload CSV file":
        uploaded_file = st.file_uploader("Upload CSV", type=["csv", "tsv", "txt"], key="batch_csv")
        if uploaded_file:
            raw_data = uploaded_file.getvalue().decode("utf-8")
    else:
        raw_data = st.text_area(
            "Paste your spreadsheet data here (copy rows from Google Sheets/Excel — tab-separated)",
            height=200,
            placeholder="Title\tTopic\tPriority\tDate/Time (EST) Start\nBoys Basketball: Concord at Ygnacio Valley\tSports\tMinor\t1/26/2026 22:00:00",
            key="batch_paste"
        )

    if raw_data and raw_data.strip():
        rows = parse_csv_data(raw_data)

        if not rows:
            st.error("Couldn't parse the data. Make sure it has a header row.")
        else:
            # Find the Title and Date columns (flexible matching)
            sample_keys = list(rows[0].keys())
            title_col = None
            date_col = None

            for k in sample_keys:
                kl = k.strip().lower()
                if kl == "title":
                    title_col = k
                elif "date" in kl and "start" in kl:
                    date_col = k
                elif "date" in kl and "end" not in kl and date_col is None:
                    date_col = k

            if not title_col:
                st.error("Could not find a **Title** column. Make sure your header row includes 'Title'.")
            else:
                # Parse matchups from titles
                matchups = []
                skipped = []

                for i, row in enumerate(rows):
                    title = row.get(title_col, "").strip()
                    if not title:
                        continue

                    parsed = parse_title(title)
                    if parsed is None:
                        skipped.append((i + 2, title))  # +2 for header row + 1-indexed
                        continue

                    gender, sport, away_team, home_team = parsed

                    # Get date
                    game_date = None
                    if date_col:
                        game_date = parse_date_from_string(row.get(date_col, ""))

                    matchups.append({
                        "gender": gender,
                        "sport": sport,
                        "away_team": away_team,
                        "home_team": home_team,
                        "away_id": team_name_to_id(away_team),
                        "home_id": team_name_to_id(home_team),
                        "date": game_date,
                        "date_str": game_date.strftime("%Y-%m-%d") if game_date else "",
                        "title": title,
                        "has_home_logo": find_logo(team_name_to_id(away_team).replace("_", " ")) is not None or find_logo(team_name_to_id(away_team)) is not None,
                        "has_away_logo": find_logo(team_name_to_id(home_team).replace("_", " ")) is not None or find_logo(team_name_to_id(home_team)) is not None,
                    })

                # Show summary
                st.success(f"Found **{len(matchups)}** matchups ready to generate")

                if skipped:
                    with st.expander(f"{len(skipped)} rows skipped (non-matchup events)"):
                        for row_num, title in skipped:
                            st.write(f"Row {row_num}: {title}")

                # Preview table
                if matchups:
                    with st.expander("Preview matchups", expanded=True):
                        preview_data = []
                        for m in matchups:
                            home_logo = "✅" if find_logo(m["home_id"]) else "❌"
                            away_logo = "✅" if find_logo(m["away_id"]) else "❌"
                            preview_data.append({
                                "Sport": f"{m['gender']} {m['sport']}",
                                "Away": m["away_team"],
                                "Away Logo": away_logo,
                                "Home": m["home_team"],
                                "Home Logo": home_logo,
                                "Date": m["date"].strftime("%m/%d/%Y") if m["date"] else "No date",
                            })
                        st.dataframe(preview_data, use_container_width=True, hide_index=True)

                    # Check for missing logos across all matchups
                    all_missing = set()
                    for m in matchups:
                        if not find_logo(m["home_id"]):
                            all_missing.add(m["home_team"])
                        if not find_logo(m["away_id"]):
                            all_missing.add(m["away_team"])

                    proceed_batch = True
                    if all_missing:
                        sorted_missing = sorted(all_missing)
                        st.warning(
                            f"**{len(all_missing)} school{'s' if len(all_missing) > 1 else ''} missing logos:** "
                            f"{', '.join(sorted_missing)}. "
                            f"These will use placeholder graphics (initials in a circle). "
                            f"You can add logos in the **Manage Logos** tab first, or proceed anyway."
                        )
                        proceed_batch = st.checkbox(
                            f"Generate anyway with placeholders for {len(all_missing)} missing logo{'s' if len(all_missing) > 1 else ''}",
                            key="proceed_batch"
                        )

                    # Generate button
                    if st.button("Generate All Matchup Graphics", type="primary", use_container_width=True, key="batch_generate", disabled=bool(all_missing and not proceed_batch)):
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        generated_images = []

                        for i, m in enumerate(matchups):
                            status_text.text(f"Generating {i+1}/{len(matchups)}: {m['away_team']} vs {m['home_team']}...")
                            progress_bar.progress((i + 1) / len(matchups))

                            img = generate_matchup_graphic(
                                m["home_id"],
                                m["away_id"],
                                m["sport"].lower(),
                                m["date_str"] if m["date_str"] else datetime.now().strftime("%Y-%m-%d"),
                                m["gender"]
                            )

                            # Save to buffer
                            buf = io.BytesIO()
                            img.save(buf, format="PNG")
                            buf.seek(0)

                            filename = f"{m['date_str']}_{m['away_team']}_vs_{m['home_team']}_{m['gender']}_{m['sport']}.png"
                            filename = re.sub(r'[^\w\-_.]', '_', filename)

                            generated_images.append((filename, buf.getvalue(), img))

                        status_text.text(f"Done! Generated {len(generated_images)} images.")
                        progress_bar.progress(1.0)

                        # Show generated images in a grid
                        st.divider()
                        cols_per_row = 3
                        for i in range(0, len(generated_images), cols_per_row):
                            cols = st.columns(cols_per_row)
                            for j, col in enumerate(cols):
                                idx = i + j
                                if idx < len(generated_images):
                                    fname, data, img_obj = generated_images[idx]
                                    with col:
                                        st.image(img_obj, use_container_width=True)
                                        st.download_button(
                                            label=f"Download",
                                            data=data,
                                            file_name=fname,
                                            mime="image/png",
                                            key=f"dl_{idx}",
                                            use_container_width=True
                                        )

                        # Download all as ZIP
                        st.divider()
                        zip_buf = io.BytesIO()
                        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                            for fname, data, _ in generated_images:
                                zf.writestr(fname, data)
                        zip_buf.seek(0)

                        st.download_button(
                            label=f"⬇️ Download All ({len(generated_images)} images as ZIP)",
                            data=zip_buf,
                            file_name=f"matchup_graphics_{datetime.now().strftime('%Y%m%d')}.zip",
                            mime="application/zip",
                            use_container_width=True,
                            key="download_zip"
                        )

with tab4:
    st.subheader("Upcoming Games")
    st.caption("Auto-scraped from MaxPreps. Select games to generate matchup graphics.")

    # --- Cache / Scraper Logic ---
    def load_schedule_cache():
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def get_cache_age():
        cached = load_schedule_cache()
        if not cached or "cached_at" not in cached:
            return None
        try:
            cached_dt = datetime.fromisoformat(cached["cached_at"])
            return (datetime.now() - cached_dt).total_seconds() / 3600
        except (ValueError, TypeError):
            return None

    def run_scrape():
        try:
            from scraper import scrape_all_schools, save_cached_data
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            games, verification = scrape_all_schools(
                progress_callback=lambda cur, tot, school, sport:
                    st.session_state.update({"scrape_progress": cur / tot, "scrape_status": f"{school} — {sport}"})
            )
            save_cached_data(str(CACHE_FILE), games, verification)
            return games, verification
        except ImportError:
            st.error("Scraper module not found. Make sure `scraper.py` is in the app directory.")
            return [], {}
        except Exception as e:
            st.error(f"Scrape failed: {e}")
            return [], {}

    # Auto-refresh check
    cache_age = get_cache_age()
    cached_data = load_schedule_cache()
    needs_refresh = cache_age is None or cache_age > AUTO_REFRESH_HOURS

    # Controls row
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 1, 1])
    with ctrl_col1:
        if cached_data:
            age_str = f"{cache_age:.1f} hours ago" if cache_age and cache_age < 48 else (
                f"{cache_age/24:.1f} days ago" if cache_age else "unknown"
            )
            st.caption(f"Last updated: {age_str}")
        else:
            st.caption("No cached data — click Refresh to scrape MaxPreps")

    with ctrl_col2:
        refresh_clicked = st.button("🔄 Refresh Now", use_container_width=True)

    with ctrl_col3:
        auto_refresh = st.toggle("Auto-refresh", value=True, help=f"Re-scrape every {AUTO_REFRESH_HOURS}h")

    # Run scrape if needed
    games = []
    verification = {}

    if refresh_clicked or (needs_refresh and auto_refresh and not cached_data):
        with st.spinner("Scraping MaxPreps schedules..."):
            progress_bar = st.progress(0)
            status_text = st.empty()

            # Override progress callback to update UI
            def ui_progress(cur, tot, school, sport):
                progress_bar.progress(cur / tot)
                status_text.text(f"Scraping {school} — {sport}...")

            try:
                from scraper import scrape_all_schools, save_cached_data
                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                games, verification = scrape_all_schools(progress_callback=ui_progress)
                save_cached_data(str(CACHE_FILE), games, verification)
                status_text.text(f"Done! Found {len(games)} upcoming games.")
                progress_bar.progress(1.0)
            except Exception as e:
                st.error(f"Scrape failed: {e}")

    elif cached_data:
        games = cached_data.get("games", [])
        verification = cached_data.get("verification", {})

    # --- Display Games ---
    if games:
        # Filters
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        all_schools = sorted({g["school"] for g in games})
        all_sports = sorted({f"{g['gender']} {g['sport']}" for g in games})

        with filter_col1:
            selected_schools = st.multiselect("Filter by school", all_schools, default=all_schools, key="ug_schools")
        with filter_col2:
            selected_sports = st.multiselect("Filter by sport", all_sports, default=all_sports, key="ug_sports")
        with filter_col3:
            date_range = st.selectbox("Time range", ["Next 7 days", "Next 14 days", "Next 30 days", "All"], index=3, key="ug_range")

        # Apply filters
        filtered = games
        if selected_schools:
            filtered = [g for g in filtered if g["school"] in selected_schools]
        if selected_sports:
            filtered = [g for g in filtered if f"{g['gender']} {g['sport']}" in selected_sports]
        if date_range != "All":
            days = int(date_range.split()[1])
            cutoff = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
            filtered = [g for g in filtered if g.get("date_sort", "9999") <= cutoff]

        st.write(f"**{len(filtered)}** games shown")

        # Build display table with selection checkboxes
        if filtered:
            # Select All / Deselect All
            sel_col1, sel_col2, sel_col3 = st.columns([1, 1, 4])
            with sel_col1:
                select_all = st.button("Select All", use_container_width=True, key="select_all")
            with sel_col2:
                deselect_all = st.button("Deselect All", use_container_width=True, key="deselect_all")

            if select_all:
                st.session_state["all_selected"] = True
            if deselect_all:
                st.session_state["all_selected"] = False

            default_selected = st.session_state.get("all_selected", False)

            # Show as dataframe
            display_data = []
            for g in filtered:
                has_home_logo = "✅" if find_logo(team_name_to_id(g["school"])) else "❌"
                has_opp_logo = "✅" if find_logo(team_name_to_id(g["opponent"])) else "❌"
                display_data.append({
                    "Select": default_selected,
                    "Date": g["date"],
                    "Time": g["time"],
                    "Sport": f"{g['gender']} {g['sport']}",
                    "School": g["school"],
                    "H/A": g["home_away"],
                    "Opponent": g["opponent"],
                    "Venue": g.get("venue", ""),
                    "MaxPreps": g.get("game_url", ""),
                    "Our Logo": has_home_logo,
                    "Opp Logo": has_opp_logo,
                    "League": "⭐" if g.get("is_league") else "",
                })

            edited_df = st.data_editor(
                display_data,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", default=False),
                    "MaxPreps": st.column_config.LinkColumn("MaxPreps", display_text="Link"),
                },
                disabled=["Date", "Time", "Sport", "School", "H/A", "Opponent", "Venue", "MaxPreps", "Our Logo", "Opp Logo", "League"],
                use_container_width=True,
                hide_index=True,
                key="games_editor",
            )

            # Quick-copy section
            game_labels = [f"{g['date']} — {g['gender']} {g['sport']}: {g['school']} vs {g['opponent']}" for g in filtered]
            selected_game_idx = st.selectbox("📋 Copy game details", range(len(game_labels)), format_func=lambda i: game_labels[i], key="copy_picker")
            if selected_game_idx is not None:
                g = filtered[selected_game_idx]
                copy_cols = st.columns(3)
                hosted_by_raw = g.get("school", "") if g.get("home_away") == "Home" else g.get("opponent", "")
                hosted_by = f"{hosted_by_raw} High School" if hosted_by_raw and "High School" not in hosted_by_raw else hosted_by_raw
                # Venue always matches the hosted-by school
                # Use known address if available; for unknown opponents, only use
                # scraped venue if it doesn't belong to one of our tracked schools
                venue = KNOWN_ADDRESSES.get(hosted_by_raw, "")
                if not venue:
                    scraped_venue = g.get("venue", "")
                    our_school = g.get("school", "")
                    our_address = KNOWN_ADDRESSES.get(our_school, "")
                    # Only use scraped venue if it's not our own school's address
                    if scraped_venue and scraped_venue != our_address:
                        venue = scraped_venue
                fields = [
                    ("Title", g.get("title", "")),
                    ("Hosted By", hosted_by),
                    ("Sport", f"{g['gender']} {g['sport']}"),
                    ("Date", g.get("date", "")),
                    ("Time", g.get("time", "")),
                    ("H/A", g.get("home_away", "")),
                    ("Opponent", g.get("opponent", "")),
                    ("Venue", venue),
                    ("MaxPreps", g.get("game_url", "")),
                ]
                for i, (label, value) in enumerate(fields):
                    if value:
                        with copy_cols[i % 3]:
                            st.caption(label)
                            st.code(value, language=None)

            # Generate graphics for selected games
            selected_indices = [i for i, row in enumerate(edited_df) if row.get("Select")]

            if selected_indices:
                st.write(f"**{len(selected_indices)}** games selected")

                # Check missing logos
                selected_games = [filtered[i] for i in selected_indices]
                all_missing = set()
                for g in selected_games:
                    if not find_logo(team_name_to_id(g["school"])):
                        all_missing.add(g["school"])
                    if not find_logo(team_name_to_id(g["opponent"])):
                        all_missing.add(g["opponent"])

                proceed_upcoming = True
                if all_missing:
                    st.warning(
                        f"**{len(all_missing)} school{'s' if len(all_missing) > 1 else ''} missing logos:** "
                        f"{', '.join(sorted(all_missing))}. Placeholders will be used."
                    )
                    proceed_upcoming = st.checkbox("Generate anyway with placeholders", key="proceed_upcoming")

                if st.button(
                    f"Generate {len(selected_indices)} Matchup Graphic{'s' if len(selected_indices) > 1 else ''}",
                    type="primary",
                    use_container_width=True,
                    disabled=bool(all_missing and not proceed_upcoming),
                    key="gen_upcoming"
                ):
                    progress_bar = st.progress(0)
                    generated_images = []

                    for idx, g in enumerate(selected_games):
                        progress_bar.progress((idx + 1) / len(selected_games))

                        # Determine home/away teams
                        if g["home_away"] == "Home":
                            home_id = team_name_to_id(g["school"])
                            away_id = team_name_to_id(g["opponent"])
                        else:
                            home_id = team_name_to_id(g["opponent"])
                            away_id = team_name_to_id(g["school"])

                        try:
                            game_dt = datetime.strptime(g["date_sort"], "%Y-%m-%d")
                            date_str = game_dt.strftime("%Y-%m-%d")
                        except (ValueError, KeyError):
                            date_str = datetime.now().strftime("%Y-%m-%d")

                        img = generate_matchup_graphic(
                            home_id, away_id, g["sport"].lower(), date_str, g["gender"]
                        )

                        buf = io.BytesIO()
                        img.save(buf, format="PNG")
                        buf.seek(0)
                        fname = re.sub(r'[^\w\-_.]', '_', f"{date_str}_{g['title']}.png")
                        generated_images.append((fname, buf.getvalue(), img))

                    progress_bar.progress(1.0)

                    # Show grid
                    cols_per_row = 3
                    for i in range(0, len(generated_images), cols_per_row):
                        cols = st.columns(cols_per_row)
                        for j, col in enumerate(cols):
                            idx = i + j
                            if idx < len(generated_images):
                                fname, data, img_obj = generated_images[idx]
                                with col:
                                    st.image(img_obj, use_container_width=True)
                                    st.download_button(
                                        label="Download",
                                        data=data,
                                        file_name=fname,
                                        mime="image/png",
                                        key=f"dl_upcoming_{idx}",
                                        use_container_width=True,
                                    )

                    # ZIP download
                    if len(generated_images) > 1:
                        zip_buf = io.BytesIO()
                        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                            for fname, data, _ in generated_images:
                                zf.writestr(fname, data)
                        zip_buf.seek(0)
                        st.download_button(
                            label=f"⬇️ Download All ({len(generated_images)} images as ZIP)",
                            data=zip_buf,
                            file_name=f"matchup_graphics_{datetime.now().strftime('%Y%m%d')}.zip",
                            mime="application/zip",
                            use_container_width=True,
                            key="download_zip_upcoming",
                        )

        # --- Verification Report ---
        if verification:
            with st.expander("📋 Verification Report", expanded=False):
                status = verification.get("status", "Unknown")
                if status == "CLEAN":
                    st.success(f"✅ **{status}** — No issues found")
                else:
                    st.warning(f"⚠️ **{status}**")

                vcol1, vcol2 = st.columns(2)
                with vcol1:
                    st.metric("Total Games", verification.get("total_games", 0))
                    st.metric("Schools Scraped", verification.get("schools_scraped", 0))
                with vcol2:
                    st.metric("Sports Found", len(verification.get("sports_found", [])))
                    st.metric("Issues", len(verification.get("verification_issues", [])))

                # Games per school
                gps = verification.get("games_per_school", {})
                if gps:
                    st.write("**Games per school:**")
                    for school, count in sorted(gps.items()):
                        st.write(f"  {school}: {count}")

                # Issues
                issues = verification.get("verification_issues", [])
                if issues:
                    st.write("**Issues found:**")
                    for issue in issues:
                        if "MISMATCH" in issue or "CONFLICT" in issue:
                            st.error(issue)
                        elif "DUPLICATE" in issue:
                            st.warning(issue)
                        else:
                            st.info(issue)

                # Scrape errors
                errs = verification.get("scrape_errors", [])
                if errs:
                    st.write("**Scrape errors:**")
                    for err in errs:
                        st.error(err)

                # Full scrape log
                log = verification.get("scrape_log", [])
                if log:
                    with st.expander("Full scrape log"):
                        for entry in log:
                            st.text(entry)
    else:
        st.info("No schedule data yet. Click **Refresh Now** to scrape MaxPreps for upcoming games.")

with tab3:
    st.subheader("Current Logos")

    if available_teams:
        # Show logos in a grid
        cols = st.columns(5)
        for i, team in enumerate(sorted(available_teams)):
            logo_path = find_logo(team)
            if logo_path:
                with cols[i % 5]:
                    try:
                        logo_img = Image.open(logo_path)
                        st.image(logo_img, caption=format_team_name(team), width=80)
                    except:
                        st.write(format_team_name(team))
    else:
        st.info("No logos found. Add PNG/JPG files to the logos folder in the GitHub repo.")

    st.divider()
    st.subheader("Add New Logos")
    st.markdown("""
    To add new logos, go to the [GitHub repo](https://github.com) and:
    1. Navigate to the `logos/` folder
    2. Click **Add file** → **Upload files**
    3. Name files with underscores (e.g., `monte_vista.png`)
    4. The app will update automatically within a few minutes
    """)
