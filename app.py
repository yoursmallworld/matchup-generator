import streamlit as st
from PIL import Image, ImageDraw, ImageFont
from pathlib import Path
from datetime import datetime
import io
import base64
import zipfile
import re
import csv

# Configuration
BASE_DIR = Path(__file__).parent
LOGOS_DIR = BASE_DIR / "logos"
FONTS_DIR = BASE_DIR / "fonts"

# Image dimensions (16:9)
IMAGE_WIDTH = 1200
IMAGE_HEIGHT = 675

WHITE = (255, 255, 255)
BLACK = (0, 0, 0)

SPORTS = [
    "basketball", "football", "soccer", "baseball", "volleyball",
    "softball", "tennis", "wrestling", "track", "swimming"
]


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
            if file_normalized == normalized or normalized in file_normalized:
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

    # Team names
    team_font = get_title_font(42)

    home_name = format_team_name(home_team)
    home_bbox = draw.textbbox((0, 0), home_name, font=team_font)
    home_text_width = home_bbox[2] - home_bbox[0]
    home_logo_center = home_x + logo_size[0] // 2
    home_text_x = home_logo_center - home_text_width // 2
    draw.text((home_text_x, logo_y + logo_size[1] + 15), home_name, font=team_font, fill=BLACK)

    away_name = format_team_name(away_team)
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

tab1, tab2, tab3 = st.tabs(["Generate Graphic", "Batch Generate", "Manage Logos"])

with tab1:
    col1, col2 = st.columns(2)

    with col1:
        gender = st.radio("Gender", ["Boys", "Girls"], horizontal=True)
    with col2:
        sport = st.selectbox("Sport", [s.title() for s in SPORTS])

    # Team selection: dropdown with known teams + option to type a custom name
    team_options_with_custom = ["-- Type a custom name --"] + team_options

    col3, col4 = st.columns(2)

    with col3:
        home_select = st.selectbox("Home Team", team_options_with_custom, index=1 if len(team_options_with_custom) > 1 else 0, key="home")
        if home_select == "-- Type a custom name --":
            home_custom = st.text_input("Enter home team name", key="custom_home")
        else:
            home_custom = ""

    with col4:
        away_select = st.selectbox("Away Team", team_options_with_custom, index=min(2, len(team_options_with_custom)-1), key="away")
        if away_select == "-- Type a custom name --":
            away_custom = st.text_input("Enter away team name", key="custom_away")
        else:
            away_custom = ""

    game_date = st.date_input("Game Date", value=datetime.now())

    # Determine final team names for logo check
    if home_select == "-- Type a custom name --" and home_custom.strip():
        home_team = home_custom.strip().lower().replace(" ", "_")
    elif home_select != "-- Type a custom name --":
        home_team = [k for k, v in team_display.items() if v == home_select][0] if team_options else ""
    else:
        home_team = ""

    if away_select == "-- Type a custom name --" and away_custom.strip():
        away_team = away_custom.strip().lower().replace(" ", "_")
    elif away_select != "-- Type a custom name --":
        away_team = [k for k, v in team_display.items() if v == away_select][0] if team_options else ""
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
        filename = f"{away_display}_vs_{home_display}_{gender}_{sport}_{date_str}.png"

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

                            filename = f"{m['away_team']}_vs_{m['home_team']}_{m['gender']}_{m['sport']}_{m['date_str']}.png"
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
