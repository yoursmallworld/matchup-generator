import streamlit as st
from PIL import Image, ImageDraw, ImageFont
from pathlib import Path
from datetime import datetime
import io
import base64

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


# ========== STREAMLIT APP ==========

st.set_page_config(page_title="Matchup Generator", page_icon="🏆", layout="centered")

st.title("🏆 Matchup Generator")
st.caption("Generate sports matchup graphics for Smallworld Concord")

# Get available teams from logos folder
available_teams = get_available_teams()
team_display = {t: format_team_name(t) for t in available_teams}

# Add option for custom team name
team_options = sorted(team_display.values())

tab1, tab2 = st.tabs(["Generate Graphic", "Manage Logos"])

with tab1:
    col1, col2 = st.columns(2)

    with col1:
        gender = st.radio("Gender", ["Boys", "Girls"], horizontal=True)
    with col2:
        sport = st.selectbox("Sport", [s.title() for s in SPORTS])

    col3, col4 = st.columns(2)

    with col3:
        home_input = st.selectbox("Home Team", team_options, index=0, key="home")
    with col4:
        away_input = st.selectbox("Away Team", team_options, index=min(1, len(team_options)-1), key="away")

    game_date = st.date_input("Game Date", value=datetime.now())

    # Allow custom team names
    with st.expander("Use a custom team name?"):
        custom_home = st.text_input("Custom Home Team (leave blank to use dropdown)", key="custom_home")
        custom_away = st.text_input("Custom Away Team (leave blank to use dropdown)", key="custom_away")

    if st.button("Generate Matchup Graphic", type="primary", use_container_width=True):
        # Determine final team names
        home_team = custom_home.strip().lower().replace(" ", "_") if custom_home.strip() else \
            [k for k, v in team_display.items() if v == home_input][0]
        away_team = custom_away.strip().lower().replace(" ", "_") if custom_away.strip() else \
            [k for k, v in team_display.items() if v == away_input][0]

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
