import os
import json
import shutil
import time
import datetime
import textwrap
import logging
import boto3
import ffmpeg
from PIL import Image, ImageDraw, ImageFont
import numpy as np
import subprocess
import re
import googleapiclient.discovery
import googleapiclient.errors
from googleapiclient.http import MediaFileUpload
from instagram_private_api import Client, ClientCompatPatch, ClientError, ClientLoginError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('social_media_autoposter')

# S3 bucket configuration - UPDATED with correct bucket names
ASSETS_BUCKET = "marketing-automation-static"  # For miscellaneous assets (CTA videos, music, etc.)
LONG_VIDEOS_BUCKET = "longs-clips"  # For long-form content
SHORTS_REELS_BUCKET = "shorts-clips"  # For shorts and reels content
CONFIG_BUCKET = "marketing-automation-static"  # For configuration files (using the same static bucket)

# S3 path configuration
CONFIG_FILE_KEY = "content_posting_schedule.json"
TRACKING_FILE_KEY = "posting_tracker.json"
TITLES_LONG_KEY = "titles.json"
TITLES_SHORTS_KEY = "titles-shorts.json"

# Local path configuration (for Lambda execution)
TEMP_DIR = "/tmp/autoposter/temp"
OUTPUT_DIR = "/tmp/autoposter/output"
DOWNLOAD_DIR = "/tmp/autoposter/download"

# Constants for CTA content
LONG_TEXT_CTAS = {
    "beginningCTA_1": "More in The Creator's Database (Link in desc.)",
    "beginningCTA_2": "Exclusive Clips in The Creator's Database (Link in desc.)"
}

LONG_VIDEO_CTAS = {
    "endCTA_1": "endCTA1.mp4",
    "endCTA_2": "endCTA2.mp4"
}

SHORT_TEXT_CTAS = {
    "shortTextCTA_1": "Speed Up Social Media Growth\n(Click the Link in Bio)",
    "shortTextCTA_2": "Want Exclusive {Creator Name} Clips?\n(Link in Bio)"
}

REELS_TEXT_CTAS = {
    "reelstextCTA_1": "How to Really Grow Your Audience\n(Read the Description)",
    "reelstextCTA_2": "The Truth of Social Media Growth\n(Read the Description)"
}

REELS_DESC_CTAS = {
    "reelsdescCTA_1": "103 of our creators went from 0-10k followers in 3 weeks. The Creator's Database uses AI algorithms to analyze thousands of hours of content to save you time and find the exact gems that will grow your audience. This way, it takes just 20 minutes to implement what took others years to learn. THIS IS NOT A COURSE. If you want access to a tool that identifies exactly what is bottlenecking your growth and gives you every single piece of information you need to fix it. CLICK THE LINK IN BIO TO JOIN",
    "reelsdescCTA_2": "Watching reels won't grow your audience. The Creator's Database uses AI algorithms to analyze thousands of hours of content to save you time and find the exact gems that will grow your audience. This way, it takes just 20 minutes to implement what took others years to learn. THIS IS NOT A COURSE. If you want access to a tool that identifies exactly what is bottlenecking your growth and gives you EVERY single piece of information you need to fix it. CLICK THE LINK IN BIO TO JOIN",
    "reelsdescCTA_3": "Watching reels won't grow your audience. The Creator's Database uses AI to analyze thousands of hours of podcasts and courses on social media growth to save you time. THIS IS NOT A COURSE. If you want access to a tool that identifies exactly what is bottlenecking your growth and gives you EVERY single piece of information you need to fix it. Skip 99% of the fluff and get only what works. CLICK THE LINK IN BIO TO JOIN",
    "reelsdescCTA_4": "How did 103 of our creators grow from 0-10k followers in 3 weeks? The Creator's Database uses AI to analyze thousands of hours of podcasts and courses on social media growth to save you time. THIS IS NOT A COURSE. If you want access to a tool that identifies exactly what is bottlenecking your growth and gives you EVERY single piece of information you need to fix it. Skip 99% of the fluff and get only what works. CLICK THE LINK IN BIO TO JOIN"
}

LONGS_DESCRIPTION = "Join The Creator's Database for Hundreds of Hidden Gems, Resources, and Priority AI Access: https://thecreatorsdb.com/"

# Initialize AWS S3 client
s3_client = boto3.client('s3')

# Initialize YouTube client (cached for reuse)
youtube_clients = {}

# Initialize Instagram client (cached for reuse)
instagram_clients = {}


def setup_directories():
    """Create necessary directories for processing"""
    for directory in [TEMP_DIR, OUTPUT_DIR, DOWNLOAD_DIR]:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")


def download_file_from_s3(bucket, key, local_path):
    """Download a file from S3 to local storage"""
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3_client.download_file(bucket, key, local_path)
        logger.info(f"Downloaded {bucket}/{key} to {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {bucket}/{key}: {e}")
        return False


def upload_file_to_s3(local_path, bucket, key):
    """Upload a file from local storage to S3"""
    try:
        s3_client.upload_file(local_path, bucket, key)
        logger.info(f"Uploaded {local_path} to {bucket}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error uploading {local_path} to {bucket}/{key}: {e}")
        return False


def load_config_from_s3():
    """Load the content posting schedule configuration from S3"""
    config_local_path = os.path.join(DOWNLOAD_DIR, "content_posting_schedule.json")

    if download_file_from_s3(CONFIG_BUCKET, CONFIG_FILE_KEY, config_local_path):
        try:
            with open(config_local_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error parsing config file: {e}")
            return None
    return None


def load_or_create_tracking_data():
    """Load or create the posting tracker data from S3"""
    tracking_local_path = os.path.join(DOWNLOAD_DIR, "posting_tracker.json")

    try:
        # Try to download existing tracking file
        if download_file_from_s3(CONFIG_BUCKET, TRACKING_FILE_KEY, tracking_local_path):
            with open(tracking_local_path, 'r') as f:
                return json.load(f)

        # If file doesn't exist, create a new tracking structure
        logger.info("No existing tracking data found. Creating new tracking file.")
        tracking_data = {
            "last_processed_day": None,
            "last_run": None,
            "posts": {}
        }

        # Save and upload the new tracking file
        with open(tracking_local_path, 'w') as f:
            json.dump(tracking_data, f, indent=4)

        upload_file_to_s3(tracking_local_path, CONFIG_BUCKET, TRACKING_FILE_KEY)
        return tracking_data

    except Exception as e:
        logger.error(f"Error loading or creating tracking data: {e}")
        # Return a minimal tracking structure in case of error
        return {
            "last_processed_day": None,
            "last_run": None,
            "posts": {}
        }


def update_tracking_data(tracking_data):
    """Update the tracking data in S3"""
    tracking_local_path = os.path.join(DOWNLOAD_DIR, "posting_tracker.json")

    try:
        # Update the last run timestamp
        tracking_data["last_run"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save and upload the tracking file
        with open(tracking_local_path, 'w') as f:
            json.dump(tracking_data, f, indent=4)

        upload_file_to_s3(tracking_local_path, CONFIG_BUCKET, TRACKING_FILE_KEY)
        logger.info("Updated tracking data in S3")
        return True
    except Exception as e:
        logger.error(f"Error updating tracking data: {e}")
        return False


def determine_processing_day(tracking_data, config):
    """Determine which day needs to be processed based on tracking data"""
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    # If we already processed today, don't do it again
    if tracking_data.get("last_processed_day") == today:
        logger.info(f"Already processed content for today ({today}). Skipping.")
        return None

    # Get the list of available day keys from the first channel
    if not config or "youtubeChannels" not in config or not config["youtubeChannels"]:
        logger.error("Invalid configuration: No YouTube channels found")
        return None

    first_channel = list(config["youtubeChannels"].keys())[0]
    day_keys = list(config["youtubeChannels"][first_channel].keys())

    # If this is the first run, start with day1
    if not tracking_data.get("last_processed_day"):
        tracking_data["last_processed_day"] = today
        return "day1"

    # Find the last processed day and determine the next one
    last_day = None
    for channel_id, post_data in tracking_data.get("posts", {}).items():
        for post in post_data:
            if "day" in post and (not last_day or post["day"] > last_day):
                last_day = post["day"]

    if not last_day:
        # No previous posts found, start with day1
        tracking_data["last_processed_day"] = today
        return "day1"

    # Extract day number and increment
    try:
        day_num = int(last_day.replace("day", ""))
        next_day = f"day{day_num + 1}"

        # Check if the next day exists in the config
        if next_day in day_keys:
            tracking_data["last_processed_day"] = today
            return next_day
        else:
            # If we've reached the end, cycle back to day1
            logger.info(f"Reached the end of scheduled days. Cycling back to day1.")
            tracking_data["last_processed_day"] = today
            return "day1"

    except (ValueError, AttributeError):
        logger.error(f"Error determining next day from {last_day}")
        tracking_data["last_processed_day"] = today
        return "day1"

def load_titles(is_short=False):
    """Load titles from S3 JSON files"""
    key = TITLES_SHORTS_KEY if is_short else TITLES_LONG_KEY
    local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(key))

    if download_file_from_s3(CONFIG_BUCKET, key, local_path):
        try:
            with open(local_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading titles from {key}: {e}")
            return {}
    return {}


def get_video_info(file_path):
    """Get video information using ffprobe"""
    try:
        probe = ffmpeg.probe(file_path)
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
        if video_stream:
            width = int(video_stream['width'])
            height = int(video_stream['height'])
            duration = float(probe['format']['duration'])
            return width, height, duration
        return None, None, None
    except Exception as e:
        logger.error(f"Error getting video info: {e}")
        return None, None, None


def format_title_into_two_lines(title, max_chars_per_line=25):
    """Split a title into two balanced lines for better visual presentation"""
    # If title already has line breaks, return as is
    if '\n' in title:
        return title

    # If title is short enough, return as is
    if len(title) <= max_chars_per_line:
        return title

    words = title.split()

    # If just one or two words, return as is
    if len(words) <= 2:
        return title

    # Find the middle point to split
    total_chars = len(title)
    middle_char_index = total_chars // 2

    # Find the best word break point closest to the middle
    best_split_index = 0
    current_length = 0

    for i, word in enumerate(words):
        current_length += len(word) + (1 if i > 0 else 0)  # Add space except for first word

        # If we've passed the middle point, this might be a good place to break
        if current_length >= middle_char_index:
            best_split_index = i
            break

    # Make sure we don't split too early
    if best_split_index == 0 and len(words) > 1:
        best_split_index = 1

    # Create the two lines
    first_line = ' '.join(words[:best_split_index])
    second_line = ' '.join(words[best_split_index:])

    # If the split is very unbalanced, try to move a word between lines
    if abs(len(first_line) - len(second_line)) > 10 and len(words) > 2:
        if len(first_line) > len(second_line) and best_split_index > 1:
            best_split_index -= 1
        elif len(second_line) > len(first_line) and best_split_index < len(words) - 1:
            best_split_index += 1

        # Recalculate with new split point
        first_line = ' '.join(words[:best_split_index])
        second_line = ' '.join(words[best_split_index:])

    return f"{first_line}\n{second_line}"


def create_text_overlay(text, width, height, font_size=100, position="bottom", padding=20,
                        bg_color=(0, 0, 0, 180), text_color=(255, 255, 255, 255)):
    """Create a text overlay for videos"""
    # Create transparent image
    overlay = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    try:
        # Try to load the appropriate font
        font_path = os.path.join(DOWNLOAD_DIR, "fonts", "Poppins.ttf")
        if os.path.exists(font_path):
            font = ImageFont.truetype(font_path, font_size)
        else:
            font = ImageFont.load_default()
    except:
        font = ImageFont.load_default()

    # Wrap text to fit width
    avg_char_width = font_size * 0.6  # Approximation
    chars_per_line = int((width - 2 * padding) / avg_char_width)
    wrapped_text = textwrap.fill(text, width=chars_per_line)

    # Calculate text dimensions
    text_bbox = draw.textbbox((0, 0), wrapped_text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]

    # Make the rectangle wider (90% of width)
    rect_width = int(width * 0.9)  # Use 90% of the width
    rect_x = (width - rect_width) // 2  # Center horizontally

    # Calculate vertical position
    if position == "top":
        rect_y = padding
    elif position == "bottom":
        rect_y = height - text_height - padding - 40  # Extra padding at bottom
    else:  # center
        rect_y = (height - text_height) // 2

    # Make the text box taller with more padding
    rect_height = text_height + (padding * 2)  # More padding for height

    # Draw rectangle background with rounded corners
    draw.rectangle([(rect_x, rect_y), (rect_x + rect_width, rect_y + rect_height)],
                   fill=bg_color)

    # Draw text
    text_x = (width - text_width) // 2
    text_y = rect_y + (rect_height - text_height) // 2  # Center text vertically in box
    draw.text((text_x, text_y), wrapped_text, font=font, fill=text_color)

    return overlay


def create_text_image(text, width, height, font_size=40, position="bottom"):
    """Create a text image and save it as PNG for ffmpeg"""
    overlay = create_text_overlay(text, width, height, font_size, position)
    return overlay


def extract_creator_name(clip_id):
    """Extract creator name from clip ID (format: 'CreatorName-001')"""
    if not clip_id or '-' not in clip_id:
        return "Creator"

    parts = clip_id.split('-')
    if len(parts) < 2:
        return "Creator"

    # Handle multi-word creator names
    creator_parts = parts[:-1]  # Everything before the last part
    return " ".join(creator_parts)


def download_fonts_and_assets():
    """Download necessary fonts and assets from S3"""
    # Create fonts directory
    fonts_dir = os.path.join(DOWNLOAD_DIR, "fonts")
    os.makedirs(fonts_dir, exist_ok=True)

    # Download fonts
    font_files = ["Poppins.ttf", "Poppins-Bold.ttf"]
    for font in font_files:
        font_key = f"fonts/{font}"
        font_path = os.path.join(fonts_dir, font)
        download_file_from_s3(ASSETS_BUCKET, font_key, font_path)

    # Create CTA videos directory
    cta_dir = os.path.join(DOWNLOAD_DIR, "cta_videos")
    os.makedirs(cta_dir, exist_ok=True)

    # Download CTA videos
    for cta_video in LONG_VIDEO_CTAS.values():
        cta_key = f"cta_videos/{cta_video}"
        cta_path = os.path.join(cta_dir, cta_video)
        download_file_from_s3(ASSETS_BUCKET, cta_key, cta_path)

    # Create music directory
    music_dir = os.path.join(DOWNLOAD_DIR, "music")
    os.makedirs(music_dir, exist_ok=True)

    # Download default music tracks (assuming there are standard tracks)
    music_tracks = ["track1.mp3", "track2.mp3", "track3.mp3"]
    for track in music_tracks:
        track_key = f"music/{track}"
        track_path = os.path.join(music_dir, track)
        download_file_from_s3(ASSETS_BUCKET, track_key, track_path)


def download_clip(clip_id, is_short=False):
    """Download a clip from the appropriate S3 bucket"""
    bucket = SHORTS_REELS_BUCKET if is_short else LONG_VIDEOS_BUCKET
    key = f"{clip_id}.mp4"

    # Create the local directory for clips
    clips_dir = os.path.join(DOWNLOAD_DIR, "shorts" if is_short else "longs")
    os.makedirs(clips_dir, exist_ok=True)

    local_path = os.path.join(clips_dir, f"{clip_id}.mp4")

    # Download the clip
    success = download_file_from_s3(bucket, key, local_path)

    if not success:
        logger.error(f"Failed to download clip {clip_id} from {bucket}")
        return None

    return local_path


def add_music_to_video(video_path, music_track, output_path):
    """Add music track to a video, making sure it doesn't override existing audio"""
    if not music_track:
        return video_path

    music_path = os.path.join(DOWNLOAD_DIR, "music", f"{music_track}.mp3")
    if not os.path.exists(music_path):
        logger.warning(f"Music track not found: {music_path}")
        return video_path

    try:
        # Get video duration
        _, _, video_duration = get_video_info(video_path)
        if not video_duration:
            logger.warning("Could not determine video duration")
            return video_path

        # Add music, keeping original audio at a significantly higher volume
        cmd = [
            'ffmpeg', '-y',
            '-i', video_path,  # Input video
            '-i', music_path,  # Input music
            '-filter_complex',
            # Increase original audio to 6.0x volume and decrease music to 2.0x volume
            '[0:a]volume=6.0[a1];[1:a]volume=2.0,atrim=0:{0},aloop=loop=-1:size=2e+009[a2];[a1][a2]amix=inputs=2:duration=first[a]'.format(
                video_duration),
            '-map', '0:v',  # Map video from first input
            '-map', '[a]',  # Map mixed audio
            '-c:v', 'copy',  # Copy video codec to avoid re-encoding
            '-c:a', 'aac',  # Audio codec
            '-b:a', '256k',  # Higher audio bitrate for better quality
            '-shortest',  # End when shortest input ends
            output_path
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Added music to video: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error adding music: {e}")
        return video_path


def create_long_video(clip_id, text_cta_type, video_cta_type, title_index):
    """Process a long-form video by adding text CTA and end video CTA"""
    # Download the clip file
    clip_path = download_clip(clip_id, is_short=False)
    if not clip_path:
        logger.error(f"Error: Clip file not found for: {clip_id}")
        return None

    # Load title
    titles = load_titles(is_short=False)
    if clip_id not in titles or not titles[clip_id]:
        logger.warning(f"No title found for clip {clip_id}")
        title = f"Video by {extract_creator_name(clip_id)}"
    else:
        try:
            title_idx = int(title_index) - 1  # Convert to 0-based index
            title = titles[clip_id][title_idx % len(titles[clip_id])]
        except (ValueError, IndexError):
            title = titles[clip_id][0]

    logger.info(f"Processing long video: {clip_id}")
    logger.info(f"Title: {title}")

    # Get video info
    width, height, duration = get_video_info(clip_path)
    if not width or not height or not duration:
        logger.error(f"Error: Could not get video info for {clip_path}")
        return None

    # Replace creator name in text CTA
    creator_name = extract_creator_name(clip_id)
    text_cta = LONG_TEXT_CTAS.get(text_cta_type, "").replace("{Creator Name}", creator_name)

    # Create temp files for processing
    temp_clip = os.path.join(TEMP_DIR, f"temp_clip_{clip_id}.mp4")
    final_output = os.path.join(OUTPUT_DIR, f"{clip_id}_final.mp4")

    try:
        # Copy the clip to temp directory
        shutil.copy(clip_path, temp_clip)

        # Create text overlay image for CTA with white rounded rectangle
        cta_height = 80  # Height for CTA bar
        text_overlay_img = os.path.join(TEMP_DIR, f"text_overlay_{clip_id}.png")

        # Create a transparent background image
        overlay_background = Image.new('RGBA', (width, cta_height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay_background)

        # Use Poppins font
        cta_font_size = 40  # Smaller size for long videos
        try:
            font_path = os.path.join(DOWNLOAD_DIR, "fonts", "Poppins-Bold.ttf")
            font_cta = ImageFont.truetype(font_path, cta_font_size)
        except:
            font_cta = ImageFont.load_default()

        # Calculate text dimensions
        text_bbox = draw.textbbox((0, 0), text_cta, font=font_cta)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]

        # Add padding around text
        h_padding = 50  # Horizontal padding
        v_padding = 15  # Vertical padding

        # Calculate rectangle dimensions
        rect_width = text_width + (2 * h_padding)

        # Make the white bar wider
        min_width = int(width * 0.6)  # Minimum 60% of video width
        rect_width = max(rect_width, min_width)
        rect_width = min(rect_width, int(width * 0.9))  # Cap at 90%

        rect_height = text_height + (2 * v_padding)

        # Calculate position (centered at bottom)
        rect_x = (width - rect_width) // 2
        rect_y = (cta_height - rect_height) // 2

        # Create a rounded rectangle
        corner_radius = 18
        rounded_rect = Image.new('RGBA', (rect_width, rect_height), (255, 255, 255, 230))
        rounded_rect_mask = Image.new('L', (rect_width, rect_height), 0)
        rounded_rect_draw = ImageDraw.Draw(rounded_rect_mask)

        # Draw the rounded rectangle on the mask
        rounded_rect_draw.rectangle([(corner_radius, 0), (rect_width - corner_radius, rect_height)], fill=255)
        rounded_rect_draw.rectangle([(0, corner_radius), (rect_width, rect_height - corner_radius)], fill=255)

        # Draw four circles at corners to create rounded effect
        rounded_rect_draw.pieslice([(0, 0), (corner_radius * 2, corner_radius * 2)], 180, 270, fill=255)
        rounded_rect_draw.pieslice([(rect_width - corner_radius * 2, 0), (rect_width, corner_radius * 2)], 270, 360,
                                   fill=255)
        rounded_rect_draw.pieslice([(0, rect_height - corner_radius * 2), (corner_radius * 2, rect_height)], 90, 180,
                                   fill=255)
        rounded_rect_draw.pieslice([(rect_width - corner_radius * 2, rect_height - corner_radius * 2),
                                    (rect_width, rect_height)], 0, 90, fill=255)

        # Apply the mask to the rectangle
        rounded_rect.putalpha(rounded_rect_mask)

        # Paste the rounded rectangle onto the overlay background
        overlay_background.paste(rounded_rect, (rect_x, rect_y), rounded_rect)

        # Draw text centered in the rectangle
        text_x = rect_x + (rect_width - text_width) // 2
        text_y = rect_y + (rect_height - text_height) // 2
        draw.text((text_x, text_y), text_cta, fill=(0, 0, 0, 255), font=font_cta)  # Black text

        # Save overlay image
        overlay_background.save(text_overlay_img, "PNG")

        # Calculate position for CTA overlay (at bottom of video)
        cta_position = height - cta_height - 50  # 50px from bottom

        # Timing for text CTA
        cta_start_time = min(15, duration / 3)  # Add CTA after 15s or 1/3 of video
        cta_end_time = cta_start_time + 7  # Display CTA for 7 seconds

        # Create video with text CTA that appears at specified time
        with_text_cta = os.path.join(TEMP_DIR, f"with_text_cta_{clip_id}.mp4")
        cmd = [
            'ffmpeg', '-y',
            '-i', temp_clip,  # Input video
            '-i', text_overlay_img,  # Text overlay
            '-filter_complex',
            f'[0:v][1:v]overlay=0:{cta_position}:enable=\'between(t,{cta_start_time},{cta_end_time})\'',
            '-c:v', 'libx264',  # Use h264 codec
            '-crf', '23',  # Quality setting
            '-preset', 'medium',  # Encoding speed/quality balance
            '-c:a', 'aac',  # Audio codec
            '-b:a', '192k',  # Audio bitrate
            '-pix_fmt', 'yuv420p',  # Compatible pixel format
            '-movflags', '+faststart',  # Optimize for streaming
            with_text_cta
        ]

        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logger.info(f"Successfully created video with text CTA")
        except Exception as e:
            logger.error(f"Error adding text CTA: {e}")
            # Fallback - just use the original video
            logger.warning("Using fallback - copying original video")
            shutil.copy(clip_path, with_text_cta)

        # Download and add end video CTA if specified
        final_video = with_text_cta

        if video_cta_type in LONG_VIDEO_CTAS:
            cta_video_file = LONG_VIDEO_CTAS[video_cta_type]
            cta_video_path = os.path.join(DOWNLOAD_DIR, "cta_videos", cta_video_file)

            if os.path.exists(cta_video_path):
                # Create a file with end CTA video appended
                with_end_cta = os.path.join(TEMP_DIR, f"with_end_cta_{clip_id}.mp4")

                # Create a temporary file listing the videos to concatenate
                concat_list = os.path.join(TEMP_DIR, f"concat_list_{clip_id}.txt")
                with open(concat_list, 'w') as f:
                    f.write(f"file '{with_text_cta}'\n")
                    f.write(f"file '{cta_video_path}'\n")

                # Concatenate the videos
                concat_cmd = [
                    'ffmpeg', '-y',
                    '-f', 'concat',
                    '-safe', '0',
                    '-i', concat_list,
                    '-c', 'copy',
                    with_end_cta
                ]

                try:
                    subprocess.run(concat_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    logger.info(f"Successfully added end CTA video")
                    final_video = with_end_cta
                except Exception as e:
                    logger.error(f"Error adding end CTA video: {e}")
                    # Keep using the video with just the text CTA
                    final_video = with_text_cta
            else:
                logger.warning(f"End CTA video not found: {cta_video_path}")

        # Copy the final video to output directory
        shutil.copy(final_video, final_output)

        logger.info(f"Long video created: {final_output}")

        # Clean up temp files
        for tmp_file in [temp_clip, text_overlay_img, with_text_cta]:
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except:
                    pass

        if 'with_end_cta' in locals() and os.path.exists(locals()['with_end_cta']):
            try:
                os.remove(locals()['with_end_cta'])
            except:
                pass

        if 'concat_list' in locals() and os.path.exists(locals()['concat_list']):
            try:
                os.remove(locals()['concat_list'])
            except:
                pass

        return {
            "path": final_output,
            "title": title,
            "description": LONGS_DESCRIPTION,
            "clip_id": clip_id
        }

    except Exception as e:
        logger.error(f"Error creating long video: {e}")
        import traceback
        traceback.print_exc()  # Print full stack trace for better debugging
        return None


def create_youtube_short(clip_id, music_track, text_cta_type, video_cta_type):
    """Process a video specifically for YouTube Shorts format"""
    # Download the clip file from S3
    clip_path = download_clip(clip_id, is_short=True)
    if not clip_path:
        logger.error(f"Error: Clip file not found for: {clip_id}")
        return None

    # Load title for the short
    titles = load_titles(is_short=True)
    if clip_id not in titles or not titles[clip_id]:
        logger.warning(f"No title found for clip {clip_id}")
        title = f"{extract_creator_name(clip_id)} on\nCreating Viral Content"
    else:
        title = format_title_into_two_lines(titles[clip_id][0])

    logger.info(f"Processing YouTube Short: {clip_id}")
    logger.info(f"Title: {title}")

    # Get video info
    width, height, duration = get_video_info(clip_path)
    if not width or not height or not duration:
        logger.error(f"Error: Could not get video info for {clip_path}")
        return None

    # Replace creator name in text CTA
    creator_name = extract_creator_name(clip_id)
    text_cta = SHORT_TEXT_CTAS.get(text_cta_type, "").replace("{Creator Name}", creator_name)

    # Create temp files
    temp_clip = os.path.join(TEMP_DIR, f"temp_clip_{clip_id}.mp4")

    # Create a final output file with correct format
    final_output = os.path.join(OUTPUT_DIR, f"{clip_id}_youtube_short.mp4")

    try:
        # Copy the clip to temp directory
        shutil.copy(clip_path, temp_clip)

        # Add music if specified
        if music_track:
            temp_with_music = os.path.join(TEMP_DIR, f"temp_with_music_{clip_id}.mp4")
            add_music_to_video(temp_clip, music_track, temp_with_music)
            # Replace temp_clip with the version with music
            if os.path.exists(temp_with_music):
                os.remove(temp_clip)
                os.rename(temp_with_music, temp_clip)
                logger.info(f"Added music track {music_track} to clip")

        # Format shorts to look like example image with top title and bottom CTA
        # Get clip info for proper sizing
        clip_width, clip_height, _ = get_video_info(temp_clip)

        # Create a 9:16 aspect ratio video with black bars
        target_height = int(clip_width * 16 / 9) if clip_width > clip_height else clip_height
        target_width = int(target_height * 9 / 16) if clip_height > clip_width else clip_width

        # Creating temporary files for our processing
        with_black_bars = os.path.join(TEMP_DIR, f"with_black_bars_{clip_id}.mp4")
        with_title = os.path.join(TEMP_DIR, f"with_title_{clip_id}.mp4")
        with_bottom_cta = os.path.join(TEMP_DIR, f"with_bottom_cta_{clip_id}.mp4")

        # Step 1: Add black bars to make it 9:16 if needed
        if clip_width > clip_height:  # Landscape video
            vertical_padding = (target_height - clip_height) // 2
            pad_cmd = [
                'ffmpeg', '-y',
                '-i', temp_clip,
                '-vf', f'pad=width={clip_width}:height={target_height}:x=0:y={vertical_padding}:color=black',
                '-c:a', 'copy',
                with_black_bars
            ]
            subprocess.run(pad_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:  # Already portrait
            shutil.copy(temp_clip, with_black_bars)

        # Step 2: Add title at the top
        # Create title image
        title_height = 170  # Height for title bar
        title_img = os.path.join(TEMP_DIR, f"title_{clip_id}.png")

        # Create a transparent background image
        title_background = Image.new('RGBA', (target_width, title_height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(title_background)

        # Different font size for title (larger)
        title_font_size = 60  # Large size for title

        try:
            font_path = os.path.join(DOWNLOAD_DIR, "fonts", "Poppins-Bold.ttf")
            font_title = ImageFont.truetype(font_path, title_font_size)
        except:
            font_title = ImageFont.load_default()

        # Improved text centering for multi-line titles
        title_lines = title.split('\n')
        line_gap = 20  # Space between lines

        # Calculate text dimensions for all lines combined
        line_heights = []
        total_text_height = 0
        max_line_width = 0

        for line in title_lines:
            line_bbox = draw.textbbox((0, 0), line, font=font_title)
            line_width = line_bbox[2] - line_bbox[0]
            line_height = line_bbox[3] - line_bbox[1]
            line_heights.append((line, line_width, line_height))
            total_text_height += line_height
            max_line_width = max(max_line_width, line_width)

        # Add gaps between lines
        if len(title_lines) > 1:
            total_text_height += line_gap * (len(title_lines) - 1)

        # Add padding around text for the white rounded rectangle
        h_padding = 40  # Horizontal padding
        v_padding_top = 15  # Less padding for better vertical alignment
        v_padding_bottom = 15  # Equal padding

        # Calculate the white background rectangle dimensions
        rect_width = max_line_width + (2 * h_padding)
        rect_height = total_text_height + v_padding_top + v_padding_bottom

        # Make sure the white box is not too wide (max 80% of screen width)
        rect_width = min(rect_width, int(target_width * 0.8))

        # Calculate positions for the white rectangle
        rect_x = (target_width - rect_width) // 2
        rect_y = (title_height - rect_height) // 2

        # Draw rounded rectangle with white background
        # Using PIL for rounded corners - create a separate mask
        corner_radius = 25  # Increased from 15 to 25 for more rounded corners
        rounded_rect = Image.new('RGBA', (rect_width, rect_height), (255, 255, 255, 255))
        rounded_rect_mask = Image.new('L', (rect_width, rect_height), 0)
        rounded_rect_draw = ImageDraw.Draw(rounded_rect_mask)

        # Draw the rounded rectangle on the mask
        rounded_rect_draw.rectangle([(corner_radius, 0), (rect_width - corner_radius, rect_height)], fill=255)
        rounded_rect_draw.rectangle([(0, corner_radius), (rect_width, rect_height - corner_radius)], fill=255)

        # Draw four circles at corners to create rounded effect
        rounded_rect_draw.pieslice([(0, 0), (corner_radius * 2, corner_radius * 2)], 180, 270, fill=255)
        rounded_rect_draw.pieslice([(rect_width - corner_radius * 2, 0), (rect_width, corner_radius * 2)], 270, 360,
                                   fill=255)
        rounded_rect_draw.pieslice([(0, rect_height - corner_radius * 2), (corner_radius * 2, rect_height)], 90, 180,
                                   fill=255)
        rounded_rect_draw.pieslice(
            [(rect_width - corner_radius * 2, rect_height - corner_radius * 2), (rect_width, rect_height)], 0, 90,
            fill=255)

        # Apply the mask to the rectangle
        rounded_rect.putalpha(rounded_rect_mask)

        # Paste the rounded rectangle onto the title background
        title_background.paste(rounded_rect, (rect_x, rect_y), rounded_rect)

        # Calculate text block height with line gaps
        text_block_height = total_text_height
        if len(title_lines) > 1:
            text_block_height += line_gap * (len(title_lines) - 1)

        # Calculate starting y position with a slight upward offset for better positioning
        vertical_offset = 4  # Slight upward shift to make text appear more centered
        text_y = rect_y + ((rect_height - text_block_height) // 2) - vertical_offset
        current_y = text_y

        # Draw each line centered horizontally
        for i, (line, line_width, line_height) in enumerate(line_heights):
            text_x = rect_x + (rect_width - line_width) // 2
            draw.text((text_x, current_y), line, fill=(0, 0, 0, 255), font=font_title)
            if i < len(line_heights) - 1:
                current_y += line_height + line_gap

        # Save title image
        title_background.save(title_img, "PNG")

        # Add title to the video
        title_cmd = [
            'ffmpeg', '-y',
            '-i', with_black_bars,
            '-i', title_img,
            '-filter_complex', '[0:v][1:v]overlay=0:400',  # Position at top with 400px padding
            '-c:a', 'copy',
            with_title
        ]
        subprocess.run(title_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Step 3: Add bottom text CTA with similar styling
        cta_height = 160  # Height for CTA bar
        cta_img = os.path.join(TEMP_DIR, f"cta_{clip_id}.png")

        # Create a transparent background for CTA
        cta_background = Image.new('RGBA', (target_width, cta_height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(cta_background)

        # Different font size for CTA (smaller)
        cta_font_size = 45  # Smaller size for CTA

        try:
            font_path = os.path.join(DOWNLOAD_DIR, "fonts", "Poppins-Bold.ttf")
            font_cta = ImageFont.truetype(font_path, cta_font_size)
        except:
            font_cta = ImageFont.load_default()

        # Apply the same styling for CTA
        cta_lines = text_cta.split('\n')
        cta_line_heights = []
        cta_total_height = 0
        cta_max_width = 0

        for line in cta_lines:
            line_bbox = draw.textbbox((0, 0), line, font=font_cta)
            line_width = line_bbox[2] - line_bbox[0]
            line_height = line_bbox[3] - line_bbox[1]
            cta_line_heights.append((line, line_width, line_height))
            cta_total_height += line_height
            cta_max_width = max(cta_max_width, line_width)

        if len(cta_lines) > 1:
            cta_total_height += line_gap * (len(cta_lines) - 1)

        # Make sure padding is equal on top and bottom
        v_padding = 15  # Equal padding for top and bottom, reduced for better alignment

        # Calculate CTA rectangle dimensions with equal padding
        cta_rect_width = cta_max_width + (2 * h_padding)
        cta_rect_height = cta_total_height + (2 * v_padding)  # Equal padding on top and bottom

        # Make sure the white box is not too wide (max 80% of screen width)
        cta_rect_width = min(cta_rect_width, int(target_width * 0.8))

        # Calculate positions for the CTA rectangle
        left_offset = 40  # Adjust this value to control how far left the CTA appears
        cta_rect_x = ((target_width - cta_rect_width) // 2) - left_offset
        cta_rect_y = (cta_height - cta_rect_height) // 2

        # Draw rounded rectangle for CTA
        cta_rounded_rect = Image.new('RGBA', (cta_rect_width, cta_rect_height), (255, 255, 255, 255))
        cta_rounded_mask = Image.new('L', (cta_rect_width, cta_rect_height), 0)
        cta_rounded_draw = ImageDraw.Draw(cta_rounded_mask)

        # Draw the rounded rectangle on the mask
        cta_rounded_draw.rectangle([(corner_radius, 0), (cta_rect_width - corner_radius, cta_rect_height)], fill=255)
        cta_rounded_draw.rectangle([(0, corner_radius), (cta_rect_width, cta_rect_height - corner_radius)], fill=255)

        # Draw four circles at corners to create rounded effect
        cta_rounded_draw.pieslice([(0, 0), (corner_radius * 2, corner_radius * 2)], 180, 270, fill=255)
        cta_rounded_draw.pieslice([(cta_rect_width - corner_radius * 2, 0), (cta_rect_width, corner_radius * 2)], 270,
                                  360, fill=255)
        cta_rounded_draw.pieslice([(0, cta_rect_height - corner_radius * 2), (corner_radius * 2, cta_rect_height)], 90,
                                  180, fill=255)
        cta_rounded_draw.pieslice([(cta_rect_width - corner_radius * 2, cta_rect_height - corner_radius * 2),
                                   (cta_rect_width, cta_rect_height)], 0, 90, fill=255)

        # Apply the mask to the rectangle
        cta_rounded_rect.putalpha(cta_rounded_mask)

        # Paste the rounded rectangle onto the CTA background
        cta_background.paste(cta_rounded_rect, (cta_rect_x, cta_rect_y), cta_rounded_rect)

        # Calculate CTA text block height with line gaps
        cta_text_block_height = cta_total_height
        if len(cta_lines) > 1:
            cta_text_block_height += line_gap * (len(cta_lines) - 1)

        # Center text vertically within the rectangle with slight upward adjustment
        vertical_offset = 4  # Same offset as for title text
        cta_text_y = cta_rect_y + ((cta_rect_height - cta_text_block_height) // 2) - vertical_offset
        current_y = cta_text_y

        # Draw each line centered horizontally
        for i, (line, line_width, line_height) in enumerate(cta_line_heights):
            text_x = cta_rect_x + (cta_rect_width - line_width) // 2
            draw.text((text_x, current_y), line, fill=(0, 0, 0, 255), font=font_cta)
            if i < len(cta_line_heights) - 1:
                current_y += line_height + line_gap

        # Save CTA image
        cta_background.save(cta_img, "PNG")

        # Calculate position for bottom CTA
        bottom_position = target_height - cta_height - 400

        # Use a single ffmpeg command with the 'enable' parameter in the overlay filter
        # The 'gte(t,5)' condition makes the overlay only appear after 5 seconds
        cta_cmd = [
            'ffmpeg', '-y',
            '-i', with_title,
            '-i', cta_img,
            '-filter_complex', f'[0:v][1:v]overlay=0:{bottom_position}:enable=\'gte(t,5)\'',  # Only show after 5 seconds
            '-c:a', 'copy',
            with_bottom_cta
        ]
        logger.info(f"Running CTA command")
        subprocess.run(cta_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Step 4: We're no longer using video CTA - just copy the file with the bottom CTA
        shutil.copy(with_bottom_cta, final_output)

        logger.info(f"YouTube Short created: {final_output}")

        # Clean up temp files
        for tmp_file in [temp_clip, with_black_bars, with_title, with_bottom_cta, title_img, cta_img]:
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except:
                    pass  # Ignore errors during cleanup

        return {
            "path": final_output,
            "title": title,
            "clip_id": clip_id
        }

    except Exception as e:
        logger.error(f"Error creating YouTube Short: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_instagram_reel(clip_id, music_track, text_cta_type, desc_cta_type):
    """Process a video specifically for Instagram Reels format"""
    # Download the clip file from S3
    clip_path = download_clip(clip_id, is_short=True)
    if not clip_path:
        logger.error(f"Error: Clip file not found for: {clip_id}")
        return None

    # Load title for the reel
    titles = load_titles(is_short=True)
    if clip_id not in titles or not titles[clip_id]:
        logger.warning(f"No title found for clip {clip_id}")
        title = f"{extract_creator_name(clip_id)} on\nCreating Viral Content"
    else:
        title = format_title_into_two_lines(titles[clip_id][0])

    logger.info(f"Processing Instagram Reel: {clip_id}")
    logger.info(f"Title: {title}")

    # Get video info
    width, height, duration = get_video_info(clip_path)
    if not width or not height or not duration:
        logger.error(f"Error: Could not get video info for {clip_path}")
        return None

    # Replace creator name in text CTA
    creator_name = extract_creator_name(clip_id)
    text_cta = REELS_TEXT_CTAS.get(text_cta_type, "").replace("{Creator Name}", creator_name)

    # Get description CTA for Instagram
    description = REELS_DESC_CTAS.get(desc_cta_type, "")

    # Create temp files
    temp_clip = os.path.join(TEMP_DIR, f"temp_clip_{clip_id}.mp4")

    # Create a final output file with correct format
    final_output = os.path.join(OUTPUT_DIR, f"{clip_id}_instagram_reel.mp4")

    try:
        # Copy the clip to temp directory
        shutil.copy(clip_path, temp_clip)

        # Add music if specified
        if music_track:
            temp_with_music = os.path.join(TEMP_DIR, f"temp_with_music_{clip_id}.mp4")
            add_music_to_video(temp_clip, music_track, temp_with_music)
            # Replace temp_clip with the version with music
            if os.path.exists(temp_with_music):
                os.remove(temp_clip)
                os.rename(temp_with_music, temp_clip)
                logger.info(f"Added music track {music_track} to clip")

        # Format reels to look like example image with top title and bottom CTA
        # Get clip info for proper sizing
        clip_width, clip_height, _ = get_video_info(temp_clip)

        # Create a 9:16 aspect ratio video with black bars
        target_height = int(clip_width * 16 / 9) if clip_width > clip_height else clip_height
        target_width = int(target_height * 9 / 16) if clip_height > clip_width else clip_width

        # Creating temporary files for our processing
        with_black_bars = os.path.join(TEMP_DIR, f"with_black_bars_{clip_id}.mp4")
        with_title = os.path.join(TEMP_DIR, f"with_title_{clip_id}.mp4")
        with_bottom_cta = os.path.join(TEMP_DIR, f"with_bottom_cta_{clip_id}.mp4")

        # Step 1: Add black bars to make it 9:16 if needed
        if clip_width > clip_height:  # Landscape video
            vertical_padding = (target_height - clip_height) // 2
            pad_cmd = [
                'ffmpeg', '-y',
                '-i', temp_clip,
                '-vf', f'pad=width={clip_width}:height={target_height}:x=0:y={vertical_padding}:color=black',
                '-c:a', 'copy',
                with_black_bars
            ]
            subprocess.run(pad_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:  # Already portrait
            shutil.copy(temp_clip, with_black_bars)

        # Step 2: Add title at the top with white rounded rectangle background
        title_height = 170  # Height for title bar
        title_img = os.path.join(TEMP_DIR, f"title_{clip_id}.png")

        # Create a transparent background image
        title_background = Image.new('RGBA', (target_width, title_height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(title_background)

        # Different font size for title (larger)
        title_font_size = 60  # Large size for title

        try:
            font_path = os.path.join(DOWNLOAD_DIR, "fonts", "Poppins-Bold.ttf")
            font_title = ImageFont.truetype(font_path, title_font_size)
        except:
            font_title = ImageFont.load_default()

        # Apply same styling for title as with YouTube shorts
        title_lines = title.split('\n')
        line_gap = 20
        line_heights = []
        total_text_height = 0
        max_line_width = 0

        for line in title_lines:
            line_bbox = draw.textbbox((0, 0), line, font=font_title)
            line_width = line_bbox[2] - line_bbox[0]
            line_height = line_bbox[3] - line_bbox[1]
            line_heights.append((line, line_width, line_height))
            total_text_height += line_height
            max_line_width = max(max_line_width, line_width)

        # Add gaps between lines
        if len(title_lines) > 1:
            total_text_height += line_gap * (len(title_lines) - 1)

        # Add padding around text for the white rounded rectangle
        h_padding = 40  # Horizontal padding
        v_padding_top = 15  # Less padding for better vertical alignment
        v_padding_bottom = 15  # Equal padding

        # Calculate the white background rectangle dimensions
        rect_width = max_line_width + (2 * h_padding)
        rect_height = total_text_height + v_padding_top + v_padding_bottom

        # Make sure the white box is not too wide (max 80% of screen width)
        rect_width = min(rect_width, int(target_width * 0.8))

        # Calculate positions for the white rectangle
        rect_x = (target_width - rect_width) // 2
        rect_y = (title_height - rect_height) // 2

        # Draw rounded rectangle with white background
        corner_radius = 25  # Increased for more rounded corners
        rounded_rect = Image.new('RGBA', (rect_width, rect_height), (255, 255, 255, 255))
        rounded_rect_mask = Image.new('L', (rect_width, rect_height), 0)
        rounded_rect_draw = ImageDraw.Draw(rounded_rect_mask)

        # Draw the rounded rectangle on the mask
        rounded_rect_draw.rectangle([(corner_radius, 0), (rect_width - corner_radius, rect_height)], fill=255)
        rounded_rect_draw.rectangle([(0, corner_radius), (rect_width, rect_height - corner_radius)], fill=255)

        # Draw four circles at corners to create rounded effect
        rounded_rect_draw.pieslice([(0, 0), (corner_radius * 2, corner_radius * 2)], 180, 270, fill=255)
        rounded_rect_draw.pieslice([(rect_width - corner_radius * 2, 0), (rect_width, corner_radius * 2)], 270, 360,
                               fill=255)
        rounded_rect_draw.pieslice([(0, rect_height - corner_radius * 2), (corner_radius * 2, rect_height)], 90, 180,
                               fill=255)
        rounded_rect_draw.pieslice([(rect_width - corner_radius * 2, rect_height - corner_radius * 2),
                               (rect_width, rect_height)], 0, 90, fill=255)

        # Apply the mask to the rectangle
        rounded_rect.putalpha(rounded_rect_mask)

        # Paste the rounded rectangle onto the title background
        title_background.paste(rounded_rect, (rect_x, rect_y), rounded_rect)

        # Calculate text block height with line gaps
        text_block_height = total_text_height
        if len(title_lines) > 1:
            text_block_height += line_gap * (len(title_lines) - 1)

        # Calculate starting y position with a slight upward offset for better positioning
        vertical_offset = 4  # Slight upward shift to make text appear more centered
        text_y = rect_y + ((rect_height - text_block_height) // 2) - vertical_offset
        current_y = text_y

        # Draw each line centered horizontally
        for i, (line, line_width, line_height) in enumerate(line_heights):
            text_x = rect_x + (rect_width - line_width) // 2
            draw.text((text_x, current_y), line, fill=(0, 0, 0, 255), font=font_title)
            if i < len(line_heights) - 1:
                current_y += line_height + line_gap

        # Save title image
        title_background.save(title_img, "PNG")

        # Add title to the video
        title_cmd = [
            'ffmpeg', '-y',
            '-i', with_black_bars,
            '-i', title_img,
            '-filter_complex', '[0:v][1:v]overlay=0:400',  # Position at top with 400px padding
            '-c:a', 'copy',
            with_title
        ]
        subprocess.run(title_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Step 3: Add bottom text CTA with similar styling to shorts
        cta_height = 160  # Height for CTA bar
        cta_img = os.path.join(TEMP_DIR, f"cta_{clip_id}.png")

        # Create a transparent background for CTA
        cta_background = Image.new('RGBA', (target_width, cta_height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(cta_background)

        # Use the same font size for CTA as in shorts
        cta_font_size = 45

        try:
            font_path = os.path.join(DOWNLOAD_DIR, "fonts", "Poppins-Bold.ttf")
            font_cta = ImageFont.truetype(font_path, cta_font_size)
        except:
            font_cta = ImageFont.load_default()

        # Apply the same styling for CTA as with shorts
        cta_lines = text_cta.split('\n')
        cta_line_heights = []
        cta_total_height = 0
        cta_max_width = 0

        for line in cta_lines:
            line_bbox = draw.textbbox((0, 0), line, font=font_cta)
            line_width = line_bbox[2] - line_bbox[0]
            line_height = line_bbox[3] - line_bbox[1]
            cta_line_heights.append((line, line_width, line_height))
            cta_total_height += line_height
            cta_max_width = max(cta_max_width, line_width)

        if len(cta_lines) > 1:
            cta_total_height += line_gap * (len(cta_lines) - 1)

        # Add padding around text
        h_padding = 40  # Horizontal padding
        v_padding = 15  # Vertical padding (reduced for better alignment)

        # Calculate CTA rectangle dimensions
        cta_rect_width = cta_max_width + (2 * h_padding)
        cta_rect_height = cta_total_height + (2 * v_padding)  # Equal padding on top and bottom

        # Make sure the white box is not too wide (max 80% of screen width)
        cta_rect_width = min(cta_rect_width, int(target_width * 0.8))

        # Calculate positions for the CTA rectangle
        # Use a left offset as in shorts to create asymmetric look
        left_offset = 40  # Adjust this value to control how far left the CTA appears
        cta_rect_x = ((target_width - cta_rect_width) // 2) - left_offset
        cta_rect_y = (cta_height - cta_rect_height) // 2

        # Draw rounded rectangle for CTA with same design as shorts
        cta_rounded_rect = Image.new('RGBA', (cta_rect_width, cta_rect_height), (255, 255, 255, 255))
        cta_rounded_mask = Image.new('L', (cta_rect_width, cta_rect_height), 0)
        cta_rounded_draw = ImageDraw.Draw(cta_rounded_mask)

        # Draw the rounded rectangle on the mask
        cta_rounded_draw.rectangle([(corner_radius, 0), (cta_rect_width - corner_radius, cta_rect_height)], fill=255)
        cta_rounded_draw.rectangle([(0, corner_radius), (cta_rect_width, cta_rect_height - corner_radius)], fill=255)

        # Draw four circles at corners to create rounded effect
        cta_rounded_draw.pieslice([(0, 0), (corner_radius * 2, corner_radius * 2)], 180, 270, fill=255)
        cta_rounded_draw.pieslice([(cta_rect_width - corner_radius * 2, 0), (cta_rect_width, corner_radius * 2)], 270,
                              360, fill=255)
        cta_rounded_draw.pieslice([(0, cta_rect_height - corner_radius * 2), (corner_radius * 2, cta_rect_height)], 90,
                              180, fill=255)
        cta_rounded_draw.pieslice([(cta_rect_width - corner_radius * 2, cta_rect_height - corner_radius * 2),
                               (cta_rect_width, cta_rect_height)], 0, 90, fill=255)

        # Apply the mask to the rectangle
        cta_rounded_rect.putalpha(cta_rounded_mask)

        # Paste the rounded rectangle onto the CTA background
        cta_background.paste(cta_rounded_rect, (cta_rect_x, cta_rect_y), cta_rounded_rect)

        # Calculate CTA text block height with line gaps
        cta_text_block_height = cta_total_height
        if len(cta_lines) > 1:
            cta_text_block_height += line_gap * (len(cta_lines) - 1)

        # Center text vertically within the rectangle with slight upward adjustment
        vertical_offset = 4  # Same offset as for title text
        cta_text_y = cta_rect_y + ((cta_rect_height - cta_text_block_height) // 2) - vertical_offset
        current_y = cta_text_y

        # Draw each line centered horizontally
        for i, (line, line_width, line_height) in enumerate(cta_line_heights):
            text_x = cta_rect_x + (cta_rect_width - line_width) // 2
            draw.text((text_x, current_y), line, fill=(0, 0, 0, 255), font=font_cta)
            if i < len(cta_line_heights) - 1:
                current_y += line_height + line_gap

        # Save CTA image
        cta_background.save(cta_img, "PNG")

        # Calculate position for bottom CTA
        # Position at the bottom with some padding
        bottom_position = target_height - cta_height - 400

        # Use the same approach as shorts with timed overlay
        cta_cmd = [
            'ffmpeg', '-y',
            '-i', with_title,
            '-i', cta_img,
            '-filter_complex',
            # Only show after 5 seconds - simplified command
            f'[0:v][1:v]overlay=0:{bottom_position}:enable=\'gte(t,5)\'',
            '-c:a', 'copy',
            with_bottom_cta
        ]
        subprocess.run(cta_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # For Instagram reels, we don't add a final video CTA
        # Just use the version with the title and bottom CTA
        shutil.copy(with_bottom_cta, final_output)

        logger.info(f"Instagram Reel created: {final_output}")

        # Clean up temp files
        for tmp_file in [temp_clip, with_black_bars, with_title, with_bottom_cta, title_img, cta_img]:
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except:
                    pass  # Ignore errors during cleanup

        return {
            "path": final_output,
            "title": title,
            "description": description,
            "clip_id": clip_id
        }

    except Exception as e:
        logger.error(f"Error creating Instagram Reel: {e}")
        import traceback
        traceback.print_exc()
        return None


def process_day(config, day_key, tracking_data):
    """Process videos for a specific day across all channels"""
    success_count = 0
    error_count = 0

    # Process all YouTube channels
    for channel_id in config.get("youtubeChannels", {}):
        try:
            channel_data = config["youtubeChannels"][channel_id]

            if day_key not in channel_data:
                logger.info(f"No content scheduled for {channel_id} on {day_key}")
                continue

            day_data = channel_data[day_key]

            # Process long video
            long_data = day_data.get("long", {})
            if long_data:
                clip_id = long_data.get("clip")
                text_cta = long_data.get("textCTA")
                video_cta = long_data.get("videoCTA")
                title_index = long_data.get("title")
                post_time = long_data.get("postTime")

                if clip_id and text_cta and video_cta and post_time:
                    logger.info(f"\nProcessing LONG video for {channel_id}: {clip_id}")
                    logger.info(f"  Text CTA: {text_cta}")
                    logger.info(f"  Video CTA: {video_cta}")
                    logger.info(f"  Post Time: {post_time}")

                    long_video = create_long_video(clip_id, text_cta, video_cta, title_index)
                    if long_video:
                        schedule_post("YouTube", "long", channel_id, None, long_video, post_time, config, tracking_data)
                        success_count += 1
                    else:
                        logger.error(f"Failed to create long video for {channel_id}: {clip_id}")
                        error_count += 1

            # Process YouTube shorts
            shorts_data = day_data.get("shorts", [])
            for i, short in enumerate(shorts_data):
                clip_id = short.get("clip")
                music_track = short.get("musicTrack")
                text_cta = short.get("textCTA")
                video_cta = short.get("videoCTA")
                post_time = short.get("postTime")

                if clip_id and text_cta and video_cta and post_time:
                    logger.info(f"\nProcessing YOUTUBE SHORT {i + 1}/{len(shorts_data)} for {channel_id}: {clip_id}")
                    logger.info(f"  Music: {music_track}")
                    logger.info(f"  Text CTA: {text_cta}")
                    logger.info(f"  Video CTA: {video_cta}")
                    logger.info(f"  Post Time: {post_time}")

                    short_video = create_youtube_short(clip_id, music_track, text_cta, video_cta)
                    if short_video:
                        schedule_post("YouTube", "short", channel_id, None, short_video, post_time, config,
                                      tracking_data)
                        success_count += 1
                    else:
                        logger.error(f"Failed to create YouTube short for {channel_id}: {clip_id}")
                        error_count += 1

        except Exception as e:
            logger.error(f"Error processing YouTube channel {channel_id} for {day_key}: {e}")
            error_count += 1

    # Process all Instagram accounts
    for account_id in config.get("instagramAccounts", {}):
        try:
            account_data = config["instagramAccounts"][account_id]

            if day_key not in account_data:
                logger.info(f"No content scheduled for Instagram {account_id} on {day_key}")
                continue

            ig_data = account_data[day_key]
            reels_data = ig_data.get("reels", [])

            for i, reel in enumerate(reels_data):
                clip_id = reel.get("clip")
                music_track = reel.get("musicTrack")
                text_cta = reel.get("textCTA")
                desc_cta = reel.get("descriptionCTA")
                post_time = reel.get("postTime")

                if clip_id and text_cta and post_time:
                    logger.info(f"\nProcessing INSTAGRAM REEL {i + 1}/{len(reels_data)} for {account_id}: {clip_id}")
                    logger.info(f"  Music: {music_track}")
                    logger.info(f"  Text CTA: {text_cta}")
                    logger.info(f"  Description CTA: {desc_cta}")
                    logger.info(f"  Post Time: {post_time}")

                    reel_video = create_instagram_reel(clip_id, music_track, text_cta, desc_cta)
                    if reel_video:
                        # Map Instagram account to YouTube channel (assuming account1 corresponds to channel1)
                        channel_id = f"channel{account_id.replace('account', '')}"
                        schedule_post("Instagram", "reel", channel_id, account_id, reel_video, post_time, config,
                                      tracking_data)
                        success_count += 1
                    else:
                        logger.error(f"Failed to create Instagram reel for {account_id}: {clip_id}")
                        error_count += 1

        except Exception as e:
            logger.error(f"Error processing Instagram account {account_id} for {day_key}: {e}")
            error_count += 1

    return success_count, error_count


def cleanup():
    """Clean up temporary files and directories"""
    try:
        for dir_path in [TEMP_DIR, DOWNLOAD_DIR]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
        logger.info("Cleaned up temporary files and directories")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def lambda_handler(event, context):
    """AWS Lambda handler function with chunked processing"""
    try:
        # Add PIL path fix
        import sys
        import os

        def fix_pillow_path():
            for path in sys.path:
                if os.path.isdir(os.path.join(path, 'PIL')):
                    sys.path.append(os.path.join(path, 'PIL'))
                    logger.info(f"Added PIL path: {os.path.join(path, 'PIL')}")
                    break

        # Apply the PIL path fix
        fix_pillow_path()

        # Check instagram-private-api version
        try:
            instagram_version = pkg_resources.get_distribution("instagram_private_api").version
            logger.info(f"Using instagram-private-api version: {instagram_version}")

            # Add compatibility fix for older version if needed
            if instagram_version == "1.6.0.0":
                logger.info("Using compatibility mode for instagram-private-api 1.6.0.0")
        except Exception as e:
            logger.warning(f"Could not determine instagram-private-api version: {e}")

        logger.info("Starting social media autoposter Lambda function")

        # Set up necessary directories
        setup_directories()

        # Download fonts and assets
        download_fonts_and_assets()

        # Load configuration
        config = load_config_from_s3()
        if not config:
            logger.error("Failed to load configuration")
            return {
                "statusCode": 500,
                "body": "Failed to load configuration"
            }

        # Load tracking data
        tracking_data = load_or_create_tracking_data()

        # Initialize chunked processing fields if they don't exist
        if "chunked_processing" not in tracking_data:
            tracking_data["chunked_processing"] = {
                "active_day": None,
                "channels_processed": [],
                "channels_pending": []
            }

        # Check if we're in the middle of processing a day
        chunked = tracking_data["chunked_processing"]
        if chunked["active_day"]:
            # We're in the middle of processing a day
            day_to_process = chunked["active_day"]
            logger.info(f"Continuing to process {day_to_process}")
        else:
            # Start processing a new day
            day_to_process = determine_processing_day(tracking_data, config)
            if not day_to_process:
                logger.info("No day to process at this time")
                return {
                    "statusCode": 200,
                    "body": "No day to process at this time"
                }

            # Initialize tracking for the new day
            chunked["active_day"] = day_to_process
            chunked["channels_processed"] = []

            # Get all YouTube channels and Instagram accounts to process
            all_channels = list(config.get("youtubeChannels", {}).keys())
            all_accounts = [f"instagram_{acc}" for acc in config.get("instagramAccounts", {}).keys()]
            chunked["channels_pending"] = all_channels + all_accounts

            update_tracking_data(tracking_data)
            logger.info(
                f"Starting to process {day_to_process} with {len(chunked['channels_pending'])} channels/accounts")

        # Process videos in chunks to stay within Lambda time limits
        success_count = 0
        error_count = 0

        # Get time remaining (leave 30 seconds buffer)
        start_time = time.time()
        max_process_time = 840  # 14 minutes in seconds (15 min limit - 1 min buffer)

        # Process channels until we're out of time or done with all channels
        while chunked["channels_pending"] and (time.time() - start_time) < max_process_time:
            # Get the next channel/account to process
            current = chunked["channels_pending"][0]

            if current.startswith("instagram_"):
                # Process Instagram account
                account_id = current.replace("instagram_", "")
                logger.info(f"Processing Instagram account {account_id} for {day_to_process}")

                try:
                    account_data = config["instagramAccounts"][account_id]
                    if day_to_process in account_data:
                        ig_data = account_data[day_to_process]
                        reels_data = ig_data.get("reels", [])

                        for i, reel in enumerate(reels_data):
                            clip_id = reel.get("clip")
                            music_track = reel.get("musicTrack")
                            text_cta = reel.get("textCTA")
                            desc_cta = reel.get("descriptionCTA")
                            post_time = reel.get("postTime")

                            if clip_id and text_cta and post_time:
                                logger.info(f"Processing IG Reel {i + 1}/{len(reels_data)}: {clip_id}")
                                reel_video = create_instagram_reel(clip_id, music_track, text_cta, desc_cta)
                                if reel_video:
                                    # Map IG account to YT channel
                                    channel_id = f"channel{account_id.replace('account', '')}"
                                    schedule_post("Instagram", "reel", channel_id, account_id, reel_video, post_time,
                                                  config, tracking_data)
                                    success_count += 1
                                else:
                                    error_count += 1

                            # Check time after each video
                            if (time.time() - start_time) >= max_process_time:
                                logger.info("Time limit approaching, pausing processing")
                                break

                except Exception as e:
                    logger.error(f"Error processing Instagram account {account_id}: {e}")
                    error_count += 1
            else:
                # Process YouTube channel
                channel_id = current
                logger.info(f"Processing YouTube channel {channel_id} for {day_to_process}")

                try:
                    channel_data = config["youtubeChannels"][channel_id]
                    if day_to_process in channel_data:
                        day_data = channel_data[day_to_process]

                        # Process long video
                        long_data = day_data.get("long", {})
                        if long_data:
                            clip_id = long_data.get("clip")
                            text_cta = long_data.get("textCTA")
                            video_cta = long_data.get("videoCTA")
                            title_index = long_data.get("title")
                            post_time = long_data.get("postTime")

                            if clip_id and text_cta and video_cta and post_time:
                                logger.info(f"Processing long video: {clip_id}")
                                long_video = create_long_video(clip_id, text_cta, video_cta, title_index)
                                if long_video:
                                    schedule_post("YouTube", "long", channel_id, None, long_video, post_time, config,
                                                  tracking_data)
                                    success_count += 1
                                else:
                                    error_count += 1

                        # Check time after processing long video
                        if (time.time() - start_time) >= max_process_time:
                            logger.info("Time limit approaching, pausing processing")
                            break

                        # Process YouTube shorts
                        shorts_data = day_data.get("shorts", [])
                        for i, short in enumerate(shorts_data):
                            clip_id = short.get("clip")
                            music_track = short.get("musicTrack")
                            text_cta = short.get("textCTA")
                            video_cta = short.get("videoCTA")
                            post_time = short.get("postTime")

                            if clip_id and text_cta and video_cta and post_time:
                                logger.info(f"Processing YT Short {i + 1}/{len(shorts_data)}: {clip_id}")
                                short_video = create_youtube_short(clip_id, music_track, text_cta, video_cta)
                                if short_video:
                                    schedule_post("YouTube", "short", channel_id, None, short_video, post_time, config,
                                                  tracking_data)
                                    success_count += 1
                                else:
                                    error_count += 1

                            # Check time after each short
                            if (time.time() - start_time) >= max_process_time:
                                logger.info("Time limit approaching, pausing processing")
                                break

                except Exception as e:
                    logger.error(f"Error processing YouTube channel {channel_id}: {e}")
                    error_count += 1

            # Mark this channel as processed
            chunked["channels_processed"].append(current)
            chunked["channels_pending"].remove(current)
            update_tracking_data(tracking_data)

        # Check if we've completed all channels for this day
        if not chunked["channels_pending"]:
            logger.info(f"Completed processing all channels for {day_to_process}")
            # Reset chunked processing for next day
            tracking_data["chunked_processing"]["active_day"] = None
            tracking_data["chunked_processing"]["channels_processed"] = []
            tracking_data["chunked_processing"]["channels_pending"] = []
            # Update last processed day
            tracking_data["last_processed_day"] = datetime.datetime.now().strftime("%Y-%m-%d")
            tracking_data["last_processed_key"] = day_to_process
        else:
            logger.info(f"Partially processed {day_to_process}. Will continue in next execution.")
            logger.info(f"Processed: {chunked['channels_processed']}")
            logger.info(f"Pending: {chunked['channels_pending']}")

        # Update tracking data
        update_tracking_data(tracking_data)

        # Clean up
        cleanup()

        return {
            "statusCode": 200,
            "body": f"Processed {success_count} videos successfully, {error_count} failures. More to process: {len(chunked['channels_pending'])}."
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        import traceback
        traceback.print_exc()

        # Clean up even if there's an error
        cleanup()

        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }


def initialize_youtube_client(channel_id, config):
    """Initialize a YouTube API client for a specific channel"""
    if channel_id in youtube_clients:
        return youtube_clients[channel_id]

    try:
        # Get API credentials from config
        channel_config = config["youtubeChannels"][channel_id]
        credentials = channel_config.get("credentials", {})

        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        refresh_token = credentials.get("refresh_token")

        if not client_id or not client_secret or not refresh_token:
            logger.error(f"Missing YouTube API credentials for channel {channel_id}")
            return None

        # Set up API client
        import google.oauth2.credentials
        import googleapiclient.discovery

        credentials = google.oauth2.credentials.Credentials(
            None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret
        )

        youtube = googleapiclient.discovery.build("youtube", "v3", credentials=credentials)
        youtube_clients[channel_id] = youtube

        logger.info(f"Initialized YouTube client for channel {channel_id}")
        return youtube

    except Exception as e:
        logger.error(f"Error initializing YouTube client for {channel_id}: {e}")
        return None


def initialize_instagram_client(account_id, config):
    """Initialize an Instagram API client for a specific account"""
    if account_id in instagram_clients:
        return instagram_clients[account_id]

    try:
        # Get API credentials from config
        account_config = config["instagramAccounts"][account_id]
        credentials = account_config.get("credentials", {})

        username = credentials.get("username")
        password = credentials.get("password")

        if not username or not password:
            logger.error(f"Missing Instagram API credentials for account {account_id}")
            return None

        # Set up API client
        client = Client(username, password)
        instagram_clients[account_id] = client

        logger.info(f"Initialized Instagram client for account {account_id}")
        return client

    except Exception as e:
        logger.error(f"Error initializing Instagram client for {account_id}: {e}")
        return None


def post_to_youtube(channel_id, file_path, title, description="", is_short=False, config=None):
    """Post a video to YouTube using the YouTube API"""
    youtube = initialize_youtube_client(channel_id, config)
    if not youtube:
        logger.error(f"Cannot post to YouTube for channel {channel_id}: No YouTube client")
        return {"status": "error", "message": "YouTube client initialization failed"}

    try:
        # Prepare video upload
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "categoryId": "22"  # People & Blogs
            },
            "status": {
                "privacyStatus": "public",
                "selfDeclaredMadeForKids": False
            }
        }

        # For shorts, add special tags
        if is_short:
            body["snippet"]["tags"] = ["#shorts"]

        logger.info(f"Uploading video to YouTube: {title}")
        logger.info(f"File: {file_path}")

        # Create upload request
        media = MediaFileUpload(file_path,
                                mimetype="video/mp4",
                                resumable=True)

        # Execute the upload request
        request = youtube.videos().insert(
            part=",".join(body.keys()),
            body=body,
            media_body=media
        )

        response = request.execute()

        # Create response object
        result = {
            "status": "success",
            "platform": "YouTube Shorts" if is_short else "YouTube",
            "video_id": response["id"],
            "url": f"https://youtube.com/watch?v={response['id']}"
        }

        logger.info(f"Successfully uploaded to YouTube: {result['url']}")
        return result

    except Exception as e:
        logger.error(f"Error posting to YouTube: {e}")
        return {
            "status": "error",
            "platform": "YouTube Shorts" if is_short else "YouTube",
            "message": str(e)
        }


def post_to_instagram(account_id, file_path, title, description="", config=None):
    """Post a video to Instagram as a Reel using the Instagram API"""
    client = initialize_instagram_client(account_id, config)
    if not client:
        logger.error(f"Cannot post to Instagram for account {account_id}: No Instagram client")
        return {"status": "error", "message": "Instagram client initialization failed"}

    try:
        # Prepare and upload video
        logger.info(f"Uploading video to Instagram Reels: {title}")
        logger.info(f"File: {file_path}")

        # Combine title and description
        caption = f"{description}\n\n{title}"

        # Upload video as reel
        result = client.post_video(file_path, caption, to_reel=True)

        media_id = result.get("media", {}).get("id", "unknown")
        code = result.get("media", {}).get("code", "unknown")

        # Create response object
        response = {
            "status": "success",
            "platform": "Instagram Reels",
            "media_id": media_id,
            "url": f"https://instagram.com/p/{code}"
        }

        logger.info(f"Successfully uploaded to Instagram: {response['url']}")
        return response

    except Exception as e:
        logger.error(f"Error posting to Instagram: {e}")
        return {
            "status": "error",
            "platform": "Instagram Reels",
            "message": str(e)
        }


def schedule_post(platform, content_type, channel_id, account_id, file_info, post_time, config, tracking_data):
    """Schedule a post for the specified time"""
    # Parse the post time
    try:
        hour, minute = map(int, post_time.split(':'))
        current_time = datetime.datetime.now()
        post_datetime = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # If the time has already passed today, schedule for tomorrow
        if post_datetime < current_time:
            post_datetime += datetime.timedelta(days=1)

        # Calculate delay until post time
        delay_seconds = (post_datetime - current_time).total_seconds()

        # For testing/demo purposes, use a shorter delay
        if delay_seconds > 300:  # 5 minutes
            demo_delay = 60  # 1 minute
            logger.info(f"DEMO MODE: Using {demo_delay}s delay instead of waiting {delay_seconds / 60:.1f} minutes")
            delay_seconds = demo_delay

        # Create post data for tracking
        post_data = {
            "platform": platform,
            "content_type": content_type,
            "channel_id": channel_id,
            "account_id": account_id if platform.lower() == "instagram" else None,
            "title": file_info.get("title"),
            "clip_id": file_info.get("clip_id"),
            "file_path": file_info.get("path"),
            "scheduled_time": post_time,
            "actual_time": None,
            "post_id": None,
            "post_url": None,
            "status": "scheduled",
            "day": tracking_data.get("last_processed_key", "unknown")
        }

        # Add to tracking data
        if channel_id not in tracking_data.get("posts", {}):
            tracking_data["posts"][channel_id] = []

        tracking_data["posts"][channel_id].append(post_data)
        update_tracking_data(tracking_data)

        # Schedule the post
        logger.info(f"Scheduling {platform} {content_type} post for {post_time} " +
                    f"({delay_seconds / 60:.1f} minutes from now)")

        def post_job():
            try:
                post_data["actual_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                logger.info(f"\n{'=' * 50}")
                logger.info(f"POSTING {content_type.upper()} ON {platform.upper()} AT {post_data['actual_time']}")
                logger.info(f"{'=' * 50}")
                logger.info(f"  Title: {file_info.get('title', 'Unknown')}")
                logger.info(f"  File: {file_info.get('path', 'Unknown')}")

                response = None

                # Call the appropriate posting function based on platform and content type
                if platform.lower() == "youtube":
                    description = file_info.get("description", "")
                    is_short = content_type.lower() == "short"
                    response = post_to_youtube(channel_id, file_info.get("path"),
                                               file_info.get("title"), description, is_short, config)

                elif platform.lower() == "instagram":
                    description = file_info.get("description", "")
                    response = post_to_instagram(account_id, file_info.get("path"),
                                                 file_info.get("title"), description, config)

                # Update tracking data with response
                if response:
                    post_data["status"] = response.get("status", "unknown")
                    if response.get("status") == "success":
                        post_data["post_id"] = response.get("video_id", response.get("media_id", "unknown"))
                        post_data["post_url"] = response.get("url", "")

                        logger.info(f"\nPosting successful!")
                        logger.info(f"  Platform: {response.get('platform')}")
                        logger.info(f"  ID: {post_data['post_id']}")
                        logger.info(f"  URL: {post_data['post_url']}")
                    else:
                        post_data["error"] = response.get("message", "Unknown error")
                        logger.error(f"\nPosting failed: {post_data['error']}")
                else:
                    post_data["status"] = "error"
                    post_data["error"] = "No response from posting function"
                    logger.error("\nPosting failed: No response from posting function.")

                # Update tracking data
                update_tracking_data(tracking_data)

                logger.info(f"{'=' * 50}")
            except Exception as e:
                logger.error(f"Error in post_job: {e}")
                post_data["status"] = "error"
                post_data["error"] = str(e)
                update_tracking_data(tracking_data)

        # For AWS Lambda, we need to handle scheduling differently since the
        # process will terminate after execution. Using a delay here.
        time.sleep(delay_seconds)
        post_job()

        return True
    except Exception as e:
        logger.error(f"Error scheduling post: {e}")
        return False