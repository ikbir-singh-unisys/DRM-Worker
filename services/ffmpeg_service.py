# worker/services/ffmpeg_service.py
from typing import List, Dict
import logging
import subprocess
from pathlib import Path
import json

logger = logging.getLogger(__name__)

def validate_input_file(input_path: str) -> bool:
    """Validate that the input file exists and is a valid video."""
    input_path = Path(input_path)
    if not input_path.exists() or input_path.stat().st_size == 0:
        logger.error(f"Input file {input_path} does not exist or is empty.")
        return False
    try:
        cmd = [
            "ffprobe", "-v", "error", "-show_format",
            "-show_streams", "-of", "json", str(input_path)
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Input file {input_path} is valid.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Invalid input file {input_path}: {e.stderr}")
        return False

def has_audio_stream(input_path: str) -> bool:
    """Check if the input file has an audio stream."""
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "a",
            "-show_entries", "stream=index", "-of", "json", str(input_path)
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        streams = json.loads(result.stdout).get("streams", [])
        return len(streams) > 0
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error checking audio streams in {input_path}: {e.stderr}")
        return False

def get_video_stream_info(input_path: str) -> Dict:
    """Get video stream details (pixel format, profile)."""
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=pix_fmt,profile",
            "-of", "json", str(input_path)
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        stream_info = json.loads(result.stdout).get("streams", [{}])[0]
        return {
            "pix_fmt": stream_info.get("pix_fmt", ""),
            "profile": stream_info.get("profile", "").lower()
        }
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error getting video stream info for {input_path}: {e.stderr}")
        return {"pix_fmt": "", "profile": ""}

def select_transcode_params(stream_info: Dict) -> Dict:
    """Select H.264 profile and pixel format based on input."""
    pix_fmt = stream_info.get("pix_fmt", "")
    profile = stream_info.get("profile", "").lower()

    # If input is 4:2:2 or 10-bit, convert to yuv420p and use 'high' profile
    if "422" in pix_fmt or "10" in pix_fmt or "422" in profile:
        logger.info(f"Input has 4:2:2 or 10-bit ({pix_fmt}, {profile}). Using high profile and yuv420p.")
        return {
            "profile": "high",
            "pix_fmt": "yuv420p"
        }
    # Default to 'main' profile for 4:2:0 8-bit inputs
    return {
        "profile": "main",
        "pix_fmt": "yuv420p"
    }

# def transcode_video(input_path: str, output_dir: str) -> List[str]:
#     output_dir = Path(output_dir)
#     output_dir.mkdir(parents=True, exist_ok=True)

#     if not validate_input_file(input_path):
#         raise RuntimeError(f"Invalid input file: {input_path}")

#     has_audio = has_audio_stream(input_path)
#     logger.info(f"Input {input_path} has audio stream: {has_audio}")

#     stream_info = get_video_stream_info(input_path)
#     transcode_params = select_transcode_params(stream_info)
#     logger.info(f"Selected transcode params: profile={transcode_params['profile']}, pix_fmt={transcode_params['pix_fmt']}")

#     bitrate_settings = [
#         {"resolution": "1920x1080", "bitrate": "3000k"},
#         {"resolution": "1280x720", "bitrate": "2000k"},
#         {"resolution": "854x480", "bitrate": "1000k"},
#         {"resolution": "640x360", "bitrate": "600k"},
#     ]

#     outputs = []
#     for setting in bitrate_settings:
#         output_path = output_dir / f"video_{setting['resolution']}.mp4"
#         cmd = [
#             "ffmpeg", "-y", "-i", str(input_path),
#             "-c:v", "libx264", "-b:v", setting["bitrate"],
#             "-s", setting["resolution"], "-preset", "veryfast",
#             "-profile:v", transcode_params["profile"],
#             "-pix_fmt", transcode_params["pix_fmt"],
#             "-level", "4.0",
#         ]
#         if has_audio:
#             cmd.extend(["-c:a", "aac", "-b:a", "128k"])
#         else:
#             cmd.append("-an")
#         cmd.append(str(output_path))

#         try:
#             result = subprocess.run(cmd, check=True, capture_output=True, text=True)
#             logger.info(f"Transcoded video to {output_path}")
#             outputs.append(str(output_path))
#         except subprocess.CalledProcessError as e:
#             logger.error(f"FFmpeg failed for {output_path}: {e.stderr}")
#             raise RuntimeError(f"FFmpeg transcoding failed: {e.stderr}")
#     return outputs


def transcode_video(input_path: str, output_dir: str) -> List[str]:
    """
    Transcode video into multiple resolutions with optimized FFmpeg settings for streaming.
    
    Args:
        input_path (str): Path to input video file.
        output_dir (str): Directory to save transcoded outputs.
        
    Returns:
        List[str]: List of paths to transcoded video files.
        
    Raises:
        RuntimeError: If input validation or FFmpeg transcoding fails.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Validate input file
    if not validate_input_file(input_path):
        logger.error(f"Invalid input file: {input_path}")
        raise RuntimeError(f"Invalid input file: {input_path}")

    # Check for audio stream
    has_audio = has_audio_stream(input_path)
    logger.info(f"Input {input_path} has audio stream: {has_audio}")

    # Get video stream info and transcode parameters
    stream_info = get_video_stream_info(input_path)
    transcode_params = select_transcode_params(stream_info)
    logger.info(f"Selected transcode params: profile={transcode_params['profile']}, pix_fmt={transcode_params['pix_fmt']}")

    # Define output settings for different resolutions
    bitrate_settings = [
        {"resolution": "1920x1080", "bitrate": "3000k", "output_name": "output_1080p.mp4"},
        {"resolution": "1280x720", "bitrate": "2000k", "output_name": "output_720p.mp4"},
        {"resolution": "854x480", "bitrate": "1000k", "output_name": "output_480p.mp4"},
        {"resolution": "640x360", "bitrate": "600k", "output_name": "output_360p.mp4"},
    ]

    outputs = []
    for setting in bitrate_settings:
        output_path = output_dir / setting["output_name"]
        
        # Base FFmpeg command
        cmd = [
            "ffmpeg", "-y", "-i", str(input_path),
            "-map", "0:v",  # Map video stream
            "-c:v", "libx264",
            "-preset", "medium",
            "-pix_fmt", transcode_params["pix_fmt"],
            "-r", "24",  # Frame rate
            "-b:v", setting["bitrate"],
            "-s:v", setting["resolution"],
            "-g", "48",  # GOP size (2 seconds at 24 fps)
            "-keyint_min", "48",  # Minimum keyframe interval
            "-sc_threshold", "0",  # Disable scene-based keyframes
            "-movflags", "+faststart",  # Optimize for web
            "-profile:v", transcode_params["profile"],
            "-level", "4.0",
        ]

        # Add audio if present
        if has_audio:
            cmd.extend(["-map", "0:a", "-c:a", "aac", "-b:a", "128k", "-ac", "2"])
        else:
            cmd.append("-an")  # No audio

        cmd.append(str(output_path))

        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8"  # Ensure UTF-8 for logs
            )
            logger.info(f"Transcoded video to {output_path}")
            outputs.append(str(output_path))
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg failed for {output_path}: {e.stderr}")
            raise RuntimeError(f"FFmpeg transcoding failed for {output_path}: {e.stderr}")
        except UnicodeEncodeError as e:
            logger.error(f"Encoding error during FFmpeg execution: {e}")
            raise RuntimeError(f"Encoding error: {e}")

    return outputs


def transcode_audio(input_path: str, output_dir: str, language: str, isPaid: bool) -> Dict[str, str]:
    output_dir = Path(output_dir) / language
    output_dir.mkdir(parents=True, exist_ok=True)
    
    bitrates = {
        "128k": ["-b:a", "128k"]
    }
    
    outputs = {}
    for br, params in bitrates.items():
        output_path = output_dir / f"{br}.{'mp4' if isPaid else 'aac'}"
        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-c:a", "aac", *params,
            "-vn"  # Exclude video for both MP4 and AAC
        ]
        if isPaid:
            cmd.append("-f")
            cmd.append("mp4")
        cmd.append(str(output_path))
        
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            outputs[br] = str(output_path).replace('\\', '/')  # Normalize path
            logger.info(f"Transcoded audio to {output_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg audio transcoding failed for {output_path}: {e.stderr}")
            raise RuntimeError(f"FFmpeg audio transcoding failed: {e.stderr}")
    return outputs

