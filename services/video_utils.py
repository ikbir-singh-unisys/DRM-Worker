# worker/services/video_utils.py
import subprocess
import json
import logging
import chardet
from typing import List, Dict
from pathlib import Path

logger = logging.getLogger(__name__)

def log_raw_bytes(file_path, max_bytes=50):
    """Log the first few bytes of a file in hex for debugging."""
    with open(file_path, "rb") as f:
        raw_data = f.read(max_bytes)
    hex_data = raw_data.hex()
    logger.info(f"First {max_bytes} bytes of {file_path} (hex): {hex_data}")

def detect_and_convert_srt_to_utf8(input_srt: str, output_srt: str) -> str:
    """Detect the encoding of the SRT file and convert it to UTF-8."""
    with open(input_srt, "rb") as f:
        raw_data = f.read()
    
    log_raw_bytes(input_srt)
    
    possible_encodings = ["utf-16", "windows-1252", "latin-1", "utf-8", "ascii"]
    chardet_result = chardet.detect(raw_data)
    chardet_encoding = chardet_result["encoding"]
    chardet_confidence = chardet_result["confidence"]
    logger.info(f"chardet detected encoding for {input_srt}: {chardet_encoding} (confidence: {chardet_confidence:.2f})")
    possible_encodings.insert(0, chardet_encoding)
    
    content = None
    detected_encoding = None

    for encoding in possible_encodings:
        try:
            content = raw_data.decode(encoding)
            detected_encoding = encoding
            logger.info(f"Successfully decoded {input_srt} with encoding: {encoding}")
            break
        except (UnicodeDecodeError, LookupError) as e:
            logger.error(f"Failed to decode {input_srt} with encoding {encoding}: {e}")
    
    if content is None:
        content = raw_data.decode("windows-1252", errors="replace")
        detected_encoding = "windows-1252 (fallback)"
        logger.info(f"Falling back to windows-1252 encoding with replacement for {input_srt}")
    
    with open(output_srt, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"Converted {input_srt} to UTF-8: {output_srt}")
    log_raw_bytes(output_srt)
    
    return detected_encoding

def convert_srt_to_vtt_batch(srt_paths: List[Dict[str, str]], output_dir: str) -> List[Dict[str, str]]:
    """Convert SRT files to WebVTT with encoding detection."""
    vtt_paths = []
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    if not srt_paths:
        logger.info("No SRT files provided, returning empty VTT paths.")
        return []

    for srt_path in srt_paths:
        if not isinstance(srt_path, dict) or 'file_path' not in srt_path or 'language' not in srt_path:
            logger.error(f"Invalid SRT path format: {srt_path}")
            raise ValueError(f"Expected dictionary with 'file_path' and 'language', got: {srt_path}")

        srt_file = Path(srt_path["file_path"])
        language = srt_path["language"]
        temp_utf8_srt = Path(output_dir) / f"{srt_file.stem}_utf8.srt"
        vtt_path = Path(output_dir) / f"{srt_file.stem}.vtt"

        # Convert SRT to UTF-8
        detected_encoding = detect_and_convert_srt_to_utf8(str(srt_file), str(temp_utf8_srt))
        
        # Convert UTF-8 SRT to WebVTT
        command = [
            "ffmpeg", "-y",
            "-sub_charenc", detected_encoding,
            "-i", str(temp_utf8_srt),
            "-c:s", "webvtt",
            str(vtt_path)
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Converted {temp_utf8_srt} to WebVTT: {vtt_path}")
        vtt_paths.append({"file_path": str(vtt_path), "language": language})

    return vtt_paths

def get_video_duration(file_path: str) -> float:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "format=duration",
                "-of", "json",
                file_path
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True
        )
        info = json.loads(result.stdout)
        duration = float(info["format"]["duration"])
        return duration
    except subprocess.CalledProcessError as e:
        logger.error(f"ffprobe error: {e.stderr}")
        raise
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing ffprobe output: {e}")
        raise