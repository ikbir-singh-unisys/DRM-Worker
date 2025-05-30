# worker/services/drm_service.py
import os
import subprocess
import logging
import shutil
from pathlib import Path
from typing import Union, List, Dict
import secrets
import json
import platform
import time

logger = logging.getLogger(__name__)

class DRMService:
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_hex_key(self) -> str:
        return secrets.token_hex(16)

    def save_keys_to_file(self, key: str, kid: str, cek: str) -> Path:
        key_file = self.output_dir / "drm_keys.txt"
        with open(key_file, "w") as f:
            f.write(f"KEY={key}\nKID={kid}\nCEK={cek}\n")
        logger.info(f"Saved DRM keys to {key_file}")
        return key_file

    def get_video_info(self, input_file: Path) -> Dict[str, any]:
        try:
            cmd_res = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "csv=p=0", str(input_file)
            ]
            res_output = subprocess.run(cmd_res, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            width, height = res_output.stdout.strip().split(',')

            cmd_br = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "format=bit_rate",
                "-of", "csv=p=0", str(input_file)
            ]
            br_output = subprocess.run(cmd_br, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            bitrate = int(br_output.stdout.strip()) if br_output.stdout.strip().isdigit() else 3000000

            return {
                "resolution": f"{width}x{height}",
                "bitrate": bitrate
            }
        except Exception as e:
            logger.error(f"Failed to extract video info for {input_file}: {e}")
            return {"resolution": "1920x1080", "bitrate": 3000000}

    def _get_video_codec(self, video_path: str) -> str:
        try:
            cmd = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,profile,level",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path)
            ]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            codec_info = result.stdout.strip().split('\n')
            codec_name = codec_info[0].lower()
            profile = codec_info[1].lower()
            level = codec_info[2].split('.')[0]
            
            if codec_name == "h264":
                profile_map = {
                    "baseline": "4200",
                    "main": "4d00",
                    "high": "6400"
                }
                return f"avc1.{profile_map.get(profile, '6400')}{level}"
            return "avc1.64001f"
        except Exception as e:
            logger.error(f"Error getting codec info: {e}")
            return "avc1.64001f"

    def _get_video_resolution(self, video_path: str) -> str:
        try:
            cmd = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "csv=p=0",
                str(video_path)
            ]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            width, height = result.stdout.strip().split(',')
            return f"{width}x{height}"
        except Exception as e:
            logger.error(f"Error getting resolution: {e}")
            return "1920x1080"

    def _calculate_bandwidth(self, resolution: str, bitrate: int) -> int:
        base_bandwidth = bitrate
        return int(base_bandwidth * 1.2)

    def has_audio_stream(self, input_path: str) -> bool:
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


    def has_audio_stream(self, video_path: str) -> bool:
        """Check if a video file has an audio stream using ffprobe."""
        cmd = [
            "ffprobe", "-v", "error", "-show_streams",
            "-select_streams", "a", "-print_format", "json",
            str(video_path)
        ]
        try:
            result = subprocess.run(
                cmd, check=True, capture_output=True, text=True
            )
            streams = json.loads(result.stdout).get("streams", [])
            return len(streams) > 0
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to probe audio streams for {video_path}: {e.stderr}")
            return False
        except json.JSONDecodeError:
            logger.error(f"Invalid ffprobe output for {video_path}")
            return False

    def fragment_files(self, input_dir: Union[str, Path], audio_files: Dict[str, str] = None) -> List[str]:
        """Fragment video and audio MP4 files."""
        input_dir = Path(input_dir)
        fragmented_dir = self.output_dir / "fragmented"
        fragmented_dir.mkdir(exist_ok=True)

        fragmented_files = []
        # Fragment video files
        for mp4_file in input_dir.glob("*.mp4"):
            output_file = fragmented_dir / f"frag_{mp4_file.name}"
            logger.info(f"Fragmenting video {mp4_file.name}")
            subprocess.run(["mp4fragment", str(mp4_file), str(output_file)], check=True)
            fragmented_files.append(str(output_file))

        # Fragment audio files
        if audio_files:
            for lang, audio_path in audio_files.items():
                audio_file = Path(audio_path)
                output_file = fragmented_dir / f"frag_audio_{lang}.mp4"
                logger.info(f"Fragmenting audio {audio_file.name} for {lang}")
                subprocess.run(["mp4fragment", str(audio_file), str(output_file)], check=True)
                fragmented_files.append(str(output_file))

        return fragmented_files

    def package_with_drm(self, fragmented_files: List[str], job: dict, audio_files: Dict[str, str], vtt_paths: List[Dict[str, str]]):
        """Package DASH and HLS with DRM encryption, including audio and subtitles."""
        logger.info("Packaging with DRM (DASH + HLS)...")

        kid = 'e507c3597bd4170605ca6989242068cd'
        drm_key = '7e3f0dae946381dbfe7a0d287c93e52c'
        cek = 'd44ac4ab26d374b6e904dd70772586eb'

        # kid = self.generate_hex_key()
        # drm_key = self.generate_hex_key()
        # cek = self.generate_hex_key()

        self.save_keys_to_file(drm_key, kid, cek)

        dash_dir = self.output_dir / "dash"
        # Ensure dash_dir exists and is empty
        if dash_dir.exists():
            shutil.rmtree(dash_dir)
        # dash_dir.mkdir(exist_ok=True)

        # Set UTF-8 environment for Windows
        if platform.system() == "Windows":
            try:
                subprocess.run(["chcp", "65001"], check=True, capture_output=True, text=True, shell=True)
                logger.info("Set console code page to UTF-8 (chcp 65001)")
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to set chcp 65001: {e.stderr}")
            os.environ["PYTHONUTF8"] = "1"
            logger.info("Set PYTHONUTF8=1 environment variable")

        # Base mp4dash command
        command = [
            'mp4dash',
            '--profiles=on-demand',
            '--output', str(dash_dir.as_posix()),
            '--mpd-name', 'manifest.mpd',
            '--marlin',
            '--encryption-cenc-scheme=cbcs',
            '--encryption-args="--global-option mpeg-cenc.piff-compatible:true"',
            f'--encryption-key={kid}:{drm_key}:{cek}',
            f'--widevine-header=provider:intertrust.ki#content_id:{kid}#protection_scheme:cbcs',
            '--hls',
            f'--fairplay-key-uri=skd://${kid}',
            '--mpd-name=manifest.mpd',
            '--playready-version=4.3',
            '--playready',
            '--playready-header=LA_URL:https://pr.service.expressplay.com/playready/RightsManager.asmx',
            '--hls-master-playlist-name=manifest.m3u8'
        ]

        # Add fragmented video and audio files
        for frag_file in fragmented_files:
            frag_file = str(Path(frag_file).as_posix()) 
            if "frag_audio" in Path(frag_file).name:
                lang = Path(frag_file).name.replace("frag_audio_", "").replace(".mp4", "")
                role = "main" if lang == list(audio_files.keys())[0] else "alternate"
                command.append(f"[+language={lang},+role={role}]{frag_file}")
            else:
                command.append(frag_file)

        # Add subtitle files
        for subtitle in vtt_paths:
            if not isinstance(subtitle, dict) or 'language' not in subtitle or 'file_path' not in subtitle:
                logger.error(f"Invalid subtitle format: {subtitle}")
                continue
            lang = subtitle["language"]
            vtt_file = str(Path(subtitle["file_path"]).as_posix()) 
            command.append(f"[+format=webvtt,+language={lang}]{vtt_file}")

        print(f"Running DASH+HLS DRM packaging: {' '.join(map(str, command))}")
        logger.debug(f"Running DASH+HLS DRM packaging: {' '.join(command)}")
        try:
            # subprocess.run(command, check=True, capture_output=True, text=True)
            subprocess.run(' '.join(command), shell=True, check=True, capture_output=True, text=True)
            logger.info(f"DRM packaging completed in {dash_dir}")
        except subprocess.CalledProcessError as e:
            logger.error(f"mp4dash failed with return code {e.returncode}: {e.stderr}") 
            # Attempt to clean up temporary files
            for temp_file in dash_dir.glob("tmp*"):
                for attempt in range(3):
                    try:
                        temp_file.unlink()
                        logger.debug(f"Deleted temporary file: {temp_file}")
                        break
                    except PermissionError:
                        logger.warning(f"Retry {attempt+1}/3: Cannot delete {temp_file}, waiting...")
                        time.sleep(1)
            raise RuntimeError(f"mp4dash failed: {e.stderr}")
        except Exception as e:
            logger.error(f"Unexpected error running mp4dash: {e}")
            raise

   
    def transcode_subtitles(self, audio_files: Dict[str, str], subtitle_files: List[Dict[str, str]], video_duration: float, hls_time: int = 6, hls_list_size: int = 0) -> Dict[str, str]:
        """Generate HLS playlists for subtitles with segmentation."""
        subtitle_manifests = {}
        if not subtitle_files:
            logger.info("No subtitles provided, skipping subtitle HLS processing.")
            return subtitle_manifests

        hls_dir = self.output_dir / "hls"
        hls_dir.mkdir(parents=True, exist_ok=True)

        for subtitle in subtitle_files:
            if not isinstance(subtitle, dict) or 'language' not in subtitle or 'file_path' not in subtitle:
                logger.error(f"Invalid subtitle format: {subtitle}")
                raise ValueError(f"Expected dictionary with 'language' and 'file_path', got: {subtitle}")

            subtitle_name = f"sub_{subtitle['language']}"
            subtitle_output_dir = hls_dir / subtitle_name
            subtitle_output_dir.mkdir(exist_ok=True)

            vtt_file = subtitle["file_path"]
            playlist_path = subtitle_output_dir / "playlist.m3u8"

            # Generate HLS playlist for WebVTT
            ffmpeg_cmd_hls = [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", f"anullsrc=channel_layout=stereo:sample_rate=48000:duration={video_duration}",
                "-i", vtt_file,

                "-map", "0:a", "-map", "1:s",
                "-c:a", "aac", "-b:a", "128k",

                "-c:s", "webvtt",
                "-copyts", "-start_at_zero",
                "-f", "hls",
                "-hls_time", str(hls_time),
                "-hls_list_size", str(hls_list_size),
                "-hls_playlist_type", "vod",
                # "-hls_segment_filename", str(subtitle_output_dir / "segment%d.vtt"),
                str(playlist_path)
            ]

            try:
                result = subprocess.run(ffmpeg_cmd_hls, check=True, capture_output=True, text=True)
                logger.info(f"Subtitle HLS processing completed for {subtitle_name}: {result.stdout}")

                # Post-process playlist to ensure correct durations
                with open(playlist_path, "r", encoding="utf-8") as f:
                    playlist_content = f.readlines()

                total_duration = video_duration
                num_segments = int(total_duration // hls_time) + (1 if total_duration % hls_time else 0)
                fixed_content = [
                    "#EXTM3U\n",
                    "#EXT-X-VERSION:6\n",
                    f"#EXT-X-TARGETDURATION:{hls_time}\n",
                    "#EXT-X-MEDIA-SEQUENCE:0\n",
                    "#EXT-X-PLAYLIST-TYPE:VOD\n"
                ]
                remaining_duration = total_duration
                for i in range(num_segments):
                    duration = min(remaining_duration, hls_time)
                    fixed_content.append(f"#EXTINF:{duration:.6f},\n")
                    fixed_content.append(f"playlist{i}.vtt\n")
                    remaining_duration -= duration
                fixed_content.append("#EXT-X-ENDLIST\n")

                with open(playlist_path, "w", encoding="utf-8") as f:
                    f.writelines(fixed_content)

                # Normalize path to use forward slashes
                relative_path = str(playlist_path.relative_to(hls_dir)).replace('\\', '/')
                subtitle_manifests[subtitle["language"]] = relative_path

            except subprocess.CalledProcessError as e:
                logger.error(f"Subtitle HLS processing failed for {subtitle_name}: {e.stderr}")
                raise

        return subtitle_manifests

    def package_without_drm(self, video_files: List[str], audio_files: Dict[str, str], subtitles: List[Dict[str, str]], video_duration: float):
        """Package HLS with audio, video, and subtitles, using original video audio if no external audio provided."""
        hls_dir = self.output_dir / "hls"
        shutil.rmtree(hls_dir, ignore_errors=True)
        hls_dir.mkdir(parents=True, exist_ok=True)

        hls_time = 6
        hls_list_size = 0

        # Process audio tracks
        audio_manifests = {}
        if audio_files:
            # External audio tracks provided
            for lang, audio_path in audio_files.items():
                lang_dir = hls_dir / "audio" / lang
                lang_dir.mkdir(parents=True, exist_ok=True)
                playlist_path = lang_dir / "playlist.m3u8"

                cmd = [
                    "ffmpeg", "-y", "-i", str(audio_path),
                    "-c:a", "copy" if audio_path.endswith(".mp4") else "aac",
                    "-b:a", "128k",
                    "-f", "hls",
                    "-hls_time", str(hls_time),
                    "-hls_list_size", str(hls_list_size),
                    "-hls_playlist_type", "vod",
                    "-hls_segment_type", "mpegts",
                    str(playlist_path)
                ]
                try:
                    subprocess.run(cmd, check=True, capture_output=True, text=True)
                    relative_path = str(playlist_path.relative_to(hls_dir)).replace('\\', '/')
                    audio_manifests[lang] = relative_path
                    logger.info(f"Generated audio HLS playlist for {lang} at {playlist_path}")
                except subprocess.CalledProcessError as e:
                    logger.error(f"Failed to generate audio HLS for {lang}: {e.stderr}")
                    raise
        else:
            # No external audio; try to extract audio from first video with audio
            for video in video_files:
                if self.has_audio_stream(video):
                    lang = "original"
                    lang_dir = hls_dir / "audio" / lang
                    lang_dir.mkdir(parents=True, exist_ok=True)
                    playlist_path = lang_dir / "playlist.m3u8"

                    cmd = [
                        "ffmpeg", "-y", "-i", str(video),
                        "-vn", "-c:a", "aac", "-b:a", "128k",  # Encode to AAC for HLS compatibility
                        "-f", "hls",
                        "-hls_time", str(hls_time),
                        "-hls_list_size", str(hls_list_size),
                        "-hls_playlist_type", "vod",
                        "-hls_segment_type", "mpegts",
                        str(playlist_path)
                    ]
                    try:
                        subprocess.run(cmd, check=True, capture_output=True, text=True)
                        relative_path = str(playlist_path.relative_to(hls_dir)).replace('\\', '/')
                        audio_manifests[lang] = relative_path
                        logger.info(f"Generated original audio HLS playlist from {video} at {playlist_path}")
                        break  # Use audio from first video with audio
                    except subprocess.CalledProcessError as e:
                        logger.error(f"Failed to generate original audio HLS from {video}: {e.stderr}")
                        raise
            if not audio_manifests:
                logger.warning("No audio tracks provided, and no audio found in input videos")

        # Process subtitles
        subtitle_manifests = self.transcode_subtitles(audio_files, subtitles, video_duration, hls_time, hls_list_size)

        # Process video variants
        video_manifests = []
        for idx, video in enumerate(video_files):
            variant_dir = hls_dir / f"video/variant_{idx}"
            variant_dir.mkdir(parents=True, exist_ok=True)
            playlist_path = variant_dir / "playlist.m3u8"

            video_info = self.get_video_info(Path(video))
            resolution = video_info["resolution"]
            bitrate = video_info["bitrate"]
            codec = self._get_video_codec(video)
            bandwidth = self._calculate_bandwidth(resolution, bitrate)

            cmd = [
                "ffmpeg", "-y", "-i", str(video),
                "-c:v", "copy", "-an",  # No audio in video output
                "-f", "hls",
                "-hls_time", str(hls_time),
                "-hls_list_size", str(hls_list_size),
                "-hls_playlist_type", "vod",
                "-hls_segment_type", "mpegts",
                str(playlist_path)
            ]
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                # Normalize path to use forward slashes
                relative_path = str(playlist_path.relative_to(hls_dir)).replace('\\', '/')
                video_manifests.append({
                    "path": relative_path,
                    "bandwidth": bandwidth,
                    "resolution": resolution,
                    "codec": codec
                })
                logger.info(f"Generated video HLS playlist for {resolution} at {playlist_path}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to generate video HLS for {resolution}: {e.stderr}")
                raise

        # Generate master playlist
        master_playlist_path = hls_dir / "master.m3u8"
        with open(master_playlist_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            f.write("#EXT-X-VERSION:6\n")
            f.write("#EXT-X-INDEPENDENT-SEGMENTS\n")

            # Audio entries
            for idx, (lang, uri) in enumerate(audio_manifests.items()):
                default = "YES" if idx == 0 else "NO"
                f.write(
                    f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",LANGUAGE="{lang}",NAME="{lang.capitalize()}",DEFAULT={default},AUTOSELECT=YES,URI="{uri}"\n'
                )

            # Subtitle entries
            if subtitle_manifests:
                for idx, (lang, uri) in enumerate(subtitle_manifests.items()):
                    default = "YES" if idx == 0 else "NO"
                    f.write(
                        f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="{lang.capitalize()}",LANGUAGE="{lang}",DEFAULT={default},AUTOSELECT=YES,URI="{uri}"\n'
                    )

            # Video streams
            for video in video_manifests:
                stream_inf = (
                    f'#EXT-X-STREAM-INF:BANDWIDTH={video["bandwidth"]},AVERAGE-BANDWIDTH={int(video["bandwidth"] * 0.8)},RESOLUTION={video["resolution"]},CODECS="{video["codec"]}"'
                )
                if audio_manifests:
                    stream_inf += ',AUDIO="audio"'
                if subtitle_manifests:
                    stream_inf += ',SUBTITLES="subs"'
                stream_inf += '\n'
                f.write(stream_inf)
                f.write(f'{video["path"]}\n')

        logger.info(f"Created HLS master playlist at {master_playlist_path}")

    def process(self, input_path: Union[str, Path], job: dict, vtt_paths: List[Dict[str, str]], audio_files: Dict[str, str], video_duration: float):
        input_path = Path(input_path)
        
        if job.is_paid:
            fragmented_files = self.fragment_files(input_path, audio_files)
            self.package_with_drm(fragmented_files, job, audio_files, vtt_paths)
        else:
            transcoded_files = [str(f) for f in input_path.glob("*.mp4")]
            if not transcoded_files:
                logger.error("No transcoded files found for HLS packaging.")
                raise RuntimeError("No transcoded files found.")
            self.package_without_drm(transcoded_files, audio_files, vtt_paths, video_duration)

