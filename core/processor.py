# worker/core/processor.py
from pathlib import Path
import logging
from config.settings import settings
from services.s3_service import download_from_s3, upload_to_s3
from core.database import get_db
from services.ffmpeg_service import transcode_video, transcode_audio
from services.drm_service import DRMService
from services.notify_controller import update_status, update_progress
from services.email_service import send_email_report
from services.video_utils import get_video_duration, convert_srt_to_vtt_batch
import shutil
from core.models import AudioTrack, SubtitleTrack

logger = logging.getLogger(__name__)

class DRMProcessor:
    def process(self, job):
        job_id = job.job_id

        try:
            update_status(job_id, "processing")
            input_s3_url = job.s3_source
            output_s3_url = job.s3_destination
            input_credential_id = job.s3_input_id
            output_credential_id = job.s3_output_id or job.s3_input_id

            output_dir = Path(settings.OUTPUT_DIR) / f"job_{job_id}"
            output_dir.mkdir(parents=True, exist_ok=True)

            local_input = output_dir / "input.mp4"
            subtitle_dir = output_dir / "subtitles"
            subtitle_dir.mkdir(parents=True, exist_ok=True)
            transcoding_dir = output_dir / "transcoded"
            transcoding_dir.mkdir(parents=True, exist_ok=True)

            db = next(get_db())

            # Step 1: Download input and associated files
            update_progress(job_id, 10)
            logger.info(f"Downloading video from {input_s3_url}")
            download_from_s3(str(input_s3_url), str(local_input), input_credential_id, db)
            if not local_input.exists() or local_input.stat().st_size == 0:
                raise FileNotFoundError(f"Downloaded input.mp4 not found or empty: {local_input}")

            video_duration = get_video_duration(str(local_input))

            audio_tracks = db.query(AudioTrack).filter(AudioTrack.job_id == job_id).all()
            audio_files = []
            for track in audio_tracks:
                local_audio_path = output_dir / f"{track.language}.wav"
                logger.info(f"Downloading audio from {track.file_path}")
                download_from_s3(track.file_path, str(local_audio_path), input_credential_id, db)
                if not local_audio_path.exists() or local_audio_path.stat().st_size == 0:
                    logger.warning(f"Audio track missing or empty: {local_audio_path}")
                    continue
                audio_files.append({"file_path": str(local_audio_path), "language": track.language})
            if not audio_files:
                logger.info("No external audio tracks found, relying on default audio in video.")

            subtitle_tracks = db.query(SubtitleTrack).filter(SubtitleTrack.job_id == job_id).all()
            subtitle_files = []
            for track in subtitle_tracks:
                local_subtitle_path = output_dir / f"{track.language}.srt"
                logger.info(f"Downloading subtitle from {track.file_path}")
                download_from_s3(track.file_path, str(local_subtitle_path), input_credential_id, db)
                if not local_subtitle_path.exists() or local_subtitle_path.stat().st_size == 0:
                    logger.warning(f"Subtitle track missing or empty: {local_subtitle_path}")
                    continue
                subtitle_files.append({"file_path": str(local_subtitle_path), "language": track.language})
            if not subtitle_files:
                logger.info("No subtitle tracks found for job.")

            update_progress(job_id, 30)

            # Step 2: Convert subtitles and transcode
            vtt_paths = convert_srt_to_vtt_batch(subtitle_files, str(subtitle_dir)) if subtitle_files else []
            logger.info(f"Converted subtitles to VTT: {vtt_paths}")

            try:
                logger.info("Starting video transcoding...")
                transcoded_files = transcode_video(str(local_input), str(transcoding_dir))
                if not transcoded_files:
                    raise RuntimeError("Transcoding video returned no output files.")
                
                audio_files_dict = {}  # Renamed from audio_outputs
                for track in audio_files:
                    lang = track["language"]
                    logger.info(f"Transcoding audio for {lang}")
                    audio_files_dict[lang] = transcode_audio(
                        track["file_path"],
                        str(transcoding_dir / "audio"),
                        lang, 
                        job.is_paid
                    )["128k"]  # Use 128k bitrate
            except Exception as ffmpeg_error:
                logger.error(f"Transcoding failed: {ffmpeg_error}")
                raise RuntimeError(f"FFmpeg transcoding failed: {ffmpeg_error}")

            for f in transcoded_files:
                path = Path(f)
                if not path.exists() or path.stat().st_size == 0:
                    raise FileNotFoundError(f"Invalid transcoded file: {f}")

            # Step 3: DRM/HLS Packaging
            update_progress(job_id, 60)
            try:
                drm_service = DRMService(str(output_dir))
                drm_service.process(
                    input_path=str(transcoding_dir),
                    job=job,
                    vtt_paths=vtt_paths,
                    audio_files=audio_files_dict,  # Fixed argument name
                    video_duration=video_duration
                )
            except Exception as drm_error:
                logger.error(f"DRM/HLS processing failed: {drm_error}")
                raise RuntimeError(f"DRM/HLS processing failed: {drm_error}")

            # Step 4: Upload to S3
            update_progress(job_id, 90)
            if job.upload_to_s3:
                if not output_s3_url or not output_s3_url.startswith("s3://"):
                    raise ValueError(f"Invalid or missing s3_destination URL: {output_s3_url}")

                if job.is_paid:
                    dash_folder = output_dir / "dash"
                    drm_keys_file = output_dir / "drm_keys.txt"
                    if not dash_folder.exists():
                        raise FileNotFoundError(f"DASH folder not found: {dash_folder}")
                    if not drm_keys_file.exists():
                        raise FileNotFoundError(f"DRM keys file not found: {drm_keys_file}")
                    upload_to_s3(str(dash_folder), output_s3_url, output_credential_id, db)
                    upload_to_s3(str(drm_keys_file), output_s3_url, output_credential_id, db)
                else:
                    hls_folder = output_dir / "hls"
                    if not hls_folder.exists():
                        raise FileNotFoundError(f"HLS folder not found: {hls_folder}")
                    logger.info(f"Uploading HLS output to {output_s3_url}")
                    upload_to_s3(str(hls_folder), output_s3_url, output_credential_id, db)

                # try:
                #     shutil.rmtree(output_dir)
                #     logger.info(f"Cleaned up local directory: {output_dir}")
                # except Exception as cleanup_err:
                #     logger.warning(f"Failed to delete output directory: {cleanup_err}")

            update_progress(job_id, 100)
            update_status(job_id, "completed")

        except Exception as e:
            logger.exception("Job failed")
            logger.error(f"Error processing job {job_id}: {e}")
            update_status(job_id, "failed")
            raise
        finally:
            try:
                db.close()
            except Exception as db_close_err:
                logger.error(f"Failed to close database session: {db_close_err}")

            # if job.send_email_report:
            #     try:
            #         send_email_report(job, "DRM Processing Report", "The DRM processing job has completed.")
            #     except Exception as email_err:
            #         logger.error(f"Failed to send email report: {email_err}")