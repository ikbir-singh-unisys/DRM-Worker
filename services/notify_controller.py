# worker/services/notify_controller.py
import requests
import logging
from config.settings import settings

from typing import Optional

logger = logging.getLogger(__name__)

def update_status(job_id, status):
    try:
        logger.info(f"📡 Reporting status '{status}' for job {job_id}")
        requests.post(f"{settings.API_BASE_URL}/queue/{job_id}/status", json={"status": status}, timeout=10)
    except Exception as e:
        logger.warning(f"⚠️ Failed to send status: {e}")

def update_progress(job_id: str, percent: int, duration: Optional[float] = None):
    payload = {"progress": percent}
    if duration is not None:
        payload["duration"] = duration

    try:
        logger.info(f"📶 Reporting progress {percent}% for job {job_id}")
        requests.post(f"{settings.API_BASE_URL}/queue/{job_id}/progress", json=payload, timeout=10)
    except Exception as e:
        logger.warning(f"⚠️ Failed to send progress: {e}")
