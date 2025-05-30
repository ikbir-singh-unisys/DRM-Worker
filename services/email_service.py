# worker/services/email_service.py
import os
import smtplib
from email.mime.text import MIMEText
import logging
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Email configuration
SMTP_SERVER = os.getenv('SMTP_SERVER')  # e.g., smtp.gmail.com
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))  # default to 587 if not set
USER_EMAIL = os.getenv('USER_EMAIL')  # The "From" email
SENDER_EMAIL = os.getenv('SENDER_EMAIL')  # The "From" email
USER_PASSWORD = os.getenv('USER_PASSWORD')  # Password or App password
RECEIVER_EMAILS = os.getenv('RECEIVER_EMAILS', '').split(',')  # Comma-separated list

def send_email_report(job_id: str, success: bool, error_message: str = ""):
    subject = f"Job {job_id} {'Success' if success else 'Failure'}"
    body = f"Job ID: {job_id}\nStatus: {'Success' if success else 'Failure'}\n"
    
    if not success and error_message:
        body += f"\nError Message:\n{error_message}"

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECEIVER_EMAILS)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(USER_EMAIL, USER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAILS, msg.as_string())
        logger.info(f"📧 Email report sent for job {job_id}")
    except Exception as e:
        logger.error(f"❌ Failed to send email report for job {job_id}: {e}")
