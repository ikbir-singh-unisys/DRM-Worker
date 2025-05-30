# # worker/services/s3_service.py
# import boto3
# import os
# import logging
# from urllib.parse import urlparse
# from pathlib import Path
# from sqlalchemy.orm import Session
# from core.database import get_db
# from core.models import S3Credential

# logger = logging.getLogger(__name__)

# def get_s3_client(aws_access_key_id, aws_secret_access_key, region_name="us-east-1"):
#     return boto3.client(
#         "s3",
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key,
#         region_name=region_name
#     )

# def get_s3_credentials_from_db(s3_credential_id: int, db: Session):
#     credentials = db.query(S3Credential).filter(S3Credential.id == s3_credential_id).first()
#     if not credentials:
#         raise Exception(f"No S3 credentials found for ID {s3_credential_id}")
#     return {
#         "access_key": credentials.access_key,
#         "secret_key": credentials.secret_key,
#         "region": credentials.region or "us-east-1"
#     }

# def download_from_s3(s3_url, destination_path, s3_credential_id: int, db: Session):
#     parsed = urlparse(s3_url)
#     bucket = parsed.netloc
#     key = parsed.path.lstrip("/")

#     credentials = get_s3_credentials_from_db(s3_credential_id, db)
#     s3 = get_s3_client(
#         aws_access_key_id=credentials["access_key"],
#         aws_secret_access_key=credentials["secret_key"],
#         region_name=credentials["region"]
#     )

#     logger.info(f"⬇️ Downloading from s3://{bucket}/{key}")
#     s3.download_file(bucket, key, destination_path)

# def upload_to_s3(path: str, s3_url: str, s3_credential_id: int, db: Session):
#     """
#     Upload a file or directory to S3.
    
#     Args:
#         path: Local file or directory path.
#         s3_url: S3 destination URL (e.g., s3://bucket/prefix/).
#         s3_credential_id: ID for S3 credentials.
#         db: Database session.
        
#     Raises:
#         ValueError: If S3 URL is invalid.
#         FileNotFoundError: If path doesn't exist.
#         Exception: If upload fails.
#     """
#     parsed = urlparse(s3_url)
#     bucket = parsed.netloc
#     if not bucket:
#         raise ValueError(f"Invalid S3 URL: {s3_url}")
#     base_key = parsed.path.lstrip("/").rstrip("/")

#     credentials = get_s3_credentials_from_db(s3_credential_id, db)
#     s3 = get_s3_client(
#         aws_access_key_id=credentials["access_key"],
#         aws_secret_access_key=credentials["secret_key"],
#         region_name=credentials["region"]
#     )

#     path = Path(path)
#     if not path.exists():
#         raise FileNotFoundError(f"Path does not exist: {path}")

#     try:
#         if path.is_file():
#             # Upload single file
#             s3_key = f"{base_key}/{path.name}" if base_key else path.name
#             logger.info(f"⬆️ Uploading file {path} to s3://{bucket}/{s3_key}")
#             s3.upload_file(str(path), bucket, s3_key)
#         elif path.is_dir():
#             # Upload directory recursively
#             for root, _, files in os.walk(path):
#                 for file in files:
#                     local_path = Path(root) / file
#                     relative_path = local_path.relative_to(path.parent)
#                     s3_key = f"{base_key}/{relative_path}".replace("\\", "/")
#                     logger.info(f"⬆️ Uploading file {local_path} to s3://{bucket}/{s3_key}")
#                     s3.upload_file(str(local_path), bucket, s3_key)
#         else:
#             raise ValueError(f"Path is neither a file nor directory: {path}")
#     except Exception as e:
#         logger.error(f"Failed to upload {path} to s3://{bucket}/{s3_key}: {e}")
#         raise


# worker/services/s3_service.py
import boto3
import os
import logging
from urllib.parse import urlparse
from pathlib import Path
from sqlalchemy.orm import Session
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.config import Config
from core.database import get_db
from core.models import S3Credential

logger = logging.getLogger(__name__)

def get_s3_client(aws_access_key_id, aws_secret_access_key, region_name="ap-south-1"):
    """Create an S3 client with retry configuration."""
    config = Config(
        retries={
            'max_attempts': 5,
            'mode': 'standard'
        },
        connect_timeout=10,
        read_timeout=30
    )
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        config=config
    )

def get_s3_credentials_from_db(s3_credential_id: int, db: Session):
    """Retrieve S3 credentials from database."""
    credentials = db.query(S3Credential).filter(S3Credential.id == s3_credential_id).first()
    if not credentials:
        raise Exception(f"No S3 credentials found for ID {s3_credential_id}")
    return {
        "access_key": credentials.access_key,
        "secret_key": credentials.secret_key,
        "region": credentials.region or "us-east-1"
    }

def download_from_s3(s3_url: str, destination_path: str, s3_credential_id: int, db: Session):
    """Download a file from S3 with retry logic."""
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    credentials = get_s3_credentials_from_db(s3_credential_id, db)
    s3 = get_s3_client(
        aws_access_key_id=credentials["access_key"],
        aws_secret_access_key=credentials["secret_key"],
        region_name=credentials["region"]
    )

    logger.info(f"⬇️ Downloading from s3://{bucket}/{key} to {destination_path}")
    try:
        s3.download_file(bucket, key, destination_path)
    except EndpointConnectionError as e:
        logger.error(f"Failed to connect to S3 endpoint: {e}")
        raise
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"S3 ClientError: {error_code} - {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error downloading from S3: {e}")
        raise

def upload_to_s3(path: str, s3_url: str, s3_credential_id: int, db: Session):
    """
    Upload a file or directory to S3.
    
    Args:
        path: Local file or directory path.
        s3_url: S3 destination URL (e.g., s3://bucket/prefix/).
        s3_credential_id: ID for S3 credentials.
        db: Database session.
        
    Raises:
        ValueError: If S3 URL is invalid.
        FileNotFoundError: If path doesn't exist.
        Exception: If upload fails.
    """
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    if not bucket:
        raise ValueError(f"Invalid S3 URL: {s3_url}")
    base_key = parsed.path.lstrip("/").rstrip("/")

    credentials = get_s3_credentials_from_db(s3_credential_id, db)
    s3 = get_s3_client(
        aws_access_key_id=credentials["access_key"],
        aws_secret_access_key=credentials["secret_key"],
        region_name=credentials["region"]
    )

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    try:
        if path.is_file():
            # Upload single file
            s3_key = f"{base_key}/{path.name}" if base_key else path.name
            logger.info(f"⬆️ Uploading file {path} to s3://{bucket}/{s3_key}")
            s3.upload_file(str(path), bucket, s3_key)
        elif path.is_dir():
            # Upload directory recursively
            for root, _, files in os.walk(path):
                for file in files:
                    local_path = Path(root) / file
                    relative_path = local_path.relative_to(path.parent)
                    s3_key = f"{base_key}/{relative_path}".replace("\\", "/")
                    logger.info(f"⬆️ Uploading file {local_path} to s3://{bucket}/{s3_key}")
                    s3.upload_file(str(local_path), bucket, s3_key)
        else:
            raise ValueError(f"Path is neither a file nor directory: {path}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"S3 ClientError uploading to {s3_url}: {error_code} - {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to upload {path} to s3://{bucket}/{s3_key}: {e}")
        raise
