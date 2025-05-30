# import boto3
# from botocore.exceptions import NoCredentialsError, ClientError
# import os

# # Replace with your credentials and config
# AWS_ACCESS_KEY_ID = "AKIAQQUBKPHLWXVNJNE6"
# AWS_SECRET_ACCESS_KEY = "jEZlevvHEhryCS0nqkGm9wniFvfDgrepvkd/AWy0"
# AWS_REGION = "ap-south-1"  # or the region your bucket is in
# S3_BUCKET_NAME = "modernstreaming"
# LOCAL_FILE_PATH = "C:/ffmpeg/bin/EP2.mp4"  # Windows-style path
# S3_OBJECT_NAME = "ikbir/EP2.mp4"  # This is the path in the bucket

# def upload_file_to_s3():
#     try:
#         # Initialize S3 client
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=AWS_ACCESS_KEY_ID,
#             aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#             region_name=AWS_REGION
#         )

#         # Upload file
#         print(f"🚀 Uploading {LOCAL_FILE_PATH} to s3://{S3_BUCKET_NAME}/{S3_OBJECT_NAME}")
#         s3_client.upload_file(LOCAL_FILE_PATH, S3_BUCKET_NAME, S3_OBJECT_NAME)
#         print("✅ Upload successful!")

#     except FileNotFoundError:
#         print(f"❌ File not found: {LOCAL_FILE_PATH}")
#     except NoCredentialsError:
#         print("❌ Credentials not available.")
#     except ClientError as e:
#         print(f"❌ AWS Client error: {e}")
#     except Exception as e:
#         print(f"❌ Unexpected error: {e}")

# if __name__ == "__main__":
#     upload_file_to_s3()




import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os

# S3 Configuration
AWS_ACCESS_KEY_ID = "AKIAQQUBKPHLWXVNJNE6"
AWS_SECRET_ACCESS_KEY = "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"
AWS_REGION = "ap-south-1"
S3_BUCKET_NAME = "modernstreaming"

# Path to local file or folder
LOCAL_PATH = r"E:/ikbir/New folder/Gurmukh_Trailer_2"
# Optional prefix on S3 (e.g., folder inside bucket)
S3_PREFIX = "ikbir/Gurmukh_Trailer_2"

def upload_to_s3(local_path, s3_prefix=""):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        if os.path.isfile(local_path):
            filename = os.path.basename(local_path)
            s3_key = f"{s3_prefix}/{filename}" if s3_prefix else filename
            print(f"🚀 Uploading file: {local_path} → s3://{S3_BUCKET_NAME}/{s3_key}")
            s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key)
            print("✅ File uploaded successfully!")

        elif os.path.isdir(local_path):
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.startswith('.') or file in ['Thumbs.db']:
                        continue  # Skip hidden/system files

                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, local_path)
                    s3_key = f"{s3_prefix}/{relative_path}".replace(os.sep, "/")

                    print(f"📦 Uploading: {full_path} → s3://{S3_BUCKET_NAME}/{s3_key}")
                    s3_client.upload_file(full_path, S3_BUCKET_NAME, s3_key)

            print("✅ Folder uploaded successfully!")

        else:
            print("❌ The provided path is neither a file nor a folder.")

    except FileNotFoundError:
        print(f"❌ File or folder not found: {local_path}")
    except NoCredentialsError:
        print("❌ AWS credentials not available.")
    except ClientError as e:
        print(f"❌ AWS Client error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    upload_to_s3(LOCAL_PATH, S3_PREFIX)
