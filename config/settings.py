import os
from typing import ClassVar
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

class Settings:
    # DB_USER: str = os.getenv("DB_USER", "drm_user")
    # DB_PASSWORD: str = quote_plus(os.getenv("DB_PASSWORD", "unisys@123"))
    # DB_HOST: str = os.getenv("DB_HOST", "13.234.235.198")
    # DB_PORT: int = int(os.getenv("DB_PORT", 3306))
    # DB_NAME: str = os.getenv("DB_NAME", "drm_system")

    # DATABASE_URL: ClassVar[str] = f"mysql+pymysql://{os.getenv('DB_USER', 'drm_user')}:{quote_plus(os.getenv('DB_PASSWORD', ''))}@{os.getenv('DB_HOST', '13.234.235.198')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME', 'drm_system')}"

    DB_USER: str = os.getenv("DB_USER", "root")
    DB_PASSWORD: str = quote_plus(os.getenv("DB_PASSWORD", ""))
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", 3306))
    DB_NAME: str = os.getenv("DB_NAME", "drm_system")

    DATABASE_URL: ClassVar[str] = f"mysql+pymysql://{os.getenv('DB_USER', 'root')}:{quote_plus(os.getenv('DB_PASSWORD', ''))}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME', 'drm_system')}"


    API_BASE_URL = os.getenv("CONTROLLER_API_URL")
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
    OUTPUT_DIR: Path = Path("output")

settings = Settings()
