# worker/testMYSQLConnection.py
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus

# Replace these with your actual DB credentials
DB_USER = "drm_user"
DB_PASSWORD = quote_plus('unisys@123')  # URL-encode the password
DB_HOST = "13.234.235.198"
DB_PORT = "3306"
DB_NAME = "drm_system"

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def test_connection():
    print("🔌 Testing MySQL connection...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("✅ MySQL connection successful:", result.scalar())
    except OperationalError as e:
        print("❌ Failed to connect to MySQL:")
        print(e)

if __name__ == "__main__":
    test_connection()
