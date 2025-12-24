import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def main() -> int:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        name = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        sslmode = os.getenv("DB_SSLMODE")
        if not all([host, port, name, user, password]):
            print("DATABASE_URL is not set and DB_* params are incomplete.")
            return 1
        database_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
        if sslmode:
            database_url = f"{database_url}?sslmode={sslmode}"

    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    print("Database connection OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
