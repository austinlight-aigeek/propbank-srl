import os
from dotenv import load_dotenv

load_dotenv(override=True)

TABLE_PROPBANK = "annotated_propbank"

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": 5432,
}
