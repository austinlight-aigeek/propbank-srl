import psycopg2
from utils.config import DB_CONFIG


def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    with open("db/srl_schema.sql", "r") as schema_file:
        schema_sql = schema_file.read()
        cur.execute(schema_sql)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    create_tables()
