import psycopg2
from config import DB_CONFIG, TABLE_PROPBANK


def create_propbank_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    with open("db/propbank_table_schema.sql", "r") as schema_file:
        schema_sql = schema_file.read()
        cur.execute(schema_sql)

    conn.commit()
    cur.close()
    conn.close()

    print(f"{TABLE_PROPBANK} table created successfully")


if __name__ == "__main__":
    create_propbank_table()
