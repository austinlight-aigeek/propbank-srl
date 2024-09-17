import psycopg2
from psycopg2.extras import execute_values
from data.propbank.propbank_parser import (
    parse_propbank_data,
)  # Adjust the import path if necessary
from config import DB_CONFIG


def insert_batch_to_db(conn, cursor, data_batch):

    insert_query = """
        INSERT INTO SRL_BASE (sentence_text, agent, lemma, theme, goal, beneficiary, entities_related_to_lemma, notes, source)
        VALUES %s
    """

    # Prepare the data for batch insertion

    records_to_insert = [
        (
            data["sentence"],
            data["agent"],
            data["lemma"],
            data["theme"],
            data["goal"],
            data["beneficiary"],
            data["related_entities"],
            "",
            "propbank",  # source
        )
        for data in data_batch
    ]

    try:
        # Use execute_values for batch insert
        execute_values(cursor, insert_query, records_to_insert)
        conn.commit()
        print(f"Inserted {len(data_batch)} records successfully.")
    except Exception as e:
        print(f"Error inserting batch to database: {e}")
        conn.rollback()


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    batch = []
    batch_size = 500
    count = 0

    for data in parse_propbank_data():
        batch.append(data)

        if len(batch) >= batch_size:
            insert_batch_to_db(conn, cur, batch)
            count += len(batch)
            batch = []

    if batch:
        insert_batch_to_db(conn, cur, batch)
        count += len(batch)

    print("-" * 50)
    print(f"Total: {count} sentences were inserted.")

    # Close the database connection
    cur.close()
    conn.close()

    print(f"{count} sentences were processed.")


if __name__ == "__main__":
    main()
