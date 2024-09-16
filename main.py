import psycopg2
from psycopg2 import extras
from data.propbank.propbank_parser import (
    parse_propbank_data,
)  # Adjust the import path if necessary
from config import DB_CONFIG


def add_propbank_dataset(parsed_data_generator, batch_size=100):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    insert_query = """
        INSERT INTO SRL_BASE (sentence_text, agent, lemma, theme, goal, beneficiary, entities_related_to_lemma, notes, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare the data for batch insertion
    data_to_insert = []
    for record in parsed_data_generator:
        # Convert list fields to a format PostgreSQL understands (text array)
        agent = record["agent"] if record["agent"] else []
        lemma = record["lemma"] if record["lemma"] else []
        theme = record["theme"] if record["theme"] else []
        goal = record["goal"] if record["goal"] else []
        beneficiary = record["beneficiary"] if record["beneficiary"] else []
        entities_related_to_lemma = (
            record["entities_related_to_lemma"]
            if record["entities_related_to_lemma"]
            else None
        )

        # Add the record to the list
        data_to_insert.append(
            (
                record["sentence_text"],
                agent,
                lemma,
                theme,
                goal,
                beneficiary,
                entities_related_to_lemma,
                "",
                "propbank",
            )
        )

        if len(data_to_insert) == batch_size:
            try:
                extras.execute_batch(
                    cur, insert_query, data_to_insert, page_size=batch_size
                )
                conn.commit()
                print(f"{len(data_to_insert)} rows inserted successfully.")

            except Exception as e:
                conn.rollback()
                print(f"An error occured: {e}")
            finally:
                data_to_insert = []

    # Insert any remaining records in the final batch
    if data_to_insert:
        try:
            extras.execute_batch(
                cur, insert_query, data_to_insert, page_size=batch_size
            )
            conn.commit()
            print(f"{len(data_to_insert)} rows inserted successfully.")
        except Exception as e:
            conn.rollback()
            print(f"An error occurred: {e}")

    # Close the database connection
    cur.close()
    conn.close()


def main():
    parsed_data_generator = parse_propbank_data()
    add_propbank_dataset(parsed_data_generator, batch_size=500)


if __name__ == "__main__":
    main()
