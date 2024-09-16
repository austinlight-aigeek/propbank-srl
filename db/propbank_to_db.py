# db/propbank_to_db.py
import psycopg2
from utils.config import DB_CONFIG
from data.propbank.propbank_parser import parse_propbank_data


def insert_propbank_data(file_path):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    sentences = parse_propbank_data(file_path)

    for sentence in sentences:
        cur.execute(
            """
            INSERT INTO SemanticRoleAnnotations (sentence_text, lemma, agent, theme, goal, beneficiary, entities_related_to_lemma)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
            (
                sentence["sentence_text"],
                sentence["lemma"],
                sentence["agent"],
                sentence["theme"],
                sentence["goal"],
                sentence["beneficiary"],
                sentence["entities_related_to_lemma"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    file_path = "data/propbank/propbank_data.xml"  # Adjust this to the location of your PropBank data
    insert_propbank_data(file_path)
