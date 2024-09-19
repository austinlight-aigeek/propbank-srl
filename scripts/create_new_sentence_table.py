import spacy
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
from config import DB_CONFIG

nlp = spacy.load("en_core_web_sm")
stopwords = nlp.Defaults.stop_words


def duplicate_table():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        """
        DROP TABLE IF EXISTS sentence_new;
        CREATE TABLE sentence_new AS TABLE sentence;
    """
    )

    conn.commit()
    cursor.close()
    conn.close()

    print("duplicated sentence table successfully")


def extract_semantic_entities(sentence: str) -> list[dict[str, str]]:
    if not sentence:
        return []

    doc = nlp(sentence)
    semantic_entities = {
        "agent": set(),
        "lemma": set(),
        "theme": set(),
        "goal": set(),
        "beneficiary": set(),
        "entities_related_to_lemma": set(),
    }
    for token in doc:
        if token.dep_ == "nsubj" and token.head.pos_ == "VERB":
            agent = token.text
            lemma = token.head.lemma_

            # Find direct objects of the verb
            direct_objects = [
                child.text for child in token.head.children if child.dep_ == "dobj"
            ]
            # Find indirect objects of the verb
            indirect_objects = [
                child.text for child in token.head.children if child.dep_ == "dative"
            ]

            # Find prepositional phrases
            prep_phrases = [
                child for child in token.head.children if child.dep_ == "prep"
            ]

            # Find other relevant entities related to the verb excluding used for others
            other_entities = [
                child
                for child in token.head.children
                if child.dep_ != "nsubj"
                and child.dep_ != "dobj"
                and child.dep_ != "dative"
                and child.dep_ != "prep"
                and not child.is_punct
            ]

            # Add agent if not present
            semantic_entities["agent"].add(agent.lower())

            # Add lemma if not present
            semantic_entities["lemma"].add(lemma.lower())

            # Find prepositional phrases and extract relevant information
            for prep in prep_phrases:
                prep_object = [
                    child.text for child in prep.children if child.dep_ == "pobj"
                ]
                # Add to goal if not present
                for po in prep_object:
                    semantic_entities["goal"].add(po.lower())

            # Add to theme if not present
            for do in direct_objects:
                semantic_entities["theme"].add(do.lower())

            # Add to beneficiary if not present
            for io in indirect_objects:
                semantic_entities["beneficiary"].add(io.lower())

            new_other_entities = []
            for other_entity in other_entities:
                if other_entity.text.lower() not in stopwords:
                    new_other_entities.append(
                        other_entity.lemma_
                        if other_entity.pos_ == "VERB"
                        else other_entity.text
                    )

            # Add the ones if not present
            for oe in new_other_entities:
                semantic_entities["entities_related_to_lemma"].add(oe.lower())

    return [[{key: list(value) for key, value in semantic_entities.items()}]]


def update_sentence_table():
    conn = psycopg2.connect(**DB_CONFIG)

    fetch_cursor = conn.cursor(name="sentence_cursor")
    fetch_cursor.execute(
        "SELECT source_id, sentence_pos, sentence_text FROM sentence_new;"
    )

    all_rows = fetch_cursor.fetchall()
    fetch_cursor.close()

    batch_size = 10000

    for i in range(0, len(all_rows), batch_size):
        batch_rows = all_rows[i : i + batch_size]

        with ThreadPoolExecutor(max_workers=8) as executor:
            semantic_entities_list = list(
                executor.map(
                    lambda row: (row[0], row[1], extract_semantic_entities(row[2])),
                    batch_rows,
                )
            )

        # Prepare data for bulk update
        update_data = []
        for source_id, sentence_pos, semantic_entity in semantic_entities_list:
            if semantic_entity and len(semantic_entity) > 0:
                update_data.append(
                    (
                        semantic_entity[0].get("agent", []),
                        semantic_entity[0].get("lemma", []),
                        semantic_entity[0].get("goal", []),
                        semantic_entity[0].get("theme", []),
                        semantic_entity[0].get("beneficiary", []),
                        semantic_entity[0].get("entities_related_to_lemma", []),
                        source_id,
                        sentence_pos,
                    )
                )

        # Use execute_values for bulk update
        with conn.cursor() as update_cursor:
            execute_values(
                update_cursor,
                """
                    UPDATE sentence_new
                    SET agent = data.agent::text[], lemma = data.lemma::text[], goal = data.goal::text[], theme = data.theme::text[], 
                    beneficiary = data.beneficiary::text[], entities_related_to_lemma = data.entities_related_to_lemma::text[]
                    FROM (VALUES %s) AS data (agent, lemma, goal, theme, beneficiary, entities_related_to_lemma, source_id, sentence_pos)
                    WHERE sentence_new.source_id = data.source_id AND sentence_new.sentence_pos = data.sentence_pos;
                """,
                update_data,
            )

        conn.commit()
        print(f"{batch_size} rows were updated")

    conn.close()
    print("-" * 50)
    print("sentence_new table updated successfully")


def main():
    duplicate_table()
    update_sentence_table()


if __name__ == "__main__":
    main()
