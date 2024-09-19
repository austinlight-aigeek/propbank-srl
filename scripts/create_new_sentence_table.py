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
        return None

    doc = nlp(sentence)
    semantic_entites = {
        "agent": [],
        "lemma": [],
        "theme": [],
        "goal": [],
        "beneficiary": [],
        "entities_related_to_lemma": [],
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
            if agent not in semantic_entites["agent"]:
                semantic_entites["agent"].append(agent.lower())

            # Add lemma if not present
            if lemma not in semantic_entites["lemma"]:
                semantic_entites["lemma"].append(lemma.lower())

            # Find prepositional phrases and extract relevant information
            for prep in prep_phrases:
                prep_object = [
                    child.text for child in prep.children if child.dep_ == "pobj"
                ]
                if prep_object:
                    # Add to goal if not present
                    for po in prep_object:
                        if po not in semantic_entites["goal"]:
                            semantic_entites["goal"].append(po.lower())

            # Add to theme if not present
            if direct_objects:
                for do in direct_objects:
                    if do not in semantic_entites["theme"]:
                        semantic_entites["theme"].append(do.lower())

            # Add to beneficiary if not present
            if indirect_objects:
                for io in direct_objects:
                    if io not in semantic_entites["beneficiary"]:
                        semantic_entites["beneficiary"].append(io.lower())

            if other_entities:
                new_other_entities = []
                for other_entity in other_entities:
                    if other_entity.text.lower() not in stopwords:
                        if other_entity.pos_ == "VERB":
                            new_other_entities.append(other_entity.lemma_)
                        else:
                            new_other_entities.append(other_entity.text)

                # Add the ones if not present
                if new_other_entities:
                    for oe in new_other_entities:
                        if oe not in semantic_entites["entities_related_to_lemma"]:
                            semantic_entites["entities_related_to_lemma"].append(
                                oe.lower()
                            )

    return [semantic_entites]


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
