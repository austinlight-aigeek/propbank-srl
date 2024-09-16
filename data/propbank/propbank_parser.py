# data/propbank/propbank_parser.py
import xml.etree.ElementTree as ET


def parse_propbank_data(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    sentences = []
    for sentence in root.findall("sentence"):
        sentence_text = sentence.find("text").text
        lemma = sentence.find("predicate").text
        roles = {
            "agent": [],
            "theme": [],
            "goal": [],
            "beneficiary": [],
            "entities_related_to_lemma": [],
        }

        # Parse roles (assuming XML structure here; adjust as needed)
        for role in sentence.findall("role"):
            role_type = role.get("type")
            role_text = role.text
            if role_type in roles:
                roles[role_type].append(role_text)

        sentences.append(
            {
                "sentence_text": sentence_text,
                "lemma": lemma,
                "agent": roles["agent"],
                "theme": roles["theme"],
                "goal": roles["goal"],
                "beneficiary": roles["beneficiary"],
                "entities_related_to_lemma": roles["entities_related_to_lemma"],
            }
        )

    return sentences
