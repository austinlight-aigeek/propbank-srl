import nltk
import os
from dotenv import load_dotenv
from nltk.corpus import propbank, treebank

load_dotenv()

# Ensure you have the corpora downloaded
nltk.download("propbank")
nltk.download("treebank")

nltk.data.path.append(os.getenv("NLTK_DATA"))


def parse_propbank_data():
    # Iterate over all PropBank instances
    instances = propbank.instances()

    # Initialize a placeholder to track processing per sentence
    sentence = None
    semantic_roles = {
        "lemma": [],
        "agent": [],
        "theme": [],
        "beneficiary": [],
        "goal": [],
        "related_entities": [],
    }

    for instance in instances:
        try:
            # Get the parse tree for the sentence
            # sentnum: position of the sentence in the file
            # fileid: specific file
            tree = treebank.parsed_sents(instance.fileid)[instance.sentnum]
            current_sentence = " ".join(tree.leaves())

            if sentence is not None and current_sentence != sentence:
                yield {
                    "sentence": sentence,
                    "lemma": semantic_roles["lemma"],
                    "agent": semantic_roles["agent"],
                    "theme": semantic_roles["theme"],
                    "beneficiary": semantic_roles["beneficiary"],
                    "goal": semantic_roles["goal"],
                    "related_entities": semantic_roles["related_entities"],
                }

                # Reset current semantic roles for the new sentence
                semantic_roles = {
                    "lemma": [],
                    "agent": [],
                    "theme": [],
                    "beneficiary": [],
                    "goal": [],
                    "related_entities": [],
                }

            # Set sentence to the current sentence in processing
            sentence = current_sentence

            lemma = instance.roleset.split(".")[0]
            # Append the lemma if not already present
            if lemma not in semantic_roles["lemma"]:
                semantic_roles["lemma"].append(lemma)

            # Extract the arguments (agents, themes, etc.)
            for argloc, argid in instance.arguments:
                arg_text = " ".join(argloc.select(tree).leaves())

                # Map ARGs to lists based on PropBank guidelines
                if argid == "ARG0" and arg_text not in semantic_roles["agent"]:
                    semantic_roles["agent"].append(arg_text)

                elif argid == "ARG1" and arg_text not in semantic_roles["theme"]:
                    semantic_roles["theme"].append(arg_text)

                elif argid == "ARG2" and arg_text not in semantic_roles["beneficiary"]:
                    semantic_roles["beneficiary"].append(arg_text)

                elif "ARGM-GOL" in argid and arg_text not in semantic_roles["goal"]:
                    semantic_roles["goal"].append(arg_text)

                # Collect related entities: Noun Phrases (NPs) and entities directly related to the lemma
                if (
                    "NP" in argloc.select(tree).label()
                    and arg_text not in semantic_roles["related_entities"]
                ):
                    semantic_roles["related_entities"].append(arg_text)

            # Additional entities can be extracted based on syntactic structure
            # Extract all noun phrases from the sentence to get other entities related to the lemma
            for subtree in tree.subtrees():
                if subtree.label() == "NP":  # Example: Extract all noun phrases
                    np_text = " ".join(subtree.leaves())
                    if np_text not in semantic_roles["related_entities"]:
                        pass
                        # semantic_roles["related_entities"].append(np_text)

                # Enhance goal detection: Extract prepositional phrases and their objects
                elif subtree.label() == "PP":  # Prepositional Phrase
                    pp_text = " ".join(subtree.leaves())
                    if pp_text not in semantic_roles["goals"]:
                        semantic_roles["goals"].append(pp_text)

        except FileNotFoundError:
            # Skip if the treebank file is not found
            # print(f"File not found: {instance.fileid}. Skipping this instance.")
            continue
        except Exception as e:
            # Handle other exceptions
            # print(f"Error processing {instance.fileid}: {e}")
            continue

        # Yield the last sentence's data after finishing the loop
    if current_sentence is not None:
        yield {
            "sentence": current_sentence,
            "lemma": semantic_roles["lemma"],
            "agent": semantic_roles["agent"],
            "theme": semantic_roles["theme"],
            "beneficiary": semantic_roles["beneficiary"],
            "goal": semantic_roles["goal"],
            "related_entities": semantic_roles["related_entities"],
        }
