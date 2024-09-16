import nltk
import os
from dotenv import load_dotenv
from nltk.corpus import propbank, treebank
from data import DEFAULT_ROLES_STRUCTURE  # Import shared roles structure

load_dotenv()

# Ensure you have the corpora downloaded
nltk.download("propbank")
nltk.download("treebank")

nltk.data.path.append(os.getenv("NLTK_DATA"))


def extract_roles(tree, prop):
    # Use a copy of the shared structure to avoid modifying the original
    roles = DEFAULT_ROLES_STRUCTURE.copy()

    # Extract each argument and map it to the corresponding role
    for arg in prop.arguments:
        role_text = ""

        # Attempt to extract role text using different attributes in prop
        if (
            hasattr(arg, "location")
            and isinstance(arg.location, list)
            and len(arg.location) > 0
        ):
            # Handle the case where `location` is a list of tuples (start, end)
            start, end = arg.location[0]
            role_text = " ".join(tree.leaves()[start:end])
        elif hasattr(arg, "wordnum") and arg.wordnum is not None:
            # Handle the case where `wordnum` is a single index
            role_text = tree.leaves()[arg.wordnum]
        elif (
            hasattr(arg, "indices")
            and isinstance(arg.indices, list)
            and len(arg.indices) > 0
        ):
            # Handle the case where `indices` is a list of indices
            role_text = " ".join(tree.leaves()[i] for i in arg.indices)
        else:
            # If no valid location information is found, skip this argument
            continue

        # Core Arguments (ARG0-ARG4)
        if arg.type == "ARG0":
            # Typically the "Agent" or "Experiencer"
            roles["agent"].append(role_text)
        elif arg.type == "ARG1":
            # Typically the "Theme" or "Patient"
            roles["theme"].append(role_text)
        elif arg.type == "ARG2":
            # Context-dependent, usually "Beneficiary," "Instrument," "End State"
            roles["beneficiary"].append(role_text)
        elif arg.type == "ARG3":
            # "Starting point," "Attribute," or "Beneficiary"
            roles["goal"].append(role_text)
        elif arg.type == "ARG4":
            # "Ending point" or "Result"
            roles["goal"].append(role_text)

        # Modifier Arguments (ARGM)
        elif arg.type.startswith("ARGM-"):
            # Example ARGM types: ARGM-TMP, ARGM-LOC, ARGM-MNR, ARGM-GOL, etc.
            if arg.type == "ARGM-GOL":
                # Goal modifier (final destination)
                roles["goal"].append(role_text)
            else:
                # Collect other entities related to the lemma for informational purposes
                roles["entities_related_to_lemma"].append(role_text)

        # Handle other unspecified arguments
        else:
            # Collect other entities related to the lemma for informational purposes
            roles["entities_related_to_lemma"].append(role_text)

    return roles


def parse_propbank_data():
    # Iterate over all PropBank instances
    for instance in propbank.instances():
        try:
            # Attempt to get the parse tree for the sentence
            tree = treebank.parsed_sents(instance.fileid)[instance.sentnum]
            sentence_text = " ".join(tree.leaves())

            # Extract the verb lemma using the `roleset` attribute
            # The `roleset` is typically in the format 'verb.some_id', so we split by '.' and take the verb part
            verb_lemma = instance.roleset.split(".")[0]

            # Extract semantic roles
            roles = extract_roles(tree, instance)

            # Create a dictionary containing the necessary fields
            annotation_data = {
                "sentence_text": sentence_text,
                "agent": roles["agent"],  # List of agents
                "lemma": [verb_lemma],  # Verb lemma extracted from the roleset
                "theme": roles["theme"],  # List of themes
                "goal": roles["goal"],  # List of goals
                "beneficiary": roles["beneficiary"],  # List of beneficiaries
                "entities_related_to_lemma": roles[
                    "entities_related_to_lemma"
                ],  # List of other entities
            }

            # Yield the structured data instead of appending to a list
            yield annotation_data

        except FileNotFoundError:
            # Skip if the treebank file is not found
            # print(f"File not found: {instance.fileid}. Skipping this instance.")
            continue
        except Exception as e:
            # Handle other exceptions
            print(f"Error processing {instance.fileid}: {e}")
            continue
