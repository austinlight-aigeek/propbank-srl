import nltk

from nltk.corpus import propbank
from nltk.corpus import treebank

import re

nltk.download("propbank")
nltk.download("treebank")


def process_instance(instance):
    # Original sentence
    sent_tree = treebank.parsed_sents(instance.fileid)[instance.sentnum]
    original_text = " ".join(sent_tree.leaves())

    # Lemma
    lemma = instance.predicate.lemma

    # Extract arguments (ARG0, ARG1, etc.)
    args = {
        arg.argname: " ".join(
            instance.words[arg.loc.index()] for arg.loc in instance.arglocs[arg]
        )
        for arg in instance.arguments
    }

    
