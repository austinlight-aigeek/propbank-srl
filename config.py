# Mapping configuration between custom roles and PropBank arguments
ROLE_MAPPING = {
    "agent": "ARG0",
    "lemma": "V",  # 'V' indicates the verb or predicate in PropBank; 'lemma' refers to the verb
    "theme": "ARG1",
    "goal": "ARG2",
    "beneficiary": "ARG3",
    "entities_related_to_lemma": "ARGM",  # 'ARGM' can be any modifier like ARGM-LOC, ARGM-TMP, etc.
}
