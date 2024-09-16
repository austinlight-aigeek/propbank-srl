CREATE TABLE IF NOT EXISTS SRL_BASE (
    annotation_id PRIMARY KEY,
    sentence_text TEXT NOT NULL,
    agent TEXT[] DEFAULT '{}',
    lemma TEXT[] DEFAULT '{}',
    theme TEXT[] DEFAULT '{}',
    goal TEXT[] DEFAULT '{}',
    beneficiary TEXT[] DEFAULT '{}',
    entities_related_to_lemma TEXT[] DEFAULT '{}',
    notes TEXT DEFAULT '',
    source TEXT DEFAULT ''
);