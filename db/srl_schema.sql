CREATE TABLE IF NOT EXISTS SemanticRoleAnnotations (
    annotation_id SERIAL PRIMARY KEY,
    sentence_text TEXT NOT NULL,
    lemma TEXT,
    agent TEXT[],
    theme TEXT[],
    goal TEXT[],
    beneficiary TEXT[],
    entities_related_to_lemma TEXT[],
    notes TEXT
);