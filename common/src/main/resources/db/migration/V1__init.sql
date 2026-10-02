CREATE TABLE session (
    id         UUID PRIMARY KEY,
    last_seq   BIGINT      NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE translation (
    session_id    UUID        NOT NULL REFERENCES session (id),
    lang          VARCHAR(8)  NOT NULL,
    seq           BIGINT      NOT NULL,
    text          TEXT        NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL,
    translated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, lang, seq)
);
