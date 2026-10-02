-- One row per accepted utterance. The client-supplied utterance_id makes ingest idempotent:
-- a retry of an utterance that was already committed gets its original seq back.
CREATE TABLE utterance (
    session_id   UUID        NOT NULL REFERENCES session (id),
    utterance_id UUID        NOT NULL,
    seq          BIGINT      NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, utterance_id),
    UNIQUE (session_id, seq)
);
