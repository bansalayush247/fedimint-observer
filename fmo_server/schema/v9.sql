-- Add missing index on nostr_federations(federation_id) that was omitted in v7
BEGIN;

CREATE INDEX IF NOT EXISTS nostr_federations_federation_id ON nostr_federations (federation_id);

INSERT INTO
    schema_version (version)
VALUES
    (9);

COMMIT;
