-- Bitcoin txids use their raw hash bytes everywhere. Older withdrawal tables
-- stored display-order bytes, unlike deposits. Reverse only the legacy tables.
ALTER TABLE wallet_withdrawal_signatures ALTER CONSTRAINT wallet_withdrawal_signatures_on_chain_txid_fkey DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE wallet_withdrawal_transaction_inputs ALTER CONSTRAINT wallet_withdrawal_transaction_inputs_on_chain_txid_fkey DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE wallet_withdrawal_transaction_outputs ALTER CONSTRAINT wallet_withdrawal_transaction_outputs_on_chain_txid_fkey DEFERRABLE INITIALLY DEFERRED;
CREATE FUNCTION pg_temp.reverse_txid(value BYTEA) RETURNS BYTEA LANGUAGE SQL IMMUTABLE STRICT AS $$
    SELECT decode(string_agg(substr(encode(value, 'hex'), i * 2 + 1, 2), '' ORDER BY i DESC), 'hex')
    FROM generate_series(0, octet_length(value) - 1) AS i
$$;
UPDATE wallet_withdrawal_transactions SET on_chain_txid = pg_temp.reverse_txid(on_chain_txid), federation_txid = NULL;
UPDATE wallet_withdrawal_signatures SET on_chain_txid = pg_temp.reverse_txid(on_chain_txid);
UPDATE wallet_withdrawal_transaction_inputs SET
    on_chain_txid = pg_temp.reverse_txid(on_chain_txid),
    previous_output_txid = pg_temp.reverse_txid(previous_output_txid);
UPDATE wallet_withdrawal_transaction_outputs SET on_chain_txid = pg_temp.reverse_txid(on_chain_txid);

ALTER TABLE wallet_withdrawal_addresses ADD COLUMN recipient_amount_msat BIGINT;
CREATE TABLE wallet_withdrawal_recipient_outputs (
    on_chain_txid BYTEA NOT NULL,
    on_chain_vout INTEGER NOT NULL,
    PRIMARY KEY (on_chain_txid, on_chain_vout),
    FOREIGN KEY (on_chain_txid, on_chain_vout)
        REFERENCES wallet_withdrawal_transaction_outputs(on_chain_txid, on_chain_vout)
);
CREATE TABLE wallet_rebuild_progress (
    federation_id BYTEA PRIMARY KEY REFERENCES federations(federation_id),
    next_session INTEGER NOT NULL DEFAULT 0
);
INSERT INTO wallet_rebuild_progress(federation_id) SELECT federation_id FROM federations;

DROP MATERIALIZED VIEW utxos;
CREATE MATERIALIZED VIEW utxos AS
WITH candidates AS (
    SELECT on_chain_txid, on_chain_vout, address, amount_msat, federation_id, 0 AS ownership_priority FROM wallet_peg_ins
    UNION ALL
    SELECT o.on_chain_txid, o.on_chain_vout, o.address, o.amount_msat, t.federation_id, 1 AS ownership_priority
    FROM wallet_withdrawal_transaction_outputs o
    JOIN wallet_withdrawal_transactions t USING (on_chain_txid)
    -- Only infer change for a two-output transaction with exactly one
    -- unambiguously matched recipient. Unknown layouts remain unclassified.
    WHERE (SELECT count(*) FROM wallet_withdrawal_transaction_outputs x WHERE x.on_chain_txid=o.on_chain_txid)=2
      AND (SELECT count(*) FROM wallet_withdrawal_recipient_outputs r WHERE r.on_chain_txid=o.on_chain_txid)=1
      AND NOT EXISTS (SELECT 1 FROM wallet_withdrawal_recipient_outputs r WHERE r.on_chain_txid=o.on_chain_txid AND r.on_chain_vout=o.on_chain_vout)
)
SELECT DISTINCT ON (c.on_chain_txid, c.on_chain_vout)
    c.on_chain_txid, c.on_chain_vout, c.address, c.amount_msat, c.federation_id
FROM candidates c
WHERE NOT EXISTS (
    SELECT 1 FROM wallet_withdrawal_transaction_inputs i
    WHERE i.previous_output_txid=c.on_chain_txid AND i.previous_output_vout=c.on_chain_vout
)
ORDER BY c.on_chain_txid, c.on_chain_vout, c.ownership_priority;
CREATE UNIQUE INDEX on_chain_txid_on_chain_vout ON utxos(on_chain_txid, on_chain_vout);
INSERT INTO schema_version(version) VALUES (11);
