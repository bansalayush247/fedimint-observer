WITH matches AS (
    SELECT o.on_chain_txid, o.on_chain_vout, a.txid
    FROM wallet_withdrawal_transaction_outputs o
    JOIN wallet_withdrawal_addresses a
      ON a.address = o.address AND a.recipient_amount_msat = o.amount_msat
    WHERE o.on_chain_txid = $1 AND a.federation_id = $2
      AND (a.session_index, a.item_index) <= ($3, $4)
      AND NOT EXISTS (
          SELECT 1 FROM wallet_withdrawal_transactions used
          WHERE used.federation_id = a.federation_id
            AND used.federation_txid = a.txid AND used.on_chain_txid <> $1
      )
), recipient AS (
    INSERT INTO wallet_withdrawal_recipient_outputs
    SELECT on_chain_txid, on_chain_vout FROM matches
    WHERE (SELECT count(*) FROM matches) = 1
    ON CONFLICT DO NOTHING
)
UPDATE wallet_withdrawal_transactions
SET federation_txid = (SELECT txid FROM matches)
WHERE on_chain_txid = $1 AND (SELECT count(*) FROM matches) = 1;
