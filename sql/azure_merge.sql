-- El pipeline crea #staging_transactions, clasifica el evento de auditoría y
-- ejecuta el MERGE y el INSERT del historial dentro de la misma transacción.
MERGE finandata.fact_transactions AS target
USING #staging_transactions AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN
    UPDATE SET
        account_from = source.account_from,
        account_to = source.account_to,
        amount = source.amount,
        currency = source.currency,
        transaction_ts = source.transaction_ts,
        branch_id = source.branch_id,
        channel = source.channel,
        commission = source.commission,
        risk_score = source.risk_score,
        risk_level = source.risk_level,
        customer_document_hash = source.customer_document_hash,
        card_masked = source.card_masked,
        iban_from_masked = source.iban_from_masked,
        iban_to_masked = source.iban_to_masked,
        debit_amount = source.debit_amount,
        credit_amount = source.credit_amount,
        batch_id = source.batch_id,
        processed_at = source.processed_at,
        source_file = source.source_file,
        source_row = source.source_row,
        record_hash = source.record_hash,
        last_modified_at = CASE
            WHEN target.record_hash <> source.record_hash
                THEN source.processed_at
            ELSE target.last_modified_at
        END,
        last_modified_batch_id = CASE
            WHEN target.record_hash <> source.record_hash
                THEN source.batch_id
            ELSE target.last_modified_batch_id
        END
WHEN NOT MATCHED THEN
    INSERT (
        transaction_id, account_from, account_to, amount, currency,
        transaction_ts, branch_id, channel, commission, risk_score,
        risk_level, customer_document_hash, card_masked, iban_from_masked,
        iban_to_masked, debit_amount, credit_amount, batch_id, processed_at,
        source_file, source_row, record_hash, created_at, last_modified_at,
        created_batch_id, last_modified_batch_id
    )
    VALUES (
        source.transaction_id, source.account_from, source.account_to,
        source.amount, source.currency, source.transaction_ts, source.branch_id,
        source.channel, source.commission, source.risk_score, source.risk_level,
        source.customer_document_hash, source.card_masked,
        source.iban_from_masked, source.iban_to_masked, source.debit_amount,
        source.credit_amount, source.batch_id, source.processed_at,
        source.source_file, source.source_row, source.record_hash,
        source.processed_at, source.processed_at, source.batch_id,
        source.batch_id
    );
