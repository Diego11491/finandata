from scripts.generate_data import build_base_dataset
from src.transformations import transform_transactions


def test_sensitive_fields_are_masked_before_loading():
    source = build_base_dataset().head(2)
    transformed = transform_transactions(source, "TEST-BATCH")

    assert "customer_document" not in transformed.columns
    assert "card_number" not in transformed.columns
    assert "iban_from" not in transformed.columns
    assert transformed["customer_document_hash"].str.len().eq(64).all()
    assert transformed["card_masked"].str[-4:].tolist() == source[
        "card_number"
    ].str[-4:].tolist()
    assert transformed["batch_id"].eq("TEST-BATCH").all()


def test_financial_double_entry_is_balanced():
    transformed = transform_transactions(build_base_dataset(), "TEST-BATCH")
    assert transformed["debit_amount"].sum() == transformed["credit_amount"].sum()


def test_record_hash_is_stable_across_reprocessing():
    source = build_base_dataset().head(10)
    first = transform_transactions(source, "BATCH-001")
    second = transform_transactions(source, "BATCH-002")

    assert first["record_hash"].tolist() == second["record_hash"].tolist()


def test_record_hash_changes_when_business_data_changes():
    source = build_base_dataset().head(1)
    original = transform_transactions(source, "BATCH-001")
    changed_source = source.copy()
    changed_source.loc[changed_source.index[0], "amount"] += 100
    changed = transform_transactions(changed_source, "BATCH-002")

    assert original.iloc[0]["record_hash"] != changed.iloc[0]["record_hash"]
