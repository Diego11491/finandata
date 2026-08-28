from scripts.generate_data import build_base_dataset, inject_incident
from src.quality import evaluate_quality_gate, validate_transactions


def test_healthy_dataset_passes_quality_gate():
    result = validate_transactions(build_base_dataset())
    gate = evaluate_quality_gate(result.summary, rejection_threshold=0.01)

    assert len(result.accepted) == 300
    assert len(result.rejected) == 0
    assert gate["passed"] is True


def test_incident_has_exactly_fifteen_percent_rejected():
    incident = inject_incident(build_base_dataset())
    result = validate_transactions(incident)
    gate = evaluate_quality_gate(result.summary, rejection_threshold=0.01)

    assert len(result.rejected) == 45
    assert result.summary["rejection_rate"] == 0.15
    assert gate["passed"] is False

