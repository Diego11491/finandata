from __future__ import annotations

from typing import Any
from uuid import uuid4

import great_expectations as gx
import pandas as pd


def run_gx_suite(data: pd.DataFrame) -> dict[str, Any]:
    """Ejecuta Expectations oficiales como evidencia automatizada del lote."""
    suffix = uuid4().hex[:8]
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name=f"finandata-{suffix}")
    asset = source.add_dataframe_asset(name=f"transactions-{suffix}")
    definition = asset.add_batch_definition_whole_dataframe(f"batch-{suffix}")
    batch = definition.get_batch(batch_parameters={"dataframe": data})

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="transaction_id", severity="critical"
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="transaction_id", severity="critical"
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount", min_value=0, severity="critical"
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="iban_from", severity="critical"
        ),
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="iban_from",
            regex=r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$",
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="currency", value_set=["PEN", "USD"], severity="critical"
        ),
    ]

    results: list[dict[str, Any]] = []
    for expectation in expectations:
        validation = batch.validate(expectation)
        results.append(
            {
                "expectation": expectation.__class__.__name__,
                "success": bool(validation.success),
                "unexpected_count": int(validation.result.get("unexpected_count", 0)),
            }
        )

    return {
        "success": all(result["success"] for result in results),
        "evaluated_expectations": len(results),
        "results": results,
    }
