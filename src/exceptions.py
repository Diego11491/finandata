class DataQualityGateError(RuntimeError):
    """El lote no cumple las reglas que permiten avanzar a transformación/carga."""


class ReconciliationError(RuntimeError):
    """La carga no cuadra financieramente o difiere de los registros esperados."""

