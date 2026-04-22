"""EDM to Azure SQL type mappings."""

from __future__ import annotations

EDM_TO_SQL_TYPE = {
    "Edm.String": "NVARCHAR(MAX)",
    "Edm.Int32": "INT",
    "Edm.Int64": "BIGINT",
    "Edm.Decimal": "DECIMAL(18,6)",
    "Edm.Guid": "UNIQUEIDENTIFIER",
    "Edm.DateTimeOffset": "DATETIMEOFFSET",
    "Edm.Binary": "VARBINARY(MAX)",
}


def map_edm_to_sql(edm_type: str) -> str:
    """Map an EDM type to an Azure SQL type with a safe fallback."""
    return EDM_TO_SQL_TYPE.get(edm_type, "NVARCHAR(MAX)")
