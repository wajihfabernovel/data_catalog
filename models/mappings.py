"""EDM and Dataverse metadata to Azure SQL type mappings."""

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


def map_dataverse_attribute_to_sql(
    attribute_type: str,
    *,
    max_length: int | None = None,
    precision: int | None = None,
    is_primary_id: bool = False,
) -> str:
    """Map Dataverse attribute metadata to an Azure SQL type."""
    attr = (attribute_type or "").strip()
    if is_primary_id or attr == "Uniqueidentifier":
        return "UNIQUEIDENTIFIER"
    if attr in {"Lookup", "Owner", "Customer"}:
        return "UNIQUEIDENTIFIER"
    if attr == "String":
        if max_length and max_length > 0 and max_length <= 4000:
            return f"NVARCHAR({max_length})"
        return "NVARCHAR(MAX)"
    if attr == "Memo":
        return "NVARCHAR(MAX)"
    if attr == "Picklist":
        return "INT"
    if attr == "MultiSelectPicklist":
        return "NVARCHAR(MAX)"
    if attr == "Boolean":
        return "BIT"
    if attr in {"Integer", "State", "Status"}:
        return "INT"
    if attr == "BigInt":
        return "BIGINT"
    if attr in {"Decimal", "Money"}:
        return f"DECIMAL({precision or 18}, 2)"
    if attr == "Double":
        return "FLOAT"
    if attr == "DateTime":
        return "DATETIME2"
    if attr == "Image":
        return "VARBINARY(MAX)"
    return "NVARCHAR(MAX)"
