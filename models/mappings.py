"""EDM and Dataverse metadata to Azure SQL type mappings."""

from __future__ import annotations

EDM_TO_SQL_TYPE = {
    "Edm.String": "NVARCHAR(MAX)",
    "Edm.Int16": "SMALLINT",
    "Edm.Int32": "INT",
    "Edm.Int64": "BIGINT",
    "Edm.Decimal": "DECIMAL(18,6)",
    "Edm.Double": "FLOAT",
    "Edm.Single": "REAL",
    "Edm.Boolean": "BIT",
    "Edm.Byte": "TINYINT",
    "Edm.SByte": "SMALLINT",
    "Edm.Guid": "UNIQUEIDENTIFIER",
    "Edm.DateTime": "DATETIME2",
    "Edm.DateTimeOffset": "DATETIMEOFFSET",
    "Edm.Binary": "VARBINARY(MAX)",
}


def map_edm_to_sql(edm_type: str) -> str:
    """Map an OData EDM type to an Azure SQL type with a safe fallback."""
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
    if attr in {"Lookup", "Owner", "Customer", "PartyList"}:
        return "UNIQUEIDENTIFIER" if attr != "PartyList" else "NVARCHAR(MAX)"
    if attr == "String":
        if max_length and 1 <= max_length <= 4000:
            return f"NVARCHAR({max_length})"
        return "NVARCHAR(MAX)"
    if attr in {"Memo", "MultiSelectPicklist", "Virtual", "CalendarRules"}:
        return "NVARCHAR(MAX)"
    if attr == "EntityName":
        return "NVARCHAR(128)"
    if attr == "Picklist":
        return "INT"
    if attr in {"Boolean", "ManagedProperty"}:
        return "BIT"
    if attr in {"Integer", "State", "Status"}:
        return "INT"
    if attr == "BigInt":
        return "BIGINT"
    if attr == "Decimal":
        prec = precision if isinstance(precision, int) and 0 < precision <= 38 else 10
        return f"DECIMAL(38, {prec})"
    if attr == "Money":
        prec = precision if isinstance(precision, int) and 0 < precision <= 10 else 4
        return f"DECIMAL(38, {prec})"
    if attr == "Double":
        return "FLOAT"
    if attr == "DateTime":
        return "DATETIME2"
    if attr in {"Image", "File"}:
        return "VARBINARY(MAX)"
    return "NVARCHAR(MAX)"


def map_attr_type_to_sql(
    attr_type: str,
    max_length: int | None = None,
    precision: int | None = None,
) -> str:
    """Positional-friendly alias used by the Dataverse API service."""
    return map_dataverse_attribute_to_sql(attr_type, max_length=max_length, precision=precision)
