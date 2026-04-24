"""Shared helpers for the Streamlit data catalog app."""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import date, datetime


CHOICES_YES_NO = ["YES", "NO"]
CHOICES_YES_NO_UNSURE = ["YES", "NO", "UNSURE"]
QUALITY_RATINGS = ["CLEAN", "ACCEPTABLE", "PROBLEMATIC"]
WRITE_PATHS = ["STORED PROCEDURE", "DAB", "DIRECT SQL", "UNKNOWN"]
TARGET_RECOMMENDATIONS = ["KEEP AS IS", "REFACTOR", "MERGE WITH", "SPLIT INTO", "RETIRE"]
SIGNOFF_STATUS = ["DRAFT", "IN REVIEW", "APPROVED"]
TEAM_OPTIONS = [
    "D&IG",
    "Strategy",
    "S&R",
    "Modular Innovation",
    "Analytics",
    "Integration & Localization",
]
JOURNEY_MODULE_OPTIONS = ["Strategy", "D&IG", "S&R", "MI", "Analytics"]
JOURNEY_ROLE_OPTIONS = [
    "Product Manager",
    "R&D Scientist",
    "R&D Manager",
    "Packaging Engineer",
    "Regulatory Specialist",
    "Product Admin",
    "Admin",
]
JOURNEY_FREQUENCY_OPTIONS = ["Daily", "Weekly", "Monthly", "Ad-hoc"]
JOURNEY_COMPLEXITY_OPTIONS = ["Low", "Medium", "High"]
JOURNEY_WRITE_OPERATIONS = ["INSERT", "UPDATE", "DELETE", "UPSERT"]


def normalize_table_names(raw_value: str) -> list[str]:
    return [name.strip() for name in raw_value.split(",") if name.strip()]


def slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower()).strip("_") or "item"


def table_key_from_name(table_name: str) -> str:
    return slugify(table_name)


def widget_key(table_key: str, field_name: str) -> str:
    return f"{table_key}__{field_name}"


def build_default_table_state(parsed_table: dict) -> dict:
    return {
        "table_key": parsed_table["table_key"],
        "table_name": parsed_table["table_name"],
        "primary_key": parsed_table.get("primary_key", ""),
        "owning_team": "D&IG",
        "schema": deepcopy(parsed_table.get("schema", [])),
        "relationships": {
            "references": deepcopy(parsed_table.get("relationships", {}).get("references", [])),
            "referenced_by": deepcopy(parsed_table.get("relationships", {}).get("referenced_by", [])),
        },
        "metadata_profile": deepcopy(parsed_table.get("metadata_profile", {})),
        "data_quality": {
            "nullable_issues": "",
            "format_inconsistencies": "",
            "duplicate_records": "NO",
            "orphan_records": "NO",
            "hard_delete_in_use": "NO",
            "overall_quality_rating": "ACCEPTABLE",
            "quality_notes": "",
        },
        "pipeline": {
            "extract_by_pipeline": "UNSURE",
            "delta_extraction_column": "",
            "feed_power_bi": "UNSURE",
            "key_metrics_or_dimensions": "",
            "write_path": "UNKNOWN",
        },
        "target_model": {
            "recommendation": "KEEP AS IS",
            "merge_with": "",
            "split_into": "",
            "replaced_by": "",
            "missing_columns": "",
            "missing_constraints": "",
        },
        "signoff": {
            "completed_by": "",
            "reviewed_by": "",
            "reviewed_by_business": "",
            "status": "DRAFT",
            "date_approved": None,
            "notes": "",
        },
    }


def merge_table_state(base_table: dict, stored_table: dict | None) -> dict:
    merged = deepcopy(base_table)
    if not stored_table:
        return merged

    for key in ["table_name", "primary_key", "table_key", "owning_team"]:
        if stored_table.get(key):
            merged[key] = stored_table[key]

    if stored_table.get("schema"):
        merged_schema = {column["column_name"]: deepcopy(column) for column in merged.get("schema", [])}
        for column in stored_table["schema"]:
            existing = merged_schema.get(column["column_name"], {})
            merged_schema[column["column_name"]] = {
                **existing,
                **deepcopy(column),
                "edm_type": existing.get("edm_type", column.get("edm_type", "")),
                "sql_type": existing.get("sql_type", column.get("sql_type", "")),
            }
        merged["schema"] = sorted(merged_schema.values(), key=lambda col: col["column_name"].casefold())

    stored_relationships = deepcopy(stored_table.get("relationships", {}))
    if stored_relationships:
        if stored_relationships.get("references"):
            merged["relationships"]["references"] = stored_relationships["references"]
        if stored_relationships.get("referenced_by"):
            merged["relationships"]["referenced_by"] = stored_relationships["referenced_by"]

    if stored_table.get("metadata_profile"):
        merged["metadata_profile"] = deepcopy(stored_table["metadata_profile"])

    for section in ["data_quality", "pipeline", "target_model", "signoff"]:
        merged[section].update(deepcopy(stored_table.get(section, {})))

    return merged


def normalize_graph_item(item: dict) -> dict:
    fields = item.get("fields", {})
    return {
        "item_id": item.get("id"),
        "fields": fields,
    }


def serialize_date(value: date | datetime | str | None) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def sanitize_sheet_name(name: str) -> str:
    invalid = '[]:*?/\\'
    sanitized = "".join("_" if char in invalid else char for char in name)
    sanitized = sanitized.strip() or "Sheet"
    return sanitized[:31]


def next_journey_id(existing_ids: list[str]) -> str:
    highest = 0
    for journey_id in existing_ids:
        if not journey_id or not journey_id.startswith("J"):
            continue
        suffix = journey_id[1:]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return f"J{highest + 1:03d}"


def normalize_free_text_tables(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def classify_access_pattern(read_count: int, write_count: int) -> str:
    if read_count > write_count * 2:
        return "Read-Heavy"
    if write_count > read_count:
        return "Write-Heavy"
    return "Balanced"


def classify_centrality(journey_count: int) -> str:
    if journey_count >= 5:
        return "HIGH"
    if journey_count >= 2:
        return "MEDIUM"
    return "LOW"
