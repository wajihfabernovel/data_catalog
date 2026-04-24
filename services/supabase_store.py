"""Shared Supabase persistence for the data catalog."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import Client, create_client

from utils.helpers import parse_date, serialize_date, table_key_from_name

load_dotenv()


class SupabaseConfigError(RuntimeError):
    """Raised when required Supabase configuration is missing."""


def load_supabase_config() -> dict:
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    tables_table = os.getenv("SUPABASE_TABLES_TABLE", "catalog_tables").strip()
    columns_table = os.getenv("SUPABASE_COLUMNS_TABLE", "catalog_columns").strip()
    rel_fk_table = os.getenv("SUPABASE_REL_FK_TABLE", "catalog_relationships_fk").strip()
    rel_ref_by_table = os.getenv(
        "SUPABASE_REL_REF_BY_TABLE", "catalog_relationships_referenced_by"
    ).strip()
    journeys_table = os.getenv("SUPABASE_JOURNEYS_TABLE", "journeys").strip()
    journey_steps_table = os.getenv("SUPABASE_JOURNEY_STEPS_TABLE", "journey_steps").strip()
    journey_step_tables_table = os.getenv(
        "SUPABASE_JOURNEY_STEP_TABLES_TABLE", "journey_step_tables"
    ).strip()
    state_transitions_table = os.getenv(
        "SUPABASE_STATE_TRANSITIONS_TABLE", "state_transitions"
    ).strip()

    missing = []
    if not url:
        missing.append("SUPABASE_URL")
    if not key:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")

    return {
        "url": url,
        "key": key,
        "tables_table": tables_table,
        "columns_table": columns_table,
        "rel_fk_table": rel_fk_table,
        "rel_ref_by_table": rel_ref_by_table,
        "journeys_table": journeys_table,
        "journey_steps_table": journey_steps_table,
        "journey_step_tables_table": journey_step_tables_table,
        "state_transitions_table": state_transitions_table,
        "missing": missing,
    }


class SupabaseStore:
    """Persistence wrapper around Supabase tables."""

    def __init__(self) -> None:
        config = load_supabase_config()
        if config["missing"]:
            raise SupabaseConfigError("Missing Supabase configuration: " + ", ".join(config["missing"]))
        self.config = config
        self.client: Client = create_client(config["url"], config["key"])

    def _fetch_all_rows(self, table_name: str, page_size: int = 1000) -> list[dict]:
        rows: list[dict] = []
        start = 0
        while True:
            batch = (
                self.client.table(table_name)
                .select("*")
                .range(start, start + page_size - 1)
                .execute()
                .data
                or []
            )
            rows.extend(batch)
            if len(batch) < page_size:
                break
            start += page_size
        return rows

    @staticmethod
    def _coerce_bool(value) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"true", "yes", "1"}
        return bool(value)

    def fetch_catalog_state(self) -> dict[str, dict]:
        tables_rows = self._fetch_all_rows(self.config["tables_table"])
        columns_rows = self._fetch_all_rows(self.config["columns_table"])
        rel_fk_rows = self._fetch_all_rows(self.config["rel_fk_table"])
        rel_ref_by_rows = self._fetch_all_rows(self.config["rel_ref_by_table"])

        catalog: dict[str, dict] = {}
        for row in tables_rows:
            table_key = row.get("table_key") or table_key_from_name(row.get("table_name", ""))
            catalog[table_key] = {
                "table_key": table_key,
                "table_name": row.get("table_name", ""),
                "primary_key": row.get("primary_key", ""),
                "owning_team": row.get("owning_team", "D&IG"),
                "schema": [],
                "relationships": {"references": [], "referenced_by": []},
                "data_quality": {
                    "nullable_issues": row.get("nullable_issues", ""),
                    "format_inconsistencies": row.get("format_inconsistencies", ""),
                    "duplicate_records": row.get("duplicate_records", "NO"),
                    "orphan_records": row.get("orphan_records", "NO"),
                    "hard_delete_in_use": row.get("hard_delete_in_use", "NO"),
                    "overall_quality_rating": row.get("overall_quality_rating", "ACCEPTABLE"),
                    "quality_notes": row.get("quality_notes", ""),
                },
                "pipeline": {
                    "extract_by_pipeline": row.get("extract_by_pipeline", "UNSURE"),
                    "delta_extraction_column": row.get("delta_extraction_column", ""),
                    "feed_power_bi": row.get("feed_power_bi", "UNSURE"),
                    "key_metrics_or_dimensions": row.get("key_metrics_or_dimensions", ""),
                    "write_path": row.get("write_path", "UNKNOWN"),
                },
                "target_model": {
                    "recommendation": row.get("recommendation", "KEEP AS IS"),
                    "merge_with": row.get("merge_with", ""),
                    "split_into": row.get("split_into", ""),
                    "replaced_by": row.get("replaced_by", ""),
                    "missing_columns": row.get("missing_columns", ""),
                    "missing_constraints": row.get("missing_constraints", ""),
                },
                "signoff": {
                    "completed_by": row.get("completed_by", ""),
                    "reviewed_by": row.get("reviewed_by", ""),
                    "reviewed_by_business": row.get("reviewed_by_business", ""),
                    "status": row.get("status", "DRAFT"),
                    "date_approved": parse_date(row.get("date_approved")),
                    "notes": row.get("notes", ""),
                },
            }

        for row in columns_rows:
            table_key = row.get("table_key") or table_key_from_name(row.get("table_name", ""))
            if table_key in catalog:
                catalog[table_key]["schema"].append(
                    {
                        "column_name": row.get("column_name", ""),
                        "edm_type": row.get("edm_type", ""),
                        "sql_type": row.get("sql_type", ""),
                    }
                )

        for row in rel_fk_rows:
            table_key = row.get("table_key") or table_key_from_name(row.get("table_name", ""))
            if table_key in catalog:
                catalog[table_key]["relationships"]["references"].append(
                    {
                        "fk_column": row.get("fk_column", ""),
                        "references_table": row.get("references_table", ""),
                        "references_column": row.get("references_column", ""),
                        "cardinality": row.get("cardinality", ""),
                        "mandatory": self._coerce_bool(row.get("mandatory")),
                    }
                )

        for row in rel_ref_by_rows:
            table_key = row.get("table_key") or table_key_from_name(row.get("table_name", ""))
            if table_key in catalog:
                catalog[table_key]["relationships"]["referenced_by"].append(
                    {
                        "table_name": row.get("referencing_table_name", ""),
                        "via_column": row.get("via_column", ""),
                        "cardinality": row.get("cardinality", ""),
                    }
                )

        return catalog

    def save_tables(self, tables: list[dict], actor_name: str) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        for table in tables:
            table_key = table["table_key"]
            payload = {
                "table_key": table_key,
                "table_name": table["table_name"],
                "primary_key": table.get("primary_key", ""),
                "owning_team": table.get("owning_team", "D&IG"),
                "nullable_issues": table["data_quality"]["nullable_issues"],
                "format_inconsistencies": table["data_quality"]["format_inconsistencies"],
                "duplicate_records": table["data_quality"]["duplicate_records"],
                "orphan_records": table["data_quality"]["orphan_records"],
                "hard_delete_in_use": table["data_quality"]["hard_delete_in_use"],
                "overall_quality_rating": table["data_quality"]["overall_quality_rating"],
                "quality_notes": table["data_quality"]["quality_notes"],
                "extract_by_pipeline": table["pipeline"]["extract_by_pipeline"],
                "delta_extraction_column": table["pipeline"]["delta_extraction_column"],
                "feed_power_bi": table["pipeline"]["feed_power_bi"],
                "key_metrics_or_dimensions": table["pipeline"]["key_metrics_or_dimensions"],
                "write_path": table["pipeline"]["write_path"],
                "recommendation": table["target_model"]["recommendation"],
                "merge_with": table["target_model"]["merge_with"],
                "split_into": table["target_model"]["split_into"],
                "replaced_by": table["target_model"]["replaced_by"],
                "missing_columns": table["target_model"]["missing_columns"],
                "missing_constraints": table["target_model"]["missing_constraints"],
                "completed_by": table["signoff"]["completed_by"],
                "reviewed_by": table["signoff"]["reviewed_by"],
                "reviewed_by_business": table["signoff"]["reviewed_by_business"],
                "status": table["signoff"]["status"],
                "date_approved": serialize_date(table["signoff"]["date_approved"]),
                "notes": table["signoff"]["notes"],
                "last_synced_at": timestamp,
                "last_modified_by": actor_name,
            }
            self.client.table(self.config["tables_table"]).upsert(payload, on_conflict="table_key").execute()

            self.client.table(self.config["columns_table"]).delete().eq("table_key", table_key).execute()
            if table.get("schema"):
                self.client.table(self.config["columns_table"]).insert(
                    [
                        {
                            "table_key": table_key,
                            "table_name": table["table_name"],
                            "column_name": col["column_name"],
                            "edm_type": col["edm_type"],
                            "sql_type": col["sql_type"],
                        }
                        for col in table["schema"]
                    ]
                ).execute()

            self.client.table(self.config["rel_fk_table"]).delete().eq("table_key", table_key).execute()
            references = table.get("relationships", {}).get("references", [])
            if references:
                self.client.table(self.config["rel_fk_table"]).insert(
                    [
                        {
                            "table_key": table_key,
                            "table_name": table["table_name"],
                            "fk_column": row.get("fk_column", ""),
                            "references_table": row.get("references_table", ""),
                            "references_column": row.get("references_column", ""),
                            "cardinality": row.get("cardinality", ""),
                            "mandatory": self._coerce_bool(row.get("mandatory")),
                        }
                        for row in references
                    ]
                ).execute()

            self.client.table(self.config["rel_ref_by_table"]).delete().eq("table_key", table_key).execute()
            ref_by = table.get("relationships", {}).get("referenced_by", [])
            if ref_by:
                self.client.table(self.config["rel_ref_by_table"]).insert(
                    [
                        {
                            "table_key": table_key,
                            "table_name": table["table_name"],
                            "referencing_table_name": row.get("table_name", ""),
                            "via_column": row.get("via_column", ""),
                            "cardinality": row.get("cardinality", ""),
                        }
                        for row in ref_by
                    ]
                ).execute()
