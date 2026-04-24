"""Supabase persistence for user journey mapping."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from services.journey_export import parse_journey_workbook
from services.supabase_store import SupabaseStore, load_supabase_config
from utils.helpers import (
    classify_access_pattern,
    classify_centrality,
)


class JourneysStore(SupabaseStore):
    """Persistence wrapper around Supabase tables for journey mapping."""

    def __init__(self) -> None:
        super().__init__()
        config = load_supabase_config()
        self.journeys_table = config.get("journeys_table", "journeys")
        self.journey_steps_table = config.get("journey_steps_table", "journey_steps")
        self.journey_step_tables_table = config.get("journey_step_tables_table", "journey_step_tables")
        self.state_transitions_table = config.get("state_transitions_table", "state_transitions")

    def fetch_journeys(self) -> list[dict[str, Any]]:
        rows = self._fetch_all_rows(self.journeys_table)
        return sorted(rows, key=lambda row: row.get("journey_id", ""))

    def fetch_journey(self, journey_id: str) -> dict[str, Any] | None:
        rows = (
            self.client.table(self.journeys_table).select("*").eq("journey_id", journey_id).limit(1).execute().data
            or []
        )
        if not rows:
            return None
        journey = rows[0]
        steps = self.fetch_journey_steps(journey_id)
        return {**journey, "steps": steps}

    def fetch_journey_steps(self, journey_id: str) -> list[dict[str, Any]]:
        steps = (
            self.client.table(self.journey_steps_table)
            .select("*")
            .eq("journey_id", journey_id)
            .order("step_number")
            .execute()
            .data
            or []
        )
        step_tables = (
            self.client.table(self.journey_step_tables_table)
            .select("*")
            .eq("journey_id", journey_id)
            .order("step_number")
            .execute()
            .data
            or []
        )
        transitions = (
            self.client.table(self.state_transitions_table)
            .select("*")
            .eq("journey_id", journey_id)
            .order("step_number")
            .execute()
            .data
            or []
        )

        tables_by_step: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in step_tables:
            tables_by_step[int(row.get("step_number", 0))].append(row)

        transitions_by_step: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in transitions:
            transitions_by_step[int(row.get("step_number", 0))].append(row)

        hydrated: list[dict[str, Any]] = []
        for step in steps:
            step_number = int(step.get("step_number", 0))
            hydrated.append(
                {
                    **step,
                    "table_refs": tables_by_step.get(step_number, []),
                    "transitions": transitions_by_step.get(step_number, []),
                }
            )
        return hydrated

    def fetch_state_transitions(self, entity_table: str | None = None) -> list[dict[str, Any]]:
        query = self.client.table(self.state_transitions_table).select("*").order("entity_table").order("step_number")
        if entity_table:
            query = query.eq("entity_table", entity_table)
        return query.execute().data or []

    def save_journey(
        self,
        journey: dict[str, Any],
        steps: list[dict[str, Any]],
        step_tables: list[dict[str, Any]],
        transitions: list[dict[str, Any]],
        actor_name: str,
    ) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        existing = (
            self.client.table(self.journeys_table)
            .select("created_at")
            .eq("journey_id", journey["journey_id"])
            .limit(1)
            .execute()
            .data
            or []
        )
        created_at = existing[0].get("created_at") if existing else timestamp
        journey_payload = {
            "journey_id": journey["journey_id"],
            "journey_name": journey.get("journey_name", ""),
            "module_domain": journey.get("module_domain", ""),
            "primary_user_role": journey.get("primary_user_role", ""),
            "frequency": journey.get("frequency", ""),
            "complexity": journey.get("complexity", ""),
            "interview_date": journey.get("interview_date") or None,
            "interviewer": journey.get("interviewer", ""),
            "scrum_team": journey.get("scrum_team", ""),
            "total_steps": len(steps),
            "created_at": created_at,
            "updated_at": timestamp,
            "last_modified_by": actor_name,
        }
        self.client.table(self.journeys_table).upsert(journey_payload, on_conflict="journey_id").execute()

        self.client.table(self.journey_steps_table).delete().eq("journey_id", journey["journey_id"]).execute()
        self.client.table(self.journey_step_tables_table).delete().eq("journey_id", journey["journey_id"]).execute()
        self.client.table(self.state_transitions_table).delete().eq("journey_id", journey["journey_id"]).execute()

        if steps:
            self.client.table(self.journey_steps_table).insert(
                [
                    {
                        "journey_id": journey["journey_id"],
                        "step_number": int(step["step_number"]),
                        "user_action": step.get("user_action", ""),
                        "screen_component": step.get("screen_component", ""),
                        "status_field_changes": step.get("status_field_changes", ""),
                        "validation_rules": step.get("validation_rules", ""),
                        "business_rules": step.get("business_rules", ""),
                        "notes": step.get("notes", ""),
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    }
                    for step in steps
                ]
            ).execute()

        if step_tables:
            self.client.table(self.journey_step_tables_table).insert(
                [
                    {
                        "journey_id": journey["journey_id"],
                        "step_number": int(row["step_number"]),
                        "table_name": row.get("table_name", ""),
                        "access_mode": row.get("access_mode", "READ"),
                        "write_operation": row.get("write_operation"),
                        "is_catalog_table": bool(row.get("is_catalog_table")),
                        "catalog_table_key": row.get("catalog_table_key"),
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    }
                    for row in step_tables
                ]
            ).execute()

        if transitions:
            self.client.table(self.state_transitions_table).insert(
                [
                    {
                        "journey_id": journey["journey_id"],
                        "step_number": int(row.get("step_number") or 1),
                        "entity_table": row.get("entity_table", ""),
                        "status_field_name": row.get("status_field_name", ""),
                        "from_state": row.get("from_state"),
                        "to_state": row.get("to_state", ""),
                        "trigger_action": row.get("trigger_action", ""),
                        "user_role_required": row.get("user_role_required", ""),
                        "validation_rules": row.get("validation_rules", ""),
                        "side_effects": row.get("side_effects", ""),
                        "created_at": timestamp,
                        "updated_at": timestamp,
                    }
                    for row in transitions
                ]
            ).execute()

    def delete_journey(self, journey_id: str) -> None:
        self.client.table(self.state_transitions_table).delete().eq("journey_id", journey_id).execute()
        self.client.table(self.journey_step_tables_table).delete().eq("journey_id", journey_id).execute()
        self.client.table(self.journey_steps_table).delete().eq("journey_id", journey_id).execute()
        self.client.table(self.journeys_table).delete().eq("journey_id", journey_id).execute()

    def import_journey_workbook(self, file_bytes: bytes, actor_name: str, catalog_tables: dict[str, dict]) -> int:
        payloads = parse_journey_workbook(file_bytes)
        catalog_lookup = {
            table.get("table_name", "").strip().casefold(): table
            for table in catalog_tables.values()
            if table.get("table_name")
        }
        for payload in payloads:
            step_tables = []
            for row in payload["step_tables"]:
                table_name = row.get("table_name", "").strip()
                catalog_match = catalog_lookup.get(table_name.casefold())
                step_tables.append(
                    {
                        **row,
                        "table_name": table_name,
                        "is_catalog_table": bool(catalog_match),
                        "catalog_table_key": catalog_match.get("table_key") if catalog_match else None,
                    }
                )
            self.save_journey(
                journey=payload["journey"],
                steps=payload["steps"],
                step_tables=step_tables,
                transitions=payload["transitions"],
                actor_name=actor_name,
            )
        return len(payloads)

    def fetch_journey_analysis(
        self,
        journey_ids: list[str],
        catalog_tables: dict[str, dict],
        annotations: dict[str, dict[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        annotations = annotations or {}
        if not journey_ids:
            return []

        rows = (
            self.client.table(self.journey_step_tables_table)
            .select("*")
            .in_("journey_id", journey_ids)
            .execute()
            .data
            or []
        )
        summary: dict[str, dict[str, Any]] = {}
        catalog_lookup = {
            table.get("table_name", "").strip().casefold(): table for table in catalog_tables.values()
        }
        for row in rows:
            table_name = row.get("table_name", "").strip()
            if not table_name:
                continue
            bucket = summary.setdefault(
                table_name,
                {
                    "table_name": table_name,
                    "journey_ids_set": set(),
                    "read_count": 0,
                    "write_count": 0,
                },
            )
            bucket["journey_ids_set"].add(row.get("journey_id"))
            if row.get("access_mode") == "WRITE":
                bucket["write_count"] += 1
            else:
                bucket["read_count"] += 1

        analysis_rows: list[dict[str, Any]] = []
        for table_name, bucket in sorted(summary.items(), key=lambda item: item[0].casefold()):
            journey_id_list = sorted(bucket["journey_ids_set"])
            read_count = bucket["read_count"]
            write_count = bucket["write_count"]
            catalog_match = catalog_lookup.get(table_name.casefold())
            note = annotations.get(table_name, {})
            analysis_rows.append(
                {
                    "table_name": table_name,
                    "domain": catalog_match.get("owning_team", "Uncataloged") if catalog_match else "Uncataloged",
                    "journey_ids": ", ".join(journey_id_list),
                    "journey_ids_list": journey_id_list,
                    "read_count": read_count,
                    "write_count": write_count,
                    "access_pattern": classify_access_pattern(read_count, write_count),
                    "centrality_score": classify_centrality(len(journey_id_list)),
                    "legacy_table_type": note.get("legacy_table_type", ""),
                    "target_entity_proposed": note.get("target_entity_proposed", ""),
                    "migration_priority": note.get("migration_priority", ""),
                    "is_catalog_table": bool(catalog_match),
                    "catalog_table_key": catalog_match.get("table_key") if catalog_match else None,
                }
            )
        return analysis_rows

    def build_table_network(self, journey_ids: list[str]) -> list[tuple[str, str, str]]:
        if not journey_ids:
            return []
        rows = (
            self.client.table(self.journey_step_tables_table)
            .select("journey_id,step_number,table_name")
            .in_("journey_id", journey_ids)
            .execute()
            .data
            or []
        )
        step_map: dict[tuple[str, int], list[str]] = defaultdict(list)
        for row in rows:
            key = (row.get("journey_id", ""), int(row.get("step_number", 0)))
            table_name = row.get("table_name", "").strip()
            if table_name:
                step_map[key].append(table_name)
        edges: set[tuple[str, str, str]] = set()
        for (journey_id, _), tables in step_map.items():
            deduped = sorted(set(tables))
            for idx, left in enumerate(deduped):
                for right in deduped[idx + 1 :]:
                    edges.add((left, right, journey_id))
        return sorted(edges)
