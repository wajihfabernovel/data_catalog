"""Streamlit UI for the user journey mapping module."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from services.journey_export import (
    build_journey_workbook,
    build_state_machine_json,
    format_transition_summary,
)
from services.journeys_store import JourneysStore
from services.supabase_store import SupabaseConfigError
from utils.helpers import (
    JOURNEY_COMPLEXITY_OPTIONS,
    JOURNEY_FREQUENCY_OPTIONS,
    JOURNEY_MODULE_OPTIONS,
    JOURNEY_ROLE_OPTIONS,
    JOURNEY_WRITE_OPERATIONS,
    next_journey_id,
    normalize_free_text_tables,
)


EXPORT_DIR = Path(__file__).resolve().parent.parent / "exports"


def _blank_transition(trigger_action: str = "") -> dict[str, str]:
    return {
        "entity_table": "",
        "status_field_name": "",
        "from_state": "",
        "to_state": "",
        "trigger_action": trigger_action,
        "user_role_required": "",
        "validation_rules": "",
        "side_effects": "",
    }


def _blank_step(step_number: int) -> dict[str, Any]:
    return {
        "step_number": step_number,
        "user_action": "",
        "screen_component": "",
        "tables_read_known": [],
        "tables_read_extra": "",
        "tables_written_known": [],
        "tables_written_extra": "",
        "validation_rules": "",
        "business_rules": "",
        "notes": "",
        "transitions": [],
    }


def _journey_store() -> JourneysStore | None:
    try:
        return JourneysStore()
    except (RuntimeError, SupabaseConfigError) as exc:
        st.error(str(exc))
        return None


def _ensure_editor_state(existing_journeys: list[dict[str, Any]]) -> None:
    st.session_state.setdefault("journey_editor_loaded_id", "")
    st.session_state.setdefault("journey_editor_analysis_annotations", {})
    st.session_state.setdefault("journey_editor_journey_id", next_journey_id([j["journey_id"] for j in existing_journeys]))
    st.session_state.setdefault("journey_editor_journey_name", "")
    st.session_state.setdefault("journey_editor_module_domain", JOURNEY_MODULE_OPTIONS[0])
    st.session_state.setdefault("journey_editor_user_roles", [])
    st.session_state.setdefault("journey_editor_frequency", JOURNEY_FREQUENCY_OPTIONS[0])
    st.session_state.setdefault("journey_editor_complexity", JOURNEY_COMPLEXITY_OPTIONS[0])
    st.session_state.setdefault("journey_editor_interview_date", date.today())
    st.session_state.setdefault("journey_editor_interviewer", "Wajih")
    st.session_state.setdefault("journey_editor_steps", [_blank_step(1)])


def _reset_editor(existing_journeys: list[dict[str, Any]]) -> None:
    keys_to_clear = [key for key in st.session_state.keys() if key.startswith("journey_step_")]
    for key in keys_to_clear:
        st.session_state.pop(key)
    st.session_state["journey_editor_loaded_id"] = ""
    st.session_state["journey_editor_journey_id"] = next_journey_id([j["journey_id"] for j in existing_journeys])
    st.session_state["journey_editor_journey_name"] = ""
    st.session_state["journey_editor_module_domain"] = JOURNEY_MODULE_OPTIONS[0]
    st.session_state["journey_editor_user_roles"] = []
    st.session_state["journey_editor_frequency"] = JOURNEY_FREQUENCY_OPTIONS[0]
    st.session_state["journey_editor_complexity"] = JOURNEY_COMPLEXITY_OPTIONS[0]
    st.session_state["journey_editor_interview_date"] = date.today()
    st.session_state["journey_editor_interviewer"] = "Wajih"
    st.session_state["journey_editor_steps"] = [_blank_step(1)]


def _load_editor(journey: dict[str, Any], existing_journeys: list[dict[str, Any]]) -> None:
    _reset_editor(existing_journeys)
    st.session_state["journey_editor_loaded_id"] = journey["journey_id"]
    st.session_state["journey_editor_journey_id"] = journey["journey_id"]
    st.session_state["journey_editor_journey_name"] = journey.get("journey_name", "")
    st.session_state["journey_editor_module_domain"] = journey.get("module_domain") or JOURNEY_MODULE_OPTIONS[0]
    stored_roles = journey.get("primary_user_role", "")
    st.session_state["journey_editor_user_roles"] = [role.strip() for role in stored_roles.split(",") if role.strip()]
    st.session_state["journey_editor_frequency"] = journey.get("frequency") or JOURNEY_FREQUENCY_OPTIONS[0]
    st.session_state["journey_editor_complexity"] = journey.get("complexity") or JOURNEY_COMPLEXITY_OPTIONS[0]
    interview_date = journey.get("interview_date")
    st.session_state["journey_editor_interview_date"] = date.fromisoformat(interview_date) if interview_date else date.today()
    st.session_state["journey_editor_interviewer"] = journey.get("interviewer", "Wajih")

    steps_payload: list[dict[str, Any]] = []
    for step in sorted(journey.get("steps", []), key=lambda item: item["step_number"]):
        step_number = int(step["step_number"])
        reads = [ref["table_name"] for ref in step.get("table_refs", []) if ref.get("access_mode") == "READ"]
        writes = [ref for ref in step.get("table_refs", []) if ref.get("access_mode") == "WRITE"]
        transitions = step.get("transitions", [])
        steps_payload.append(
            {
                "step_number": step_number,
                "user_action": step.get("user_action", ""),
                "screen_component": step.get("screen_component", ""),
                "tables_read_known": [ref["table_name"] for ref in step.get("table_refs", []) if ref.get("access_mode") == "READ" and ref.get("is_catalog_table")],
                "tables_read_extra": ", ".join([name for name in reads if name not in [ref["table_name"] for ref in step.get("table_refs", []) if ref.get("access_mode") == "READ" and ref.get("is_catalog_table")]]),
                "tables_written_known": [ref["table_name"] for ref in writes if ref.get("is_catalog_table")],
                "tables_written_extra": ", ".join([ref["table_name"] for ref in writes if not ref.get("is_catalog_table")]),
                "validation_rules": step.get("validation_rules", ""),
                "business_rules": step.get("business_rules", ""),
                "notes": step.get("notes", ""),
                "transitions": [
                    {
                        "entity_table": item.get("entity_table", ""),
                        "status_field_name": item.get("status_field_name", ""),
                        "from_state": item.get("from_state", ""),
                        "to_state": item.get("to_state", ""),
                        "trigger_action": item.get("trigger_action", ""),
                        "user_role_required": item.get("user_role_required", ""),
                        "validation_rules": item.get("validation_rules", ""),
                        "side_effects": item.get("side_effects", ""),
                    }
                    for item in transitions
                ],
                "write_operations": {
                    ref["table_name"]: ref.get("write_operation", "UPDATE") or "UPDATE" for ref in writes
                },
            }
        )
    st.session_state["journey_editor_steps"] = steps_payload or [_blank_step(1)]


def _catalog_names(catalog_tables: dict[str, dict]) -> list[str]:
    return sorted(
        [table["table_name"] for table in catalog_tables.values() if table.get("table_name")],
        key=str.casefold,
    )


def _seed_step_widget_defaults(step: dict[str, Any], idx: int) -> None:
    prefix = f"journey_step_{idx}"
    st.session_state.setdefault(f"{prefix}_step_number", step["step_number"])
    st.session_state.setdefault(f"{prefix}_user_action", step.get("user_action", ""))
    st.session_state.setdefault(f"{prefix}_screen_component", step.get("screen_component", ""))
    st.session_state.setdefault(f"{prefix}_tables_read_known", step.get("tables_read_known", []))
    st.session_state.setdefault(f"{prefix}_tables_read_extra", step.get("tables_read_extra", ""))
    st.session_state.setdefault(f"{prefix}_tables_written_known", step.get("tables_written_known", []))
    st.session_state.setdefault(f"{prefix}_tables_written_extra", step.get("tables_written_extra", ""))
    st.session_state.setdefault(f"{prefix}_validation_rules", step.get("validation_rules", ""))
    st.session_state.setdefault(f"{prefix}_business_rules", step.get("business_rules", ""))
    st.session_state.setdefault(f"{prefix}_notes", step.get("notes", ""))
    transitions = step.get("transitions", [])
    transition_df = pd.DataFrame(
        transitions,
        columns=[
            "entity_table",
            "status_field_name",
            "from_state",
            "to_state",
            "trigger_action",
            "user_role_required",
            "validation_rules",
            "side_effects",
        ],
    )
    st.session_state.setdefault(f"{prefix}_transitions_data", transition_df)
    for table_name, operation in step.get("write_operations", {}).items():
        st.session_state.setdefault(f"{prefix}_write_op_{table_name}", operation)


def _collect_editor_payload(catalog_tables: dict[str, dict]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    journey = {
        "journey_id": st.session_state.get("journey_editor_journey_id", "").strip(),
        "journey_name": st.session_state.get("journey_editor_journey_name", "").strip(),
        "module_domain": st.session_state.get("journey_editor_module_domain", "").strip(),
        "primary_user_role": ", ".join(st.session_state.get("journey_editor_user_roles", [])),
        "frequency": st.session_state.get("journey_editor_frequency", "").strip(),
        "complexity": st.session_state.get("journey_editor_complexity", "").strip(),
        "interview_date": st.session_state.get("journey_editor_interview_date").isoformat()
        if st.session_state.get("journey_editor_interview_date")
        else "",
        "interviewer": st.session_state.get("journey_editor_interviewer", "").strip(),
        "scrum_team": "",
    }
    errors: list[str] = []
    steps: list[dict[str, Any]] = []
    step_tables: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []

    if not journey["journey_id"]:
        errors.append("Journey ID is required.")
    if not journey["journey_name"]:
        errors.append("Journey Name is required.")
    if not journey["module_domain"]:
        errors.append("Module/Domain is required.")

    catalog_lookup = {table["table_name"].casefold(): table for table in catalog_tables.values() if table.get("table_name")}
    editor_steps = st.session_state.get("journey_editor_steps", [])
    seen_step_numbers: set[int] = set()

    for idx, _ in enumerate(editor_steps):
        prefix = f"journey_step_{idx}"
        step_number = int(st.session_state.get(f"{prefix}_step_number", idx + 1))
        user_action = st.session_state.get(f"{prefix}_user_action", "").strip()
        if step_number in seen_step_numbers:
            errors.append(f"Step number {step_number} is duplicated.")
        seen_step_numbers.add(step_number)
        if not user_action:
            errors.append(f"Step {step_number}: User Action is required.")

        transition_rows = st.session_state.get(f"{prefix}_transitions_data")
        if isinstance(transition_rows, pd.DataFrame):
            transition_dicts = transition_rows.fillna("").to_dict(orient="records")
        else:
            transition_dicts = pd.DataFrame(transition_rows).fillna("").to_dict(orient="records") if transition_rows is not None else []
        cleaned_transitions = []
        for row in transition_dicts:
            row = {key: str(value).strip() for key, value in row.items()}
            if not any(row.values()):
                continue
            if not row.get("to_state") or not row.get("trigger_action"):
                errors.append(f"Step {step_number}: each transition requires To State and Trigger Action.")
                continue
            cleaned_row = {
                **row,
                "journey_id": journey["journey_id"],
                "step_number": step_number,
            }
            cleaned_transitions.append(cleaned_row)
            transitions.append(cleaned_row)

        steps.append(
            {
                "step_number": step_number,
                "user_action": user_action,
                "screen_component": st.session_state.get(f"{prefix}_screen_component", "").strip(),
                "status_field_changes": format_transition_summary(cleaned_transitions),
                "validation_rules": st.session_state.get(f"{prefix}_validation_rules", "").strip(),
                "business_rules": st.session_state.get(f"{prefix}_business_rules", "").strip(),
                "notes": st.session_state.get(f"{prefix}_notes", "").strip(),
            }
        )

        read_tables = st.session_state.get(f"{prefix}_tables_read_known", []) + normalize_free_text_tables(
            st.session_state.get(f"{prefix}_tables_read_extra", "")
        )
        write_tables = st.session_state.get(f"{prefix}_tables_written_known", []) + normalize_free_text_tables(
            st.session_state.get(f"{prefix}_tables_written_extra", "")
        )
        for table_name in read_tables:
            catalog_match = catalog_lookup.get(table_name.casefold())
            step_tables.append(
                {
                    "journey_id": journey["journey_id"],
                    "step_number": step_number,
                    "table_name": table_name,
                    "access_mode": "READ",
                    "write_operation": None,
                    "is_catalog_table": bool(catalog_match),
                    "catalog_table_key": catalog_match.get("table_key") if catalog_match else None,
                }
            )
        for table_name in write_tables:
            op_key = f"{prefix}_write_op_{table_name}"
            operation = st.session_state.get(op_key, "UPDATE")
            catalog_match = catalog_lookup.get(table_name.casefold())
            step_tables.append(
                {
                    "journey_id": journey["journey_id"],
                    "step_number": step_number,
                    "table_name": table_name,
                    "access_mode": "WRITE",
                    "write_operation": operation,
                    "is_catalog_table": bool(catalog_match),
                    "catalog_table_key": catalog_match.get("table_key") if catalog_match else None,
                }
            )
    if not steps:
        errors.append("At least one step is required.")
    return journey, sorted(steps, key=lambda item: item["step_number"]), step_tables, transitions, errors


def _build_graphviz_network(edges: list[tuple[str, str, str]]) -> str:
    lines = ["graph tables {", '  rankdir="LR";', '  node [shape=box, style="rounded,filled", fillcolor="#f7f7f7"];']
    seen_nodes: set[str] = set()
    for left, right, journey_id in edges:
        seen_nodes.add(left)
        seen_nodes.add(right)
        lines.append(f'  "{left}" -- "{right}" [label="{journey_id}"];')
    for node in seen_nodes:
        lines.append(f'  "{node}";')
    lines.append("}")
    return "\n".join(lines)


def _build_state_machine_dot(transitions: list[dict[str, Any]]) -> str:
    lines = ["digraph state_machine {", '  rankdir="LR";', '  node [shape=ellipse, style=filled, fillcolor="#eef5ff"];']
    states: set[str] = set()
    for transition in transitions:
        from_state = transition.get("from_state") or "START"
        to_state = transition.get("to_state") or "UNKNOWN"
        states.add(from_state)
        states.add(to_state)
        label = transition.get("trigger_action", "")
        role = transition.get("user_role_required", "")
        if role:
            label = f"{label}\\n({role})"
        lines.append(f'  "{from_state}" -> "{to_state}" [label="{label}"];')
    for state in states:
        lines.append(f'  "{state}";')
    lines.append("}")
    return "\n".join(lines)


def _render_capture_page(store: JourneysStore, catalog_tables: dict[str, dict], existing_journeys: list[dict[str, Any]], actor_name: str) -> None:
    _ensure_editor_state(existing_journeys)
    st.subheader("Capture Journey")
    meta_left, meta_right = st.columns(2)
    with meta_left:
        st.text_input("Journey ID", key="journey_editor_journey_id")
        st.text_input("Journey Name", key="journey_editor_journey_name")
        st.selectbox("Module/Domain", JOURNEY_MODULE_OPTIONS, key="journey_editor_module_domain")
        st.multiselect("User Role", JOURNEY_ROLE_OPTIONS, key="journey_editor_user_roles")
    with meta_right:
        st.selectbox("Frequency", JOURNEY_FREQUENCY_OPTIONS, key="journey_editor_frequency")
        st.selectbox("Complexity", JOURNEY_COMPLEXITY_OPTIONS, key="journey_editor_complexity")
        st.date_input("Interview Date", key="journey_editor_interview_date")
        st.text_input("Interviewer", key="journey_editor_interviewer")

    st.divider()
    st.markdown("### Journey Steps")
    catalog_names = _catalog_names(catalog_tables)
    steps = st.session_state.get("journey_editor_steps", [])
    for idx, step in enumerate(steps):
        _seed_step_widget_defaults(step, idx)
        prefix = f"journey_step_{idx}"
        header = st.session_state.get(f"{prefix}_user_action") or f"Step {idx + 1}"
        with st.expander(f"Step {idx + 1}: {header}", expanded=idx == len(steps) - 1):
            row_cols = st.columns([1, 3, 3])
            row_cols[0].number_input("Step #", min_value=1, step=1, key=f"{prefix}_step_number")
            row_cols[1].text_input("User Action", key=f"{prefix}_user_action")
            row_cols[2].text_input("Screen/Component", key=f"{prefix}_screen_component")

            read_left, read_right = st.columns(2)
            read_left.multiselect(
                "Tables Read",
                options=catalog_names,
                key=f"{prefix}_tables_read_known",
            )
            read_right.text_input(
                "Additional Tables Read (comma-separated)",
                key=f"{prefix}_tables_read_extra",
            )

            write_left, write_right = st.columns(2)
            write_left.multiselect(
                "Tables Written",
                options=catalog_names,
                key=f"{prefix}_tables_written_known",
            )
            write_right.text_input(
                "Additional Tables Written (comma-separated)",
                key=f"{prefix}_tables_written_extra",
            )

            selected_write_tables = st.session_state.get(f"{prefix}_tables_written_known", []) + normalize_free_text_tables(
                st.session_state.get(f"{prefix}_tables_written_extra", "")
            )
            if selected_write_tables:
                st.caption("Write operations")
                op_cols = st.columns(2)
                for op_idx, table_name in enumerate(selected_write_tables):
                    with op_cols[op_idx % 2]:
                        st.selectbox(
                            f"{table_name} operation",
                            JOURNEY_WRITE_OPERATIONS,
                            key=f"{prefix}_write_op_{table_name}",
                        )

            st.text_area("Validation Rules Applied", key=f"{prefix}_validation_rules")
            st.text_area("Business Rules/Logic", key=f"{prefix}_business_rules")
            st.text_area("Notes", key=f"{prefix}_notes")
            st.markdown("**Status transitions**")
            transitions_df = st.session_state.get(f"{prefix}_transitions_data")
            if not isinstance(transitions_df, pd.DataFrame):
                transitions_df = pd.DataFrame(transitions_df)
            updated_df = st.data_editor(
                transitions_df,
                num_rows="dynamic",
                hide_index=True,
                use_container_width=True,
                key=f"{prefix}_transitions_editor",
            )
            st.session_state[f"{prefix}_transitions_data"] = updated_df
            if st.button("Remove step", key=f"{prefix}_remove"):
                steps.pop(idx)
                for new_idx, item in enumerate(steps, start=1):
                    item["step_number"] = new_idx
                if not steps:
                    steps.append(_blank_step(1))
                st.session_state["journey_editor_steps"] = steps
                st.rerun()

    action_cols = st.columns([1, 1, 2])
    if action_cols[0].button("Add Step", use_container_width=True):
        steps.append(_blank_step(len(steps) + 1))
        st.session_state["journey_editor_steps"] = steps
        st.rerun()
    if action_cols[1].button("Reset", use_container_width=True):
        _reset_editor(existing_journeys)
        st.rerun()
    if action_cols[2].button("Save Journey", type="primary", use_container_width=True):
        journey, payload_steps, step_tables, transitions, errors = _collect_editor_payload(catalog_tables)
        existing_ids = {row["journey_id"] for row in existing_journeys}
        loaded_id = st.session_state.get("journey_editor_loaded_id")
        if journey["journey_id"] in existing_ids and journey["journey_id"] != loaded_id:
            errors.append(f"Journey ID {journey['journey_id']} already exists.")
        if errors:
            for error in errors:
                st.error(error)
        else:
            store.save_journey(journey, payload_steps, step_tables, transitions, actor_name)
            st.success(f"Journey {journey['journey_id']} saved.")
            _reset_editor(store.fetch_journeys())
            st.rerun()


def _render_view_page(store: JourneysStore, existing_journeys: list[dict[str, Any]]) -> None:
    st.subheader("View Journeys")
    if not existing_journeys:
        st.info("No journeys have been captured yet.")
        return

    journeys_df = pd.DataFrame(existing_journeys)
    col1, col2, col3 = st.columns(3)
    module_filter = col1.multiselect("Module/Domain", sorted(journeys_df["module_domain"].dropna().unique().tolist()))
    role_options = sorted(
        {
            role.strip()
            for value in journeys_df["primary_user_role"].dropna().tolist()
            for role in str(value).split(",")
            if role.strip()
        }
    )
    role_filter = col2.multiselect("User Role", role_options)
    complexity_filter = col3.multiselect("Complexity", sorted(journeys_df["complexity"].dropna().unique().tolist()))

    filtered_df = journeys_df.copy()
    if module_filter:
        filtered_df = filtered_df[filtered_df["module_domain"].isin(module_filter)]
    if role_filter:
        filtered_df = filtered_df[
            filtered_df["primary_user_role"].fillna("").apply(
                lambda value: any(role in [item.strip() for item in str(value).split(",") if item.strip()] for role in role_filter)
            )
        ]
    if complexity_filter:
        filtered_df = filtered_df[filtered_df["complexity"].isin(complexity_filter)]

    st.dataframe(
        filtered_df[
            [
                "journey_id",
                "journey_name",
                "module_domain",
                "primary_user_role",
                "frequency",
                "complexity",
                "total_steps",
                "interview_date",
                "scrum_team",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    journey_lookup = {journey["journey_id"]: journey for journey in existing_journeys}
    selected_journey_id = st.selectbox("Select Journey", filtered_df["journey_id"].tolist())
    selected_journey = store.fetch_journey(selected_journey_id)
    if not selected_journey:
        return
    st.markdown(f"### {selected_journey['journey_name']}")
    for step in selected_journey.get("steps", []):
        with st.expander(f"Step {step['step_number']}: {step.get('user_action', '')}", expanded=False):
            st.write(f"**Screen/Component:** {step.get('screen_component', '') or 'N/A'}")
            read_tables = [ref["table_name"] for ref in step.get("table_refs", []) if ref.get("access_mode") == "READ"]
            write_tables = [
                f"{ref['table_name']} ({ref.get('write_operation', 'UPDATE')})"
                for ref in step.get("table_refs", [])
                if ref.get("access_mode") == "WRITE"
            ]
            st.write(f"**Tables Read:** {', '.join(read_tables) if read_tables else 'None'}")
            st.write(f"**Tables Written:** {', '.join(write_tables) if write_tables else 'None'}")
            st.write(f"**Status Field Changes:** {step.get('status_field_changes') or 'None'}")
            st.write(f"**Validation Rules Applied:** {step.get('validation_rules') or 'None'}")
            st.write(f"**Business Rules/Logic:** {step.get('business_rules') or 'None'}")
            st.write(f"**Notes:** {step.get('notes') or 'None'}")

    action_cols = st.columns([1, 1, 2])
    if action_cols[0].button("Load into Capture", use_container_width=True):
        _load_editor(selected_journey, existing_journeys)
        st.success("Journey loaded into Capture Journey.")
    confirm_delete = action_cols[1].checkbox("Confirm delete", key=f"confirm_delete_{selected_journey_id}")
    if action_cols[2].button("Delete Journey", use_container_width=True):
        if not confirm_delete:
            st.error("Confirm delete before removing a journey.")
        else:
            store.delete_journey(selected_journey_id)
            st.success(f"Journey {selected_journey_id} deleted.")
            st.rerun()


def _render_analysis_page(store: JourneysStore, catalog_tables: dict[str, dict], existing_journeys: list[dict[str, Any]]) -> None:
    st.subheader("Table Analysis")
    if not existing_journeys:
        st.info("No journeys are available for analysis yet.")
        return

    selected_ids = st.multiselect(
        "Select journeys to analyze",
        options=[journey["journey_id"] for journey in existing_journeys],
        default=[journey["journey_id"] for journey in existing_journeys],
    )
    if not selected_ids:
        st.caption("Select at least one journey.")
        return

    annotations = st.session_state.get("journey_editor_analysis_annotations", {})
    analysis_rows = store.fetch_journey_analysis(selected_ids, catalog_tables, annotations)
    if not analysis_rows:
        st.info("No table activity found for the selected journeys.")
        return
    analysis_df = pd.DataFrame(analysis_rows)
    editable_df = st.data_editor(
        analysis_df[
            [
                "table_name",
                "domain",
                "journey_ids",
                "read_count",
                "write_count",
                "access_pattern",
                "centrality_score",
                "legacy_table_type",
                "target_entity_proposed",
                "migration_priority",
            ]
        ],
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key="journey_analysis_editor",
    )
    st.session_state["journey_editor_analysis_annotations"] = {
        row["table_name"]: {
            "legacy_table_type": row.get("legacy_table_type", ""),
            "target_entity_proposed": row.get("target_entity_proposed", ""),
            "migration_priority": row.get("migration_priority", ""),
        }
        for row in editable_df.fillna("").to_dict(orient="records")
    }
    uncataloged = editable_df[~editable_df["table_name"].isin(_catalog_names(catalog_tables))]
    if not uncataloged.empty:
        st.warning("Some tables are uncataloged and were captured as free text.")

    edges = store.build_table_network(selected_ids)
    if edges:
        st.markdown("### Table Dependency Network")
        st.graphviz_chart(_build_graphviz_network(edges), use_container_width=True)


def _render_state_machine_page(store: JourneysStore) -> None:
    st.subheader("State Machines")
    transitions = store.fetch_state_transitions()
    if not transitions:
        st.info("No state transitions have been captured yet.")
        return
    entities = sorted({transition.get("entity_table", "") for transition in transitions if transition.get("entity_table")})
    selected_entity = st.selectbox("Select Entity/Table", entities)
    filtered = [transition for transition in transitions if transition.get("entity_table") == selected_entity]
    st.graphviz_chart(_build_state_machine_dot(filtered), use_container_width=True)
    transition_df = pd.DataFrame(filtered)
    st.dataframe(transition_df, use_container_width=True, hide_index=True)

    missing_roles = transition_df[transition_df["user_role_required"].fillna("") == ""]
    if not missing_roles.empty:
        st.warning("Some transitions are missing a user role assignment.")

    outbound_counts: dict[str, int] = defaultdict(int)
    inbound_counts: dict[str, int] = defaultdict(int)
    for transition in filtered:
        outbound_counts[transition.get("from_state") or "START"] += 1
        inbound_counts[transition.get("to_state") or ""] += 1
    dead_ends = [state for state in inbound_counts if outbound_counts.get(state, 0) == 0]
    if dead_ends:
        st.info("Dead-end states detected: " + ", ".join(sorted(dead_ends)))

    json_payload = build_state_machine_json(selected_entity, filtered)
    st.download_button(
        "Download State Machine JSON",
        data=json_payload,
        file_name=f"{selected_entity.lower()}_state_machine.json",
        mime="application/json",
    )


def _render_export_page(
    store: JourneysStore,
    catalog_tables: dict[str, dict],
    existing_journeys: list[dict[str, Any]],
    actor_name: str,
) -> None:
    st.subheader("Export Data")
    selected_ids = st.multiselect(
        "Select journeys for export",
        options=[journey["journey_id"] for journey in existing_journeys],
        default=[journey["journey_id"] for journey in existing_journeys],
    )
    if selected_ids:
        selected_journeys = [journey for journey in existing_journeys if journey["journey_id"] in selected_ids]
        steps_by_journey = {journey_id: store.fetch_journey_steps(journey_id) for journey_id in selected_ids}
        analysis_rows = store.fetch_journey_analysis(
            selected_ids,
            catalog_tables,
            st.session_state.get("journey_editor_analysis_annotations", {}),
        )
        transitions = [
            transition
            for journey_id in selected_ids
            for step in steps_by_journey[journey_id]
            for transition in step.get("transitions", [])
        ]
        workbook = build_journey_workbook(selected_journeys, steps_by_journey, analysis_rows, transitions)
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        export_path = EXPORT_DIR / "user_journey_export.xlsx"
        export_path.write_bytes(workbook)
        st.download_button(
            "Download Excel Workbook",
            data=workbook,
            file_name="User_Journey_Capture_Template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.caption(f"Local export path: `{export_path}`")
        csv_payload = pd.DataFrame(analysis_rows).to_csv(index=False).encode("utf-8")
        st.download_button("Download Table Cross-Reference CSV", data=csv_payload, file_name="table_cross_reference.csv", mime="text/csv")

    uploaded_file = st.file_uploader("Import existing journey workbook", type=["xlsx"])
    if uploaded_file is not None and st.button("Import Workbook", use_container_width=True):
        imported_count = store.import_journey_workbook(uploaded_file.getvalue(), actor_name, catalog_tables)
        st.success(f"Imported {imported_count} journey records from workbook.")


def render_journey_mapping(catalog_tables: dict[str, dict], actor_name: str) -> None:
    store = _journey_store()
    if not store:
        return
    existing_journeys = store.fetch_journeys()
    _ensure_editor_state(existing_journeys)

    tab_capture, tab_view, tab_analysis, tab_state, tab_export = st.tabs(
        ["Capture Journey", "View Journeys", "Table Analysis", "State Machines", "Export Data"]
    )
    with tab_capture:
        _render_capture_page(store, catalog_tables, existing_journeys, actor_name)
    with tab_view:
        _render_view_page(store, existing_journeys)
    with tab_analysis:
        _render_analysis_page(store, catalog_tables, existing_journeys)
    with tab_state:
        _render_state_machine_page(store)
    with tab_export:
        _render_export_page(store, catalog_tables, existing_journeys, actor_name)
