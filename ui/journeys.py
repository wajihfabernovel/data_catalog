"""Streamlit UI for the user journey mapping module."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from services.business_flows import (
    ALL_TEMPLATES_SCOPE,
    BUSINESS_FLOW_SECTIONS,
    EDITABLE_ROW_ID,
    EDITABLE_SCOPE_COLUMN,
    GLOBAL_TEMPLATES_SCOPE,
    SECTION_COLUMNS,
    available_template_scopes,
    build_initial_templates,
    filter_template_rows,
    load_template_draft,
    merge_editor_rows,
    save_template_draft,
    section_dataframe,
    section_editable_columns,
)


TEMPLATES_STATE_KEY = "business_flow_templates"


def _ensure_template_state() -> None:
    st.session_state.setdefault(TEMPLATES_STATE_KEY, build_initial_templates())


def _selected_section(default_section: str) -> str:
    if hasattr(st, "segmented_control"):
        return st.segmented_control(
            "Business flow section",
            BUSINESS_FLOW_SECTIONS,
            default=default_section,
            key="business_flow_section",
        )
    return st.radio(
        "Business flow section",
        BUSINESS_FLOW_SECTIONS,
        index=BUSINESS_FLOW_SECTIONS.index(default_section),
        horizontal=True,
        key="business_flow_section",
    )


def _scope_default_for_new_row(scope: str) -> str:
    if scope in {ALL_TEMPLATES_SCOPE, GLOBAL_TEMPLATES_SCOPE}:
        return ""
    return scope


def _editor_dataframe(rows: list[dict], section: str) -> pd.DataFrame:
    columns = [EDITABLE_ROW_ID, *section_editable_columns(section, include_delete=True)]
    return pd.DataFrame(
        [
            {
                EDITABLE_ROW_ID: row.get(EDITABLE_ROW_ID, ""),
                "Delete": False,
                **{column: row.get(column, "") for column in section_editable_columns(section)},
            }
            for row in rows
        ],
        columns=columns,
    )


def _render_template_editor(section: str, scope: str) -> pd.DataFrame:
    templates = st.session_state[TEMPLATES_STATE_KEY]
    section_rows = templates[section]
    visible_rows = filter_template_rows(section_rows, scope)
    visible_row_ids = {str(row.get(EDITABLE_ROW_ID, "")) for row in visible_rows if row.get(EDITABLE_ROW_ID)}
    editable_columns = section_editable_columns(section)

    st.subheader(f"{section} - {scope}")
    action_cols = st.columns([1, 1, 4])
    with action_cols[0]:
        add_row = st.button("Add row", key=f"add_business_flow_{section}")
    with action_cols[1]:
        reset_section = st.button("Reset section", key=f"reset_business_flow_{section}")

    if reset_section:
        templates[section] = build_initial_templates()[section]
        st.session_state[TEMPLATES_STATE_KEY] = templates
        st.rerun()

    if add_row:
        section_rows.append(
            {
                EDITABLE_ROW_ID: "",
                EDITABLE_SCOPE_COLUMN: _scope_default_for_new_row(scope),
                **{column: "" for column in SECTION_COLUMNS[section]},
            }
        )
        visible_rows = filter_template_rows(section_rows, scope)
        visible_row_ids = {str(row.get(EDITABLE_ROW_ID, "")) for row in visible_rows if row.get(EDITABLE_ROW_ID)}

    editor_df = _editor_dataframe(visible_rows, section)
    edited_df = st.data_editor(
        editor_df,
        hide_index=True,
        num_rows="dynamic",
        width="stretch",
        key=f"business_flow_editor_{section}_{scope}",
        column_order=section_editable_columns(section, include_delete=True),
        disabled=[EDITABLE_ROW_ID],
    )

    templates[section] = merge_editor_rows(
        current_rows=section_rows,
        visible_row_ids=visible_row_ids,
        edited_rows=edited_df,
        columns=editable_columns,
    )
    st.session_state[TEMPLATES_STATE_KEY] = templates

    if st.button("Delete checked rows", key=f"delete_business_flow_{section}_{scope}"):
        templates[section] = [
            row
            for row in templates[section]
            if row.get(EDITABLE_ROW_ID) not in {
                str(editor_row.get(EDITABLE_ROW_ID, ""))
                for editor_row in edited_df.to_dict("records")
                if bool(editor_row.get("Delete"))
            }
        ]
        st.session_state[TEMPLATES_STATE_KEY] = templates
        st.rerun()

    output_df = pd.DataFrame(
        [
            {column: row.get(column, "") for column in SECTION_COLUMNS[section]}
            for row in filter_template_rows(templates[section], scope)
        ],
        columns=SECTION_COLUMNS[section],
    )
    st.download_button(
        "Download section as CSV",
        output_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{scope.lower().replace('&', 'and').replace(' ', '_')}_{section.lower().replace(' ', '_')}.csv",
        mime="text/csv",
        key=f"download_{scope}_{section}",
    )
    return output_df


def render_journey_mapping(catalog_tables: dict[str, dict], actor_name: str | None = None) -> None:
    """Render editable user journey mapping templates."""
    _ = catalog_tables, actor_name
    _ensure_template_state()

    st.caption(
        "Maintain reusable journey-mapping templates. Leave Scrum Team blank for global rows, "
        "or fill it to scope a row to a specific scrum team."
    )

    selected_section = _selected_section(BUSINESS_FLOW_SECTIONS[0])
    save_col, load_col, spacer_col = st.columns([1, 1, 4])
    with save_col:
        if st.button("Save templates locally", key="save_business_flow_templates"):
            path = save_template_draft(st.session_state[TEMPLATES_STATE_KEY])
            st.success(f"Saved templates to {path}.")
    with load_col:
        if st.button("Load saved templates", key="load_business_flow_templates"):
            try:
                st.session_state[TEMPLATES_STATE_KEY] = load_template_draft()
                st.success("Loaded saved business flow templates.")
                st.rerun()
            except FileNotFoundError as exc:
                st.warning(str(exc))

    scopes = available_template_scopes(st.session_state[TEMPLATES_STATE_KEY])
    default_scope = "D&IG" if "D&IG" in scopes else ALL_TEMPLATES_SCOPE
    selected_scope = st.selectbox(
        "Scrum team / template scope",
        scopes,
        index=scopes.index(default_scope),
        key="business_flow_template_scope",
    )
    _render_template_editor(selected_section, selected_scope)


__all__ = [
    "BUSINESS_FLOW_SECTIONS",
    "SECTION_COLUMNS",
    "_ensure_template_state",
    "_render_template_editor",
    "render_journey_mapping",
]
