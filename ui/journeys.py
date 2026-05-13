"""Streamlit UI for the user journey mapping module."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from services.business_flows import (
    BUSINESS_FLOW_SECTIONS,
    SECTION_COLUMNS,
    available_scrum_teams,
    section_dataframe,
)


def _render_flow_table(df: pd.DataFrame, table_key: str) -> None:
    st.markdown(
        """
        <style>
        .business-flow-table-wrap {
            max-height: 720px;
            overflow: auto;
            border: 1px solid #d7e3ef;
            border-radius: 6px;
        }
        .business-flow-table {
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
            font-size: 0.92rem;
        }
        .business-flow-table th {
            position: sticky;
            top: 0;
            z-index: 1;
            background: #5b9bd5;
            color: white;
            font-weight: 700;
            text-align: left;
            padding: 0.55rem 0.65rem;
            border: 1px solid #4b8fca;
        }
        .business-flow-table td {
            vertical-align: top;
            padding: 0.5rem 0.65rem;
            border: 1px solid #62b5e5;
            white-space: pre-wrap;
            overflow-wrap: anywhere;
        }
        .business-flow-table tbody tr:nth-child(odd) td {
            background: #c6eafa;
        }
        .business-flow-table tbody tr:nth-child(even) td {
            background: #ffffff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    html = df.to_html(index=False, escape=True, classes=f"business-flow-table {table_key}")
    st.markdown(f'<div class="business-flow-table-wrap">{html}</div>', unsafe_allow_html=True)


def _render_selected_business_flow_section(scrum_team: str, section: str) -> pd.DataFrame:
    df = section_dataframe(scrum_team, section)
    st.subheader(f"{section} - {scrum_team}")
    _render_flow_table(df, f"business_flow_{scrum_team}_{section}".replace(" ", "_").lower())
    return df


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


def render_journey_mapping(catalog_tables: dict[str, dict], actor_name: str | None = None) -> None:
    """Render the workbook-style user journey mapping reference."""
    _ = catalog_tables, actor_name

    scrum_teams = available_scrum_teams()
    if not scrum_teams:
        st.info("No business flow workbook content is available yet.")
        return

    st.caption(
        "Review business flow scenarios, table roles, end-to-end paths, and modeling rules by scrum team."
    )

    selected_team = st.selectbox(
        "Scrum team",
        scrum_teams,
        index=0,
        key="business_flow_scrum_team",
    )
    selected_section = _selected_section(BUSINESS_FLOW_SECTIONS[0])
    df = _render_selected_business_flow_section(selected_team, selected_section)

    st.download_button(
        "Download section as CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{selected_team.lower().replace('&', 'and')}_{selected_section.lower().replace(' ', '_')}.csv",
        mime="text/csv",
        key=f"download_{selected_team}_{selected_section}",
    )


__all__ = [
    "BUSINESS_FLOW_SECTIONS",
    "SECTION_COLUMNS",
    "_render_selected_business_flow_section",
    "render_journey_mapping",
]
