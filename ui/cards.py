"""Card rendering helpers."""

from __future__ import annotations

import streamlit as st

from ui.forms import render_table_forms


_SIGNOFF_BADGE_CLASS = {
    "DRAFT": "badge-draft",
    "IN REVIEW": "badge-review",
    "APPROVED": "badge-approved",
}
_QUALITY_BADGE_CLASS = {
    "CLEAN": "badge-clean",
    "ACCEPTABLE": "badge-acceptable",
    "PROBLEMATIC": "badge-problematic",
}


def _badge(label: str, css_class: str) -> str:
    return f'<span class="status-badge {css_class}">{label}</span>'


def render_table_card(
    table: dict,
    on_save_sp=None,
    on_save_local=None,
    on_export=None,
) -> dict:
    signoff_status = table.get("signoff", {}).get("status", "DRAFT")
    quality_rating = (
        table.get("data_quality", {})
        .get("overall_quality_rating", "ACCEPTABLE")
    )
    signoff_badge = _badge(
        signoff_status,
        _SIGNOFF_BADGE_CLASS.get(signoff_status, "badge-draft"),
    )
    quality_badge = _badge(
        quality_rating,
        _QUALITY_BADGE_CLASS.get(quality_rating, "badge-acceptable"),
    )
    team_badge = _badge(table.get("owning_team", "D&IG"), "badge-team")

    with st.container(border=True):
        summary_cols = st.columns([3, 1, 1, 1])
        summary_cols[0].markdown(
            f"### {table['table_name']} {signoff_badge} {quality_badge} {team_badge}",
            unsafe_allow_html=True,
        )
        pk = table.get("primary_key") or "N/A"
        summary_cols[1].metric("Primary key", pk)
        summary_cols[2].metric("Columns", len(table.get("schema", [])))
        summary_cols[3].metric(
            "State fields",
            len(
                [
                    column
                    for column in table.get("schema", [])
                    if column.get("is_state_machine_candidate")
                ]
            ),
        )

        profile = table.get("metadata_profile", {})
        if profile:
            st.caption(
                " | ".join(
                    [
                        f"Custom business: {profile.get('custom_business_columns', 0)}",
                        f"Rollup: {profile.get('rollup_fields', 0)}",
                        f"Formula: {profile.get('formula_fields', 0)}",
                        f"Lookups: {profile.get('lookup_columns', 0)}",
                        f"Target: {profile.get('recommended_target_entity', '') or 'N/A'}",
                    ]
                )
            )

        with st.expander("Open metadata sections", expanded=False):
            updated = render_table_forms(table)

            has_actions = any([on_save_sp, on_save_local, on_export])
            if has_actions:
                st.divider()
                tkey = table["table_key"]
                c1, c2, c3 = st.columns(3)

                if on_save_sp and c1.button(
                    "Save to Supabase",
                    key=f"sp_{tkey}",
                    use_container_width=True,
                ):
                    on_save_sp(updated)

                if on_save_local and c2.button(
                    "Save locally",
                    key=f"local_{tkey}",
                    use_container_width=True,
                ):
                    on_save_local(updated)

                if on_export and c3.button(
                    "Export to Excel",
                    key=f"export_{tkey}",
                    use_container_width=True,
                ):
                    on_export(updated)

                dl_payload = st.session_state.get(f"table_export_{tkey}")
                if dl_payload:
                    st.download_button(
                        f"Download {table['table_name']}.xlsx",
                        data=dl_payload,
                        file_name=f"{table['table_name']}.xlsx",
                        mime=(
                            "application/vnd.openxmlformats-"
                            "officedocument.spreadsheetml.sheet"
                        ),
                        key=f"dl_{tkey}",
                    )

            return updated
    return table
