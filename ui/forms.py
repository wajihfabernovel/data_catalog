"""Form components for per-table metadata entry."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.helpers import (
    CHOICES_YES_NO,
    CHOICES_YES_NO_UNSURE,
    COLUMN_CATEGORY_LABEL,
    QUALITY_RATINGS,
    SIGNOFF_STATUS,
    TEAM_OPTIONS,
    TARGET_RECOMMENDATIONS,
    WRITE_PATHS,
    widget_key,
)


def _select_index(options: list[str], value: str) -> int:
    if value in options:
        return options.index(value)
    return 0


# ---------------------------------------------------------------------------
# Schema editor (enriched when API data is present)
# ---------------------------------------------------------------------------

_BASE_COLS = ["column_name", "edm_type", "sql_type"]
_API_COLS = ["attribute_type", "source_type", "column_category", "lookup_target"]


def render_schema_section(table: dict) -> list[dict]:
    st.markdown("#### Schema")
    existing_schema = table.get("schema", [])
    has_api_data = any(col.get("attribute_type") for col in existing_schema)

    schema_df = pd.DataFrame(existing_schema) if existing_schema else pd.DataFrame()

    if schema_df.empty:
        cols = _BASE_COLS + (_API_COLS if has_api_data else [])
        schema_df = pd.DataFrame(columns=cols)
    else:
        for c in _BASE_COLS:
            if c not in schema_df.columns:
                schema_df[c] = ""
        if has_api_data:
            for c in _API_COLS:
                if c not in schema_df.columns:
                    schema_df[c] = ""

    # Build column config and disabled list
    col_config: dict = {
        "column_name": st.column_config.TextColumn("Column name"),
        "edm_type": st.column_config.TextColumn("Edm type"),
        "sql_type": st.column_config.TextColumn("SQL type"),
    }
    disabled_cols: list[str] = []

    if has_api_data:
        col_config.update(
            {
                "attribute_type": st.column_config.TextColumn("Attr type", disabled=True),
                "source_type": st.column_config.TextColumn("Source", disabled=True),
                "column_category": st.column_config.TextColumn("Category", disabled=True),
                "lookup_target": st.column_config.TextColumn("FK target", disabled=True),
            }
        )
        disabled_cols = _API_COLS
        display_order = [c for c in _BASE_COLS + _API_COLS if c in schema_df.columns]
        schema_df = schema_df[display_order]
    else:
        schema_df = schema_df[[c for c in _BASE_COLS if c in schema_df.columns]]

    edited_schema = st.data_editor(
        schema_df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key=widget_key(table["table_key"], "schema"),
        column_config=col_config,
        disabled=disabled_cols,
    )

    cleaned_rows = edited_schema.dropna(how="all").to_dict(orient="records")

    if existing_schema and not cleaned_rows:
        return existing_schema

    if has_api_data:
        # Re-merge editable changes with original rows to preserve all hidden fields
        original_by_name = {col["column_name"]: col for col in existing_schema}
        merged: list[dict] = []
        for row in cleaned_rows:
            name = row.get("column_name", "")
            if name and name in original_by_name:
                combined = dict(original_by_name[name])
                combined.update({k: v for k, v in row.items() if k in _BASE_COLS})
                merged.append(combined)
            else:
                merged.append(row)
        return merged

    return cleaned_rows


# ---------------------------------------------------------------------------
# Dataverse attribute analysis (shown when dataverse_meta is populated)
# ---------------------------------------------------------------------------

_STAT_LABELS = [
    ("total", "Total attrs"),
    ("business", "Business"),
    ("system", "System"),
    ("shadow", "Shadow"),
    ("rollup", "Rollup"),
    ("formula", "Formula"),
    ("lookup", "FK / Lookup"),
]


def render_dataverse_analysis_section(table: dict) -> None:
    """Read-only panel that shows enriched API metadata when available."""
    meta = table.get("dataverse_meta") or {}
    if not meta:
        return

    st.markdown("#### Dataverse Attribute Analysis")
    fetched_at = meta.get("fetched_at", "")
    if fetched_at:
        st.caption(f"Fetched at: {fetched_at[:19].replace('T', ' ')} UTC")

    stats = meta.get("stats", {})
    if stats:
        stat_cols = st.columns(len(_STAT_LABELS))
        for col, (key, label) in zip(stat_cols, _STAT_LABELS):
            col.metric(label, stats.get(key, 0))

    # Column category breakdown from schema
    schema = table.get("schema", [])
    if schema and any(c.get("column_category") for c in schema):
        from collections import Counter
        cat_counts = Counter(c.get("column_category", "SYSTEM") for c in schema)
        breakdown_rows = [
            {"Category": COLUMN_CATEGORY_LABEL.get(cat, cat), "Count": cnt}
            for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1])
        ]
        df_breakdown = pd.DataFrame(breakdown_rows)
        bc1, bc2 = st.columns([1, 2])
        bc1.dataframe(df_breakdown, hide_index=True, use_container_width=True)

        # Identify FK columns from schema
        fk_cols = [c for c in schema if c.get("column_category") == "LOOKUP"]
        if fk_cols:
            bc2.markdown("**FK dependencies**")
            fk_df = pd.DataFrame(
                [
                    {
                        "column": c["column_name"],
                        "target_entity": c.get("lookup_target", ""),
                        "sql_type": c.get("sql_type", ""),
                    }
                    for c in fk_cols
                ]
            )
            bc2.dataframe(fk_df, hide_index=True, use_container_width=True)

    # Picklist / state machine option values
    pl_options = [p for p in meta.get("picklist_options", []) if p.get("options")]
    if pl_options:
        with st.expander("State-machine columns (picklist option values)", expanded=False):
            for pl in pl_options:
                opts_md = " · ".join(
                    f"`{o['value']}` {o['label']}"
                    for o in pl["options"]
                    if o.get("label")
                )
                st.markdown(f"- **`{pl['logical_name']}`**: {opts_md or '(no labels)'}")

    # Computed columns — remind user these go to dbt, not persisted
    computed = [
        c for c in schema
        if c.get("column_category") in ("ROLLUP", "FORMULA")
    ]
    if computed:
        with st.expander(
            f"Computed columns → dbt ({len(computed)} not persisted in Azure SQL)",
            expanded=False,
        ):
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "column": c["column_name"],
                            "category": c.get("column_category", ""),
                            "attribute_type": c.get("attribute_type", ""),
                        }
                        for c in computed
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )

    # Shadow columns — remind user these should be dropped
    shadow = [c for c in schema if c.get("column_category") == "SHADOW"]
    if shadow:
        with st.expander(f"Shadow columns to drop ({len(shadow)})", expanded=False):
            names = ", ".join(f"`{c['column_name']}`" for c in shadow[:30])
            if len(shadow) > 30:
                names += f" … and {len(shadow) - 30} more"
            st.caption(names)


# ---------------------------------------------------------------------------
# Existing Dataverse profile section (legacy metadata_profile dict)
# ---------------------------------------------------------------------------

def render_dataverse_profile_section(table: dict) -> dict:
    st.markdown("#### Dataverse Profile")
    profile = dict(table.get("metadata_profile", {}))
    if not profile:
        st.info("No legacy Dataverse profile loaded for this table. Use the API Discovery tab to fetch live attribute metadata.")
        return {}

    metric_cols = st.columns(4)
    metric_cols[0].metric("Attributes", profile.get("total_attributes", 0))
    metric_cols[1].metric(
        "Custom business",
        profile.get("custom_business_columns", 0),
    )
    metric_cols[2].metric(
        "Rollup + formula",
        int(profile.get("rollup_fields", 0)) + int(profile.get("formula_fields", 0)),
    )
    metric_cols[3].metric("Lookups", profile.get("lookup_columns", 0))

    st.text_input(
        "Recommended target entity",
        value=profile.get("recommended_target_entity", ""),
        key=widget_key(table["table_key"], "profile_recommended_target_entity"),
    )
    st.text_input(
        "Migration priority",
        value=profile.get("migration_priority", ""),
        key=widget_key(table["table_key"], "profile_migration_priority"),
    )
    st.text_area(
        "State machine candidates",
        value=profile.get("state_machine_candidates", ""),
        key=widget_key(table["table_key"], "profile_state_machine_candidates"),
    )
    st.text_area(
        "Profile notes",
        value=profile.get("notes", ""),
        key=widget_key(table["table_key"], "profile_notes"),
    )

    profile["recommended_target_entity"] = st.session_state.get(
        widget_key(table["table_key"], "profile_recommended_target_entity"),
        profile.get("recommended_target_entity", ""),
    )
    profile["migration_priority"] = st.session_state.get(
        widget_key(table["table_key"], "profile_migration_priority"),
        profile.get("migration_priority", ""),
    )
    profile["state_machine_candidates"] = st.session_state.get(
        widget_key(table["table_key"], "profile_state_machine_candidates"),
        profile.get("state_machine_candidates", ""),
    )
    profile["notes"] = st.session_state.get(
        widget_key(table["table_key"], "profile_notes"),
        profile.get("notes", ""),
    )
    return profile


# ---------------------------------------------------------------------------
# Table context & relationships
# ---------------------------------------------------------------------------

def render_table_context_section(table: dict) -> str:
    st.markdown("#### Table Context")
    table_key = table["table_key"]
    return st.selectbox(
        "Owning team",
        TEAM_OPTIONS,
        index=_select_index(TEAM_OPTIONS, table.get("owning_team", TEAM_OPTIONS[0])),
        key=widget_key(table_key, "owning_team"),
    )


def render_relationships_section(table: dict) -> dict:
    st.markdown("#### Relationships")
    table_key = table["table_key"]
    defaults = table.get("relationships", {})

    st.caption("Foreign keys in this table")
    references_df = pd.DataFrame(
        defaults.get("references", []),
        columns=["fk_column", "references_table", "references_column", "cardinality", "mandatory"],
    )
    edited_references = st.data_editor(
        references_df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key=widget_key(table_key, "relationships_references"),
        column_config={
            "fk_column": "FK column in this table",
            "references_table": "References table",
            "references_column": "References column",
            "cardinality": "Cardinality",
            "mandatory": st.column_config.CheckboxColumn("Mandatory?"),
        },
    )

    st.caption("Tables that reference this table")
    referenced_by_df = pd.DataFrame(
        defaults.get("referenced_by", []),
        columns=["table_name", "via_column", "cardinality"],
    )
    edited_referenced_by = st.data_editor(
        referenced_by_df,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key=widget_key(table_key, "relationships_referenced_by"),
        column_config={
            "table_name": "Table name",
            "via_column": "Via column",
            "cardinality": "Cardinality",
        },
    )

    return {
        "references": edited_references.dropna(how="all").to_dict(orient="records"),
        "referenced_by": edited_referenced_by.dropna(how="all").to_dict(orient="records"),
    }


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

def render_data_quality_section(table: dict) -> dict:
    st.markdown("#### Data Quality Observations")
    table_key = table["table_key"]
    defaults = table["data_quality"]

    left, right = st.columns(2)
    nullable_issues = left.text_area(
        "Nullable issues",
        value=defaults["nullable_issues"],
        key=widget_key(table_key, "dq_nullable_issues"),
    )
    format_inconsistencies = right.text_area(
        "Format inconsistencies",
        value=defaults["format_inconsistencies"],
        key=widget_key(table_key, "dq_format_inconsistencies"),
    )

    col1, col2, col3 = st.columns(3)
    duplicate_records = col1.selectbox(
        "Duplicate records",
        CHOICES_YES_NO,
        index=_select_index(CHOICES_YES_NO, defaults["duplicate_records"]),
        key=widget_key(table_key, "dq_duplicate_records"),
    )
    orphan_records = col2.selectbox(
        "Orphan records",
        CHOICES_YES_NO,
        index=_select_index(CHOICES_YES_NO, defaults["orphan_records"]),
        key=widget_key(table_key, "dq_orphan_records"),
    )
    hard_delete_in_use = col3.selectbox(
        "Hard delete in use",
        CHOICES_YES_NO,
        index=_select_index(CHOICES_YES_NO, defaults["hard_delete_in_use"]),
        key=widget_key(table_key, "dq_hard_delete_in_use"),
    )

    rating_col, _ = st.columns([1, 1])
    overall_quality_rating = rating_col.selectbox(
        "Overall quality rating",
        QUALITY_RATINGS,
        index=_select_index(QUALITY_RATINGS, defaults["overall_quality_rating"]),
        key=widget_key(table_key, "dq_overall_quality_rating"),
    )
    quality_notes = st.text_area(
        "Quality notes",
        value=defaults["quality_notes"],
        key=widget_key(table_key, "dq_quality_notes"),
    )

    return {
        "nullable_issues": nullable_issues,
        "format_inconsistencies": format_inconsistencies,
        "duplicate_records": duplicate_records,
        "orphan_records": orphan_records,
        "hard_delete_in_use": hard_delete_in_use,
        "overall_quality_rating": overall_quality_rating,
        "quality_notes": quality_notes,
    }


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def render_pipeline_section(table: dict) -> dict:
    st.markdown("#### Pipeline Relevance")
    table_key = table["table_key"]
    defaults = table["pipeline"]

    col1, col2 = st.columns(2)
    extract_by_pipeline = col1.selectbox(
        "Extract by pipeline?",
        CHOICES_YES_NO_UNSURE,
        index=_select_index(CHOICES_YES_NO_UNSURE, defaults["extract_by_pipeline"]),
        key=widget_key(table_key, "pipeline_extract_by_pipeline"),
    )
    feed_power_bi = col2.selectbox(
        "Feed Power BI?",
        CHOICES_YES_NO_UNSURE,
        index=_select_index(CHOICES_YES_NO_UNSURE, defaults["feed_power_bi"]),
        key=widget_key(table_key, "pipeline_feed_power_bi"),
    )

    col3, col4 = st.columns(2)
    delta_extraction_column = col3.text_input(
        "Delta extraction column",
        value=defaults["delta_extraction_column"],
        key=widget_key(table_key, "pipeline_delta_extraction_column"),
    )
    write_path = col4.selectbox(
        "Write path",
        WRITE_PATHS,
        index=_select_index(WRITE_PATHS, defaults["write_path"]),
        key=widget_key(table_key, "pipeline_write_path"),
    )

    key_metrics_or_dimensions = st.text_area(
        "Key metrics or dimensions",
        value=defaults["key_metrics_or_dimensions"],
        key=widget_key(table_key, "pipeline_key_metrics_or_dimensions"),
    )

    return {
        "extract_by_pipeline": extract_by_pipeline,
        "delta_extraction_column": delta_extraction_column,
        "feed_power_bi": feed_power_bi,
        "key_metrics_or_dimensions": key_metrics_or_dimensions,
        "write_path": write_path,
    }


# ---------------------------------------------------------------------------
# Target model
# ---------------------------------------------------------------------------

def render_target_model_section(table: dict) -> dict:
    st.markdown("#### Target Model Recommendation")
    table_key = table["table_key"]
    defaults = table["target_model"]

    recommendation = st.selectbox(
        "Recommendation",
        TARGET_RECOMMENDATIONS,
        index=_select_index(TARGET_RECOMMENDATIONS, defaults["recommendation"]),
        key=widget_key(table_key, "target_recommendation"),
    )

    left, right = st.columns(2)
    merge_with = left.text_input(
        "If merge - merge with",
        value=defaults["merge_with"],
        key=widget_key(table_key, "target_merge_with"),
    )
    split_into = right.text_input(
        "If split - split into",
        value=defaults["split_into"],
        key=widget_key(table_key, "target_split_into"),
    )
    replaced_by = st.text_input(
        "If retire - replaced by",
        value=defaults["replaced_by"],
        key=widget_key(table_key, "target_replaced_by"),
    )

    missing_columns = st.text_area(
        "Missing columns needed",
        value=defaults["missing_columns"],
        key=widget_key(table_key, "target_missing_columns"),
    )
    missing_constraints = st.text_area(
        "Missing constraints needed",
        value=defaults["missing_constraints"],
        key=widget_key(table_key, "target_missing_constraints"),
    )

    return {
        "recommendation": recommendation,
        "merge_with": merge_with,
        "split_into": split_into,
        "replaced_by": replaced_by,
        "missing_columns": missing_columns,
        "missing_constraints": missing_constraints,
    }


# ---------------------------------------------------------------------------
# Sign-off
# ---------------------------------------------------------------------------

def render_signoff_section(table: dict) -> dict:
    st.markdown("#### Sign Off")
    table_key = table["table_key"]
    defaults = table["signoff"]

    col1, col2, col3 = st.columns(3)
    completed_by = col1.text_input(
        "Completed by",
        value=defaults["completed_by"],
        key=widget_key(table_key, "signoff_completed_by"),
    )
    reviewed_by = col2.text_input(
        "Reviewed by",
        value=defaults["reviewed_by"],
        key=widget_key(table_key, "signoff_reviewed_by"),
    )
    reviewed_by_business = col3.text_input(
        "Reviewed by (business)",
        value=defaults["reviewed_by_business"],
        key=widget_key(table_key, "signoff_reviewed_by_business"),
    )

    status_col, date_col = st.columns(2)
    status = status_col.selectbox(
        "Status",
        SIGNOFF_STATUS,
        index=_select_index(SIGNOFF_STATUS, defaults["status"]),
        key=widget_key(table_key, "signoff_status"),
    )
    date_approved = date_col.date_input(
        "Date approved",
        value=defaults["date_approved"],
        key=widget_key(table_key, "signoff_date_approved"),
    )
    notes = st.text_area(
        "Notes",
        value=defaults["notes"],
        key=widget_key(table_key, "signoff_notes"),
    )

    return {
        "completed_by": completed_by,
        "reviewed_by": reviewed_by,
        "reviewed_by_business": reviewed_by_business,
        "status": status,
        "date_approved": date_approved,
        "notes": notes,
    }


# ---------------------------------------------------------------------------
# Composite renderer (called by cards)
# ---------------------------------------------------------------------------

def render_table_forms(table: dict) -> dict:
    updated = dict(table)
    updated["owning_team"] = render_table_context_section(table)
    render_dataverse_analysis_section(table)  # read-only, no return value
    updated["schema"] = render_schema_section(table)
    updated["relationships"] = render_relationships_section(table)
    updated["metadata_profile"] = render_dataverse_profile_section(table)
    updated["data_quality"] = render_data_quality_section(table)
    updated["pipeline"] = render_pipeline_section(table)
    updated["target_model"] = render_target_model_section(table)
    updated["signoff"] = render_signoff_section(table)
    return updated
