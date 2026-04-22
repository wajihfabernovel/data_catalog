"""Form components for per-table metadata entry."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.helpers import (
    CHOICES_YES_NO,
    CHOICES_YES_NO_UNSURE,
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


def render_schema_section(table: dict) -> None:
    st.markdown("#### Schema")
    schema_df = pd.DataFrame(table["schema"])
    if schema_df.empty:
        st.info("No schema columns were parsed for this table.")
        return
    st.dataframe(schema_df, use_container_width=True, hide_index=True)


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
        columns=[
            "fk_column",
            "references_table",
            "references_column",
            "cardinality",
            "mandatory",
        ],
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
            "mandatory": st.column_config.SelectboxColumn(
                "Mandatory?",
                options=["YES", "NO", "UNKNOWN"],
            ),
        },
    )

    st.caption("Tables that reference this table")
    referenced_by_df = pd.DataFrame(
        defaults.get("referenced_by", []),
        columns=[
            "table_name",
            "via_column",
            "cardinality",
        ],
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

    if not defaults["date_approved"] and date_approved is not None and status != "APPROVED":
        pass

    return {
        "completed_by": completed_by,
        "reviewed_by": reviewed_by,
        "reviewed_by_business": reviewed_by_business,
        "status": status,
        "date_approved": date_approved,
        "notes": notes,
    }


def render_table_forms(table: dict) -> dict:
    updated = dict(table)
    updated["owning_team"] = render_table_context_section(table)
    render_schema_section(table)
    updated["relationships"] = render_relationships_section(table)
    updated["data_quality"] = render_data_quality_section(table)
    updated["pipeline"] = render_pipeline_section(table)
    updated["target_model"] = render_target_model_section(table)
    updated["signoff"] = render_signoff_section(table)
    return updated
