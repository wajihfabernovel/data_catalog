"""Main Streamlit entry point for the Dataverse data catalog tool."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from parser.xml_parser import parse_dataverse_xml
from services.dataverse_metadata import DataverseConfigError, DataverseMetadataClient, load_dataverse_config
from services.export import build_excel_workbook
from services.local_store import load_local_catalog_state, save_local_catalog_state
from services.supabase_store import SupabaseConfigError, SupabaseStore, load_supabase_config
from ui.api_discovery import render_api_discovery
from ui.cards import render_table_card
from ui.journeys import render_journey_mapping
from utils.helpers import build_default_table_state, merge_table_state, normalize_table_names

load_dotenv()

STYLE_PATH = Path(__file__).parent / "assets" / "styles.css"
EXPORT_DIR = Path(__file__).parent / "exports"
TABLE_PREFIX = "hive_"


def _prefix_filter(catalog: dict) -> dict:
    """Return only tables whose logical name starts with TABLE_PREFIX."""
    return {k: v for k, v in catalog.items() if v.get("table_name", "").startswith(TABLE_PREFIX)}


def load_styles() -> None:
    st.markdown(f"<style>{STYLE_PATH.read_text()}</style>", unsafe_allow_html=True)


def init_state() -> None:
    defaults = {
        "catalog_tables": {},
        "database_snapshot": {},
        "xml_payload": "",
        "table_names_raw": "",
        "search_query": "",
        "team_filters": [],
        "column_bucket_filters": [],
        "export_payload": None,
        "export_file_path": None,
        "batch_export_payload": None,
        "batch_export_path": None,
        "api_results": {},
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def get_supabase_store() -> SupabaseStore:
    return SupabaseStore()


def current_actor_name() -> str:
    return "supabase_app"


def get_dataverse_client() -> DataverseMetadataClient:
    return DataverseMetadataClient()


@st.cache_data(show_spinner=False)
def cached_parse(xml_payload: str, table_names_csv: str) -> list[dict]:
    requested_tables = normalize_table_names(table_names_csv)
    return parse_dataverse_xml(xml_payload, requested_tables)


def render_connection_section() -> None:
    config = load_supabase_config()
    if config["missing"]:
        st.info(
            "Supabase is not configured yet. Local-save mode is still available. Missing: "
            + ", ".join(config["missing"])
        )
        return
    st.success("Supabase backend configured.")
    st.caption(f"Project: `{config['url']}`")

    dv_config = load_dataverse_config()
    if dv_config["missing"]:
        st.info(
            "Dataverse metadata fetch is not configured yet. Missing: "
            + ", ".join(dv_config["missing"])
        )
    else:
        st.success("Dataverse metadata fetch configured.")
        st.caption(f"Base URL: `{dv_config['base_url']}`")


def refresh_from_database(show_message: bool = False) -> None:
    snapshot = get_supabase_store().fetch_catalog_state()
    st.session_state["database_snapshot"] = snapshot

    existing_tables = st.session_state.get("catalog_tables", {})
    for table_key, stored_table in snapshot.items():
        if table_key not in existing_tables:
            base_table = build_default_table_state(
                {
                    "table_key": stored_table["table_key"],
                    "table_name": stored_table["table_name"],
                    "primary_key": stored_table.get("primary_key", ""),
                    "schema": stored_table.get("schema", []),
                }
            )
            existing_tables[table_key] = merge_table_state(base_table, stored_table)
        else:
            existing_tables[table_key] = merge_table_state(existing_tables[table_key], stored_table)

    st.session_state["catalog_tables"] = existing_tables
    if show_message:
        st.success("Catalog data refreshed from Supabase.")


def _has_missing_schema(catalog_tables: dict) -> bool:
    return any(not table.get("schema") for table in catalog_tables.values())


def _column_bucket(column_count: int) -> str:
    if column_count == 0:
        return "0"
    if 1 <= column_count <= 10:
        return "1-10"
    if 11 <= column_count <= 50:
        return "11-50"
    return "50+"


def parse_and_sync() -> None:
    xml_payload = st.session_state.get("xml_payload", "")
    table_names_raw = st.session_state.get("table_names_raw", "")
    if not xml_payload.strip():
        st.error("Provide an XML payload or upload an XML file before parsing.")
        return
    if not normalize_table_names(table_names_raw):
        st.error("Provide at least one table name before parsing.")
        return

    parsed_tables = cached_parse(xml_payload, table_names_raw)
    if not parsed_tables:
        st.warning("No matching Dataverse EntityType nodes were found for the requested tables.")
        return

    snapshot = {}
    try:
        snapshot = get_supabase_store().fetch_catalog_state()
        st.session_state["database_snapshot"] = snapshot
    except (RuntimeError, SupabaseConfigError) as exc:
        st.info(f"Supabase sync skipped: {exc}")

    catalog_tables = st.session_state.get("catalog_tables", {})
    for parsed_table in parsed_tables:
        default_table = build_default_table_state(parsed_table)
        stored_table = snapshot.get(parsed_table["table_key"])
        catalog_tables[parsed_table["table_key"]] = merge_table_state(default_table, stored_table)

    st.session_state["catalog_tables"] = catalog_tables
    st.success(f"Loaded {len(parsed_tables)} table(s) into the catalog workspace.")


def fetch_dataverse_metadata_and_sync() -> None:
    requested_tables = normalize_table_names(st.session_state.get("table_names_raw", ""))
    if not requested_tables:
        requested_tables = [
            table["table_name"]
            for table in st.session_state.get("catalog_tables", {}).values()
            if table.get("table_name")
        ]
    if not requested_tables:
        st.error("Provide at least one table name or load catalog tables before fetching Dataverse metadata.")
        return

    requested_tables = [t for t in requested_tables if t.startswith(TABLE_PREFIX)]
    if not requested_tables:
        st.error(f"No tables matching prefix '{TABLE_PREFIX}' found.")
        return

    status = st.status("Fetching Dataverse metadata for selected tables...", expanded=True)
    try:
        status.write(f"Tables requested: {len(requested_tables)}")
        fetched_tables = get_dataverse_client().fetch_entities(requested_tables)
        status.write(f"Fetched metadata for {len(fetched_tables)} tables.")
    except (RuntimeError, DataverseConfigError, ValueError, OSError) as exc:
        status.update(label="Dataverse metadata fetch failed", state="error")
        st.error(str(exc))
        return
    except Exception as exc:  # noqa: BLE001
        status.update(label="Dataverse metadata fetch failed", state="error")
        st.error(f"Dataverse metadata fetch failed: {exc}")
        return

    snapshot = {}
    try:
        snapshot = get_supabase_store().fetch_catalog_state()
        st.session_state["database_snapshot"] = snapshot
    except (RuntimeError, SupabaseConfigError):
        snapshot = {}

    catalog_tables = st.session_state.get("catalog_tables", {})
    for fetched_table in fetched_tables:
        default_table = build_default_table_state(fetched_table)
        stored_table = snapshot.get(fetched_table["table_key"])
        merged = merge_table_state(default_table, stored_table)
        merged["metadata_profile"] = fetched_table.get("metadata_profile", merged.get("metadata_profile", {}))
        merged["relationships"] = fetched_table.get("relationships", merged.get("relationships", {}))
        merged["schema"] = fetched_table.get("schema", merged.get("schema", []))
        merged["primary_key"] = fetched_table.get("primary_key", merged.get("primary_key", ""))
        catalog_tables[fetched_table["table_key"]] = merged

    st.session_state["catalog_tables"] = catalog_tables
    status.update(label="Dataverse metadata fetch complete", state="complete")
    st.success(f"Fetched Dataverse metadata for {len(fetched_tables)} table(s).")


def fetch_all_custom_dataverse_tables_and_sync() -> None:
    status = st.status("Fetching all custom Dataverse tables...", expanded=True)
    try:
        status.write("Loading custom entity definitions with expanded attributes.")
        fetched_tables = get_dataverse_client().fetch_all_custom_entities(name_prefix=TABLE_PREFIX)
        status.write(f"Fetched and enriched {len(fetched_tables)} custom tables (prefix: '{TABLE_PREFIX}').")
    except (RuntimeError, DataverseConfigError, ValueError, OSError) as exc:
        status.update(label="Dataverse bulk metadata fetch failed", state="error")
        st.error(str(exc))
        return
    except Exception as exc:  # noqa: BLE001
        status.update(label="Dataverse bulk metadata fetch failed", state="error")
        st.error(f"Dataverse bulk metadata fetch failed: {exc}")
        return

    snapshot = {}
    try:
        snapshot = get_supabase_store().fetch_catalog_state()
        st.session_state["database_snapshot"] = snapshot
    except (RuntimeError, SupabaseConfigError):
        snapshot = {}

    catalog_tables = st.session_state.get("catalog_tables", {})
    for fetched_table in fetched_tables:
        default_table = build_default_table_state(fetched_table)
        stored_table = snapshot.get(fetched_table["table_key"])
        merged = merge_table_state(default_table, stored_table)
        merged["metadata_profile"] = fetched_table.get("metadata_profile", merged.get("metadata_profile", {}))
        merged["relationships"] = fetched_table.get("relationships", merged.get("relationships", {}))
        merged["schema"] = fetched_table.get("schema", merged.get("schema", []))
        merged["primary_key"] = fetched_table.get("primary_key", merged.get("primary_key", ""))
        catalog_tables[fetched_table["table_key"]] = merged

    st.session_state["catalog_tables"] = catalog_tables
    status.update(label="Dataverse bulk metadata fetch complete", state="complete")
    st.success(f"Fetched all custom Dataverse entities: {len(fetched_tables)} table(s).")


def render_input_section() -> None:
    st.caption(
        "Start here — paste or upload Dataverse XML metadata, enter table names, then **Parse and sync** "
        "to populate the workspace. Use **Fetch Dataverse metadata** to enrich selected tables via the "
        "Web API, or **Fetch all custom tables** to auto-discover your entire schema in one pass."
    )
    uploaded_file = st.file_uploader("Upload XML metadata", type=["xml"])
    if uploaded_file is not None:
        st.session_state["xml_payload"] = uploaded_file.getvalue().decode("utf-8")

    st.text_area(
        "XML payload",
        height=220,
        key="xml_payload",
        help="Paste Dataverse EntityDefinitions metadata XML here.",
    )
    st.text_input(
        "Table names (comma-separated)",
        key="table_names_raw",
        help="Example: account, contact, incident",
    )

    st.markdown('<p class="button-group-label">Parse &amp; Sync</p>', unsafe_allow_html=True)
    parse_cols = st.columns([1, 1, 1, 1])
    if parse_cols[0].button("Parse and sync", use_container_width=True):
        parse_and_sync()
    if parse_cols[1].button("Refresh from Supabase", use_container_width=True):
        try:
            refresh_from_database(show_message=True)
        except (RuntimeError, SupabaseConfigError) as exc:
            st.error(str(exc))
    if parse_cols[2].button("Fetch Dataverse metadata", use_container_width=True):
        fetch_dataverse_metadata_and_sync()
    if parse_cols[3].button("Fetch all custom Dataverse tables", use_container_width=True):
        fetch_all_custom_dataverse_tables_and_sync()

    st.markdown('<p class="button-group-label">Save &amp; Load</p>', unsafe_allow_html=True)
    save_cols = st.columns(4)
    if save_cols[0].button("Save to Supabase", use_container_width=True):
        tables = list(st.session_state.get("catalog_tables", {}).values())
        if not tables:
            st.warning("No catalog tables are loaded yet.")
        else:
            try:
                get_supabase_store().save_tables(tables, current_actor_name())
                refresh_from_database(show_message=False)
                st.success("Catalog data saved to Supabase.")
            except (RuntimeError, SupabaseConfigError) as exc:
                st.error(str(exc))
    if save_cols[1].button("Save locally", use_container_width=True):
        tables_map = st.session_state.get("catalog_tables", {})
        if not tables_map:
            st.warning("No catalog tables are loaded yet.")
        else:
            path = save_local_catalog_state(tables_map)
            st.success(f"Local draft saved to {path}.")
    if save_cols[2].button("Load local draft", use_container_width=True):
        try:
            st.session_state["catalog_tables"] = load_local_catalog_state()
            st.success("Local draft loaded.")
        except FileNotFoundError as exc:
            st.warning(str(exc))
    tables = list(st.session_state.get("catalog_tables", {}).values())
    if save_cols[3].button("Prepare export", use_container_width=True):
        if not tables:
            st.warning("No table data is available to export.")
        else:
            export_payload = build_excel_workbook(tables)
            EXPORT_DIR.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_path = EXPORT_DIR / f"dataverse_data_catalog_{timestamp}.xlsx"
            export_path.write_bytes(export_payload)
            st.session_state["export_payload"] = export_payload
            st.session_state["export_file_path"] = str(export_path)
            st.success(f"Excel workbook generated and saved to {export_path}.")


def render_export_section() -> None:
    export_payload = st.session_state.get("export_payload")
    export_file_path = st.session_state.get("export_file_path")
    tables = st.session_state.get("catalog_tables", {})
    if export_payload and tables:
        st.download_button(
            "Download Excel workbook",
            data=export_payload,
            file_name="dataverse_data_catalog.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        if export_file_path:
            st.caption(f"Fallback local export path: `{export_file_path}`")


def render_catalog_stats(catalog_tables: dict) -> None:
    tables = list(catalog_tables.values())
    total = len(tables)
    status_counts = {"DRAFT": 0, "IN REVIEW": 0, "APPROVED": 0}
    for t in tables:
        status = t.get("signoff", {}).get("status", "DRAFT")
        if status in status_counts:
            status_counts[status] += 1
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Tables", total)
    c2.metric("Draft", status_counts["DRAFT"])
    c3.metric("In Review", status_counts["IN REVIEW"])
    c4.metric("Approved", status_counts["APPROVED"])


def _commit_table(updated_table: dict) -> None:
    st.session_state["catalog_tables"][updated_table["table_key"]] = updated_table


def table_save_sp(updated_table: dict) -> None:
    _commit_table(updated_table)
    tables = list(st.session_state["catalog_tables"].values())
    try:
        get_supabase_store().save_tables(tables, current_actor_name())
        st.success(f"'{updated_table['table_name']}' saved to Supabase.")
    except (RuntimeError, SupabaseConfigError) as exc:
        st.error(str(exc))


def table_save_local(updated_table: dict) -> None:
    _commit_table(updated_table)
    path = save_local_catalog_state(st.session_state["catalog_tables"])
    st.success(f"'{updated_table['table_name']}' saved locally to {path}.")


def table_export(updated_table: dict) -> None:
    _commit_table(updated_table)
    payload = build_excel_workbook([updated_table])
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{updated_table['table_name']}_{ts}.xlsx"
    (EXPORT_DIR / fname).write_bytes(payload)
    st.session_state[f"table_export_{updated_table['table_key']}"] = payload
    st.success("Export ready — use the download button below.")


def render_catalog_section() -> None:
    catalog_tables = _prefix_filter(st.session_state.get("catalog_tables", {}))
    if not catalog_tables or _has_missing_schema(catalog_tables):
        try:
            refresh_from_database(show_message=False)
            catalog_tables = _prefix_filter(st.session_state.get("catalog_tables", {}))
        except (RuntimeError, SupabaseConfigError):
            catalog_tables = {}

    if not catalog_tables:
        st.info("No catalog data is loaded yet. Use Input & Sync to parse metadata or refresh from Supabase.")
        return

    st.caption(
        "Review and annotate every loaded table. Expand a card to edit signoff status, data quality, "
        "column-level notes, and team ownership. Save changes table-by-table to Supabase or export "
        "individual tables to Excel. Use the Batch tab for bulk operations."
    )
    render_catalog_stats(catalog_tables)
    render_export_section()

    action_cols = st.columns([1.2, 4])
    if action_cols[0].button("Save all to Supabase", key="catalog_save_all", use_container_width=True):
        try:
            get_supabase_store().save_tables(list(st.session_state["catalog_tables"].values()), current_actor_name())
            refresh_from_database(show_message=False)
            st.success("All loaded catalog tables saved to Supabase.")
        except (RuntimeError, SupabaseConfigError) as exc:
            st.error(str(exc))

    search_col, team_col, bucket_col = st.columns([2, 1.4, 1.1])
    search_col.text_input("Search tables", key="search_query", placeholder="Filter by table name...")
    team_col.multiselect(
        "Owning team",
        options=[
            "D&IG",
            "Strategy",
            "S&R",
            "Modular Innovation",
            "Analytics",
            "Integration & Localization",
        ],
        key="team_filters",
        placeholder="All teams",
    )
    bucket_col.multiselect(
        "Columns",
        options=["0", "1-10", "11-50", "50+"],
        key="column_bucket_filters",
        placeholder="All ranges",
    )
    search_query = st.session_state.get("search_query", "").strip().casefold()
    team_filters = set(st.session_state.get("team_filters", []))
    column_bucket_filters = set(st.session_state.get("column_bucket_filters", []))

    visible_tables = [
        table
        for table in catalog_tables.values()
        if (not search_query or search_query in table["table_name"].casefold())
        and (not team_filters or table.get("owning_team", "D&IG") in team_filters)
        and (
            not column_bucket_filters
            or _column_bucket(len(table.get("schema", []))) in column_bucket_filters
        )
    ]
    if not visible_tables:
        st.warning("No tables match the current search filter.")
        return

    for table in sorted(visible_tables, key=lambda item: item["table_name"].casefold()):
        updated_table = render_table_card(
            table,
            on_save_sp=table_save_sp,
            on_save_local=table_save_local,
            on_export=table_export,
        )
        st.session_state["catalog_tables"][table["table_key"]] = updated_table


def render_sidebar_help() -> None:
    with st.sidebar:
        st.markdown("**Supabase Connection**")
        render_connection_section()
        st.divider()

        st.markdown('<p class="sidebar-section-title">Configuration Steps</p>', unsafe_allow_html=True)
        steps = [
            ("1", "Local mode is available immediately via <em>Save locally</em> and <em>Load local draft</em>."),
            ("2", "Set <code>SUPABASE_URL</code> and <code>SUPABASE_SERVICE_ROLE_KEY</code>."),
            ("3", "Create the shared Supabase tables for catalog data and relationships."),
            ("4", "Use <em>Refresh from Supabase</em> to load teammates' latest edits."),
            ("5", "Keep local save as a fallback draft option."),
        ]
        for num, text in steps:
            st.markdown(
                f'<div class="step-item">'
                f'<div class="step-number">{num}</div>'
                f'<div class="step-text">{text}</div></div>',
                unsafe_allow_html=True,
            )

        st.caption(f"Today: {date.today().isoformat()}")


def render_batch_section() -> None:
    catalog_tables = _prefix_filter(st.session_state.get("catalog_tables", {}))
    if not catalog_tables:
        st.info("No tables loaded yet. Use the Input & Sync tab to parse metadata first.")
        return

    st.caption(
        "Run save or export actions across multiple tables in one shot. Select any subset of loaded "
        "tables, then bulk-save annotations to Supabase, dump a local draft, or generate a combined "
        "Excel workbook covering all selected tables."
    )

    table_names = sorted(
        catalog_tables.keys(), key=lambda k: catalog_tables[k]["table_name"].casefold()
    )
    display_names = {k: catalog_tables[k]["table_name"] for k in table_names}

    selected_keys = st.multiselect(
        "Select tables to act on",
        options=table_names,
        format_func=lambda k: display_names[k],
        placeholder="Choose one or more tables...",
        default=None,
    )
    if selected_keys and len(selected_keys) == len(table_names):
        st.caption("All tables selected.")

    if not selected_keys:
        st.caption("Select at least one table to enable batch actions.")
        return

    selected_tables = [catalog_tables[k] for k in selected_keys]
    st.caption(f"{len(selected_tables)} table(s) selected.")
    st.divider()

    st.markdown('<p class="button-group-label">Batch Save</p>', unsafe_allow_html=True)
    save_cols = st.columns([1, 1, 2])

    if save_cols[0].button("Save to Supabase", key="batch_save_sp", use_container_width=True):
        try:
            get_supabase_store().save_tables(selected_tables, current_actor_name())
            st.success(f"Saved {len(selected_tables)} table(s) to Supabase.")
        except (RuntimeError, SupabaseConfigError) as exc:
            st.error(str(exc))

    if save_cols[1].button("Save locally", key="batch_save_local", use_container_width=True):
        subset = {t["table_key"]: t for t in selected_tables}
        path = save_local_catalog_state(subset)
        st.success(f"Saved {len(selected_tables)} table(s) locally to {path}.")

    st.markdown('<p class="button-group-label">Batch Export</p>', unsafe_allow_html=True)
    export_cols = st.columns([1, 3])

    if export_cols[0].button("Prepare Excel export", key="batch_export", use_container_width=True):
        payload = build_excel_workbook(selected_tables)
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = EXPORT_DIR / f"batch_export_{timestamp}.xlsx"
        export_path.write_bytes(payload)
        st.session_state["batch_export_payload"] = payload
        st.session_state["batch_export_path"] = str(export_path)
        st.success(f"Excel workbook ready — {len(selected_tables)} table(s).")

    batch_payload = st.session_state.get("batch_export_payload")
    if batch_payload:
        st.download_button(
            "Download batch Excel workbook",
            data=batch_payload,
            file_name="batch_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )
        if st.session_state.get("batch_export_path"):
            st.caption(f"Local path: `{st.session_state['batch_export_path']}`")


def _relationship_dot(visible_tables: dict[str, dict]) -> str:
    lines = [
        "digraph relationships {",
        '  rankdir="LR";',
        '  node [shape=box, style="rounded,filled", fillcolor="#f7f7f7"];',
    ]
    edges: set[tuple[str, str, str]] = set()
    many_to_many_edges: set[tuple[str, str, str]] = set()
    for table_name, table in visible_tables.items():
        lines.append(f'  "{table_name}";')
        for ref in table.get("relationships", {}).get("references", []):
            target = str(ref.get("references_table", "")).strip()
            fk = str(ref.get("fk_column", "")).strip()
            if target and target in visible_tables and target != table_name:
                edges.add((table_name, target, fk))
        for rel in table.get("metadata_profile", {}).get("many_to_many", []):
            entity1 = str(rel.get("entity1", "")).strip()
            entity2 = str(rel.get("entity2", "")).strip()
            schema_name = str(rel.get("schema_name", "")).strip()
            if entity1 in visible_tables and entity2 in visible_tables and entity1 != entity2:
                ordered = tuple(sorted([entity1, entity2]))
                many_to_many_edges.add((ordered[0], ordered[1], schema_name))
    for source, target, fk in sorted(edges):
        label = fk or "FK"
        lines.append(f'  "{source}" -> "{target}" [label="{label}", color="#3b82f6"];')
    for entity1, entity2, schema_name in sorted(many_to_many_edges):
        label = schema_name or "M:N"
        lines.append(
            f'  "{entity1}" -> "{entity2}" '
            f'[dir=both, arrowhead=none, arrowtail=none, style=dashed, color="#ef4444", label="{label}"];'
        )
    lines.append("}")
    return "\n".join(lines)


def render_relationships_section() -> None:
    catalog_tables = _prefix_filter(st.session_state.get("catalog_tables", {}))
    if not catalog_tables:
        st.info("No catalog data is loaded yet. Fetch Dataverse metadata first.")
        return

    st.caption(
        "Visual FK dependency graph across your custom Dataverse entities — only custom-to-custom links "
        "are shown; system tables (e.g. systemuser, transactioncurrency) are excluded. Select a subset "
        "of tables to narrow the graph, then download the edge list as CSV for use in ERD tools."
    )
    st.markdown("### Relationships")
    table_names = sorted(catalog_tables.keys(), key=lambda k: catalog_tables[k]["table_name"].casefold())
    selected_keys = st.multiselect(
        "Select tables to visualize",
        options=table_names,
        format_func=lambda key: catalog_tables[key]["table_name"],
        default=table_names[: min(25, len(table_names))],
    )
    if not selected_keys:
        st.caption("Select at least one table.")
        return

    visible_tables = {
        catalog_tables[key]["table_name"]: catalog_tables[key]
        for key in selected_keys
    }
    st.graphviz_chart(_relationship_dot(visible_tables), use_container_width=True)

    relationship_rows = []
    many_to_many_rows = []
    for table in visible_tables.values():
        for ref in table.get("relationships", {}).get("references", []):
            target = str(ref.get("references_table", "")).strip()
            if target in visible_tables:
                relationship_rows.append(
                    {
                        "source_table": table["table_name"],
                        "source_column": ref.get("fk_column", ""),
                        "target_table": target,
                        "target_column": ref.get("references_column", ""),
                        "relationship_type": ref.get("cardinality", "Many-to-One"),
                    }
                )
        for rel in table.get("metadata_profile", {}).get("many_to_many", []):
            entity1 = str(rel.get("entity1", "")).strip()
            entity2 = str(rel.get("entity2", "")).strip()
            if entity1 in visible_tables and entity2 in visible_tables:
                many_to_many_rows.append(
                    {
                        "entity1": entity1,
                        "entity2": entity2,
                        "schema_name": rel.get("schema_name", ""),
                        "intersect_entity_name": rel.get("intersect_entity_name", ""),
                    }
                )
    if relationship_rows:
        st.markdown("#### One-to-Many / Many-to-One")
        rel_df = pd.DataFrame(relationship_rows).drop_duplicates()
        st.dataframe(rel_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download relationship edges CSV",
            data=rel_df.to_csv(index=False).encode("utf-8"),
            file_name="dataverse_relationship_edges.csv",
            mime="text/csv",
        )
    if many_to_many_rows:
        deduped = []
        seen = set()
        for row in many_to_many_rows:
            key = tuple(sorted([row["entity1"], row["entity2"]]) + [row["schema_name"]])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        mn_df = pd.DataFrame(deduped)
        st.markdown("#### Many-to-Many")
        st.dataframe(mn_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download many-to-many CSV",
            data=mn_df.to_csv(index=False).encode("utf-8"),
            file_name="dataverse_many_to_many.csv",
            mime="text/csv",
        )


def render_modeling_summary_section() -> None:
    catalog_tables = _prefix_filter(st.session_state.get("catalog_tables", {}))
    if not catalog_tables:
        st.info("No catalog data is loaded yet. Fetch Dataverse metadata first.")
        return

    st.caption(
        "Migration readiness scorecard ranked by FK centrality (HIGH / MEDIUM / LOW) and priority "
        "(P0 / P1). Drills into rollup fields to model as dbt metrics, formula columns to model as dbt "
        "expressions, shadow columns to drop, multiselect fields requiring junction tables, and "
        "state-machine picklists (statecode / statuscode) with their full option-value sets."
    )
    st.markdown("### Modeling Summary")
    summary_rows = []
    state_rows = []
    drop_rows = []
    computed_rows = []
    multi_rows = []

    for table in catalog_tables.values():
        profile = table.get("metadata_profile", {})
        summary_rows.append(
            {
                "table_name": table.get("table_name", ""),
                "primary_key": table.get("primary_key", ""),
                "centrality_score": profile.get("centrality_score", ""),
                "migration_priority": profile.get("migration_priority", ""),
                "recommended_target_entity": profile.get("recommended_target_entity", ""),
                "lookup_columns": profile.get("lookup_columns", 0),
                "incoming_relationships": profile.get("incoming_relationships", 0),
                "outgoing_relationships": profile.get("outgoing_relationships", 0),
                "rollup_fields": profile.get("rollup_fields", 0),
                "formula_fields": profile.get("formula_fields", 0),
                "multiselect_columns": profile.get("multiselect_columns", 0),
                "notes": profile.get("notes", ""),
            }
        )
        for column in table.get("schema", []):
            row = {
                "table_name": table.get("table_name", ""),
                "column_name": column.get("column_name", ""),
                "attribute_type": column.get("attribute_type", ""),
                "sql_type": column.get("sql_type", ""),
                "category": column.get("category", ""),
                "modeling_action": column.get("modeling_action", ""),
                "option_values": column.get("option_values", ""),
            }
            if column.get("is_state_machine_candidate"):
                state_rows.append(row)
            if column.get("modeling_action") == "Drop from target model":
                drop_rows.append(row)
            if column.get("category") in {"Rollup", "Formula"}:
                computed_rows.append(row)
            if column.get("attribute_type") == "MultiSelectPicklist":
                multi_rows.append(row)

    summary_df = pd.DataFrame(summary_rows).sort_values(
        by=["centrality_score", "lookup_columns", "table_name"],
        ascending=[True, False, True],
    )
    top_cols = st.columns(4)
    top_cols[0].metric("Tables", len(summary_df))
    top_cols[1].metric("High centrality", int((summary_df["centrality_score"] == "HIGH").sum()))
    top_cols[2].metric("P0 candidates", int((summary_df["migration_priority"] == "P0 - Critical").sum()))
    top_cols[3].metric("State machine fields", len(state_rows))

    st.markdown("#### Aggregate / Reference Prioritization")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
    st.download_button(
        "Download modeling summary CSV",
        data=summary_df.to_csv(index=False).encode("utf-8"),
        file_name="dataverse_modeling_summary.csv",
        mime="text/csv",
    )

    if computed_rows:
        st.markdown("#### Rollup and Formula Fields")
        st.dataframe(pd.DataFrame(computed_rows), use_container_width=True, hide_index=True)
    if drop_rows:
        st.markdown("#### Columns Marked to Drop")
        st.dataframe(pd.DataFrame(drop_rows), use_container_width=True, hide_index=True)
    if multi_rows:
        st.markdown("#### MultiSelect Columns")
        st.dataframe(pd.DataFrame(multi_rows), use_container_width=True, hide_index=True)
    if state_rows:
        st.markdown("#### State Machine Candidates")
        st.dataframe(pd.DataFrame(state_rows), use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Dataverse Data Catalog", layout="wide")
    load_styles()
    init_state()

    render_sidebar_help()

    st.markdown(
        '<div class="catalog-hero">'
        '<div class="hero-balloons" aria-hidden="true">'
        '<span class="balloon balloon-red"></span>'
        '<span class="balloon balloon-gold"></span>'
        '<span class="balloon balloon-sky"></span>'
        '<span class="balloon balloon-navy"></span>'
        '</div>'
        '<p class="hero-kicker">A cheerful little metadata command center</p>'
        "<h1>Dataverse Data Catalog</h1>"
        "<p>Collaborative Streamlit cataloging tool for Dataverse metadata, backed by Supabase</p>"
        '<p class="hero-donation">'
        'Made with a bit of love. If this catalog saves your day, imaginary donations are warmly accepted.'
        '</p>'
        "</div>",
        unsafe_allow_html=True,
    )

    tab_input, tab_api, tab_catalog, tab_relationships, tab_modeling, tab_batch, tab_journeys = st.tabs(
        ["Input & Sync", "API Discovery", "Catalog", "Relationships",
         "Modeling Summary", "Batch", "User Journey Mapping"]
    )
    with tab_input:
        render_input_section()
    with tab_api:
        def _on_api_merge(updated_tables: dict) -> None:
            st.session_state["catalog_tables"] = updated_tables

        render_api_discovery(
            st.session_state.get("catalog_tables", {}),
            on_merge=_on_api_merge,
        )
    with tab_catalog:
        render_catalog_section()
    with tab_relationships:
        render_relationships_section()
    with tab_modeling:
        render_modeling_summary_section()
    with tab_batch:
        render_batch_section()
    with tab_journeys:
        render_journey_mapping(
            st.session_state.get("catalog_tables") or st.session_state.get("database_snapshot", {}),
            current_actor_name(),
        )


if __name__ == "__main__":
    main()
