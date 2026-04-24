"""Main Streamlit entry point for the Dataverse data catalog tool."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from parser.xml_parser import parse_dataverse_xml
from services.dataverse_metadata import DataverseConfigError, DataverseMetadataClient, load_dataverse_config
from services.export import build_excel_workbook
from services.local_store import load_local_catalog_state, save_local_catalog_state
from services.supabase_store import SupabaseConfigError, SupabaseStore, load_supabase_config
from ui.cards import render_table_card
from ui.journeys import render_journey_mapping
from utils.helpers import build_default_table_state, merge_table_state, normalize_table_names

load_dotenv()

STYLE_PATH = Path(__file__).parent / "assets" / "styles.css"
EXPORT_DIR = Path(__file__).parent / "exports"


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

    try:
        fetched_tables = get_dataverse_client().fetch_entities(requested_tables)
    except (RuntimeError, DataverseConfigError, ValueError, OSError) as exc:
        st.error(str(exc))
        return
    except Exception as exc:  # noqa: BLE001
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
    st.success(f"Fetched Dataverse metadata for {len(fetched_tables)} table(s).")


def fetch_all_custom_dataverse_tables_and_sync() -> None:
    try:
        fetched_tables = get_dataverse_client().fetch_all_custom_entities()
    except (RuntimeError, DataverseConfigError, ValueError, OSError) as exc:
        st.error(str(exc))
        return
    except Exception as exc:  # noqa: BLE001
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
    st.success(f"Fetched all custom Dataverse entities: {len(fetched_tables)} table(s).")


def render_input_section() -> None:
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
    catalog_tables = st.session_state.get("catalog_tables", {})
    if not catalog_tables or _has_missing_schema(catalog_tables):
        try:
            refresh_from_database(show_message=False)
            catalog_tables = st.session_state.get("catalog_tables", {})
        except (RuntimeError, SupabaseConfigError):
            catalog_tables = {}

    if not catalog_tables:
        st.info("No catalog data is loaded yet. Use Input & Sync to parse metadata or refresh from Supabase.")
        return

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
                f'<div class="step-item"><div class="step-number">{num}</div><div class="step-text">{text}</div></div>',
                unsafe_allow_html=True,
            )

        st.caption(f"Today: {date.today().isoformat()}")


def render_batch_section() -> None:
    catalog_tables = st.session_state.get("catalog_tables", {})
    if not catalog_tables:
        st.info("No tables loaded yet. Use the Input & Sync tab to parse metadata first.")
        return

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
        '<p class="hero-donation">Made with a bit of love. If this catalog saves your day, imaginary donations are warmly accepted.</p>'
        "</div>",
        unsafe_allow_html=True,
    )

    tab_input, tab_catalog, tab_batch, tab_journeys = st.tabs(
        ["Input & Sync", "Catalog", "Batch", "User Journey Mapping"]
    )
    with tab_input:
        render_input_section()
    with tab_catalog:
        render_catalog_section()
    with tab_batch:
        render_batch_section()
    with tab_journeys:
        render_journey_mapping(
            st.session_state.get("catalog_tables") or st.session_state.get("database_snapshot", {}),
            current_actor_name(),
        )


if __name__ == "__main__":
    main()
