"""API Discovery tab — wraps DataverseMetadataClient with a UI for progress, overrides, and merge."""

from __future__ import annotations

import contextlib
import os
from datetime import datetime, timezone
from typing import Callable

import pandas as pd
import streamlit as st

from services.dataverse_metadata import DataverseConfigError, DataverseMetadataClient, load_dataverse_config
from utils.helpers import build_default_table_state


# ---------------------------------------------------------------------------
# Category normalisation  (DataverseMetadataClient → CSS badge keys)
# ---------------------------------------------------------------------------

_CATEGORY_NORMALIZE: dict[str, str] = {
    "Custom Business": "BUSINESS",
    "Primary Key": "BUSINESS",
    "System": "SYSTEM",
    "Virtual / Shadow": "SHADOW",
    "Rollup": "ROLLUP",
    "Formula": "FORMULA",
    "Lookup / FK": "LOOKUP",
    "MultiSelect": "LOOKUP",
}

_STAT_LABELS = [
    ("total",    "Total attrs"),
    ("business", "Business"),
    ("system",   "System"),
    ("shadow",   "Shadow"),
    ("rollup",   "Rollup"),
    ("formula",  "Formula"),
    ("lookup",   "FK / Lookup"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def _env_override(**kwargs: str):
    """Temporarily patch os.environ for the duration of the block."""
    saved = {}
    for key, val in kwargs.items():
        if val:
            saved[key] = os.environ.get(key)
            os.environ[key] = val
    try:
        yield
    finally:
        for key, original in saved.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


def _build_client(**env_overrides: str) -> DataverseMetadataClient:
    with _env_override(**env_overrides):
        return DataverseMetadataClient()


def _extract_picklist_options(schema: list[dict]) -> list[dict]:
    """Convert the formatted option_values strings on state-machine columns into structured dicts."""
    result = []
    for col in schema:
        if not col.get("is_state_machine_candidate"):
            continue
        raw = (col.get("option_values") or "").strip()
        if not raw:
            continue
        options = []
        for part in raw.split(";"):
            part = part.strip()
            if "=" in part:
                val, label = part.split("=", 1)
                options.append(
                    {"value": val.strip(), "label": label.strip(), "color": ""})
            elif part:
                options.append({"value": part, "label": "", "color": ""})
        if options:
            result.append(
                {"logical_name": col["column_name"], "options": options})
    return result


def _merge_profile_into_catalog(profile: dict, catalog_tables: dict) -> dict:
    """Merge a DataverseMetadataClient entity profile into the catalog table entry."""
    tkey = profile["table_key"]
    api_schema: list[dict] = profile.get("schema", [])
    meta_profile: dict = profile.get("metadata_profile", {})

    base = dict(catalog_tables[tkey]) if tkey in catalog_tables else build_default_table_state(
        {
            "table_key": tkey,
            "table_name": profile["table_name"],
            "primary_key": profile.get("primary_key", ""),
            "schema": [],
        }
    )

    if profile.get("primary_key") and not base.get("primary_key"):
        base["primary_key"] = profile["primary_key"]

    # Enrich existing schema columns; add API-only columns
    existing_by_name: dict[str, dict] = {
        col["column_name"]: dict(col) for col in base.get("schema", [])
    }
    for attr in api_schema:
        name = attr["column_name"]
        enrichment = {
            "attribute_type": attr.get("attribute_type", ""),
            "source_type": attr.get("source_type_label", ""),
            "column_category": _CATEGORY_NORMALIZE.get(attr.get("category", ""), "SYSTEM"),
            "lookup_target": attr.get("targets", ""),
            "is_custom": bool(attr.get("is_custom_attribute", False)),
            "is_valid_odata": bool(attr.get("is_valid_odata_attribute", True)),
            "max_length": attr.get("max_length") or None,
            "precision": attr.get("precision") or None,
            "option_values": attr.get("option_values", ""),
            "is_state_machine_candidate": bool(attr.get("is_state_machine_candidate", False)),
            "modeling_action": attr.get("modeling_action", ""),
            "sql_type": attr.get("sql_type", ""),
        }
        if name in existing_by_name:
            existing_by_name[name].update(enrichment)
        else:
            existing_by_name[name] = {
                "column_name": name,
                "edm_type": attr.get("edm_type", ""),
                **enrichment,
            }

    base["schema"] = sorted(existing_by_name.values(),
                            key=lambda c: c["column_name"].casefold())
    base["relationships"] = profile.get(
        "relationships", base.get("relationships", {}))
    base["metadata_profile"] = meta_profile

    # Normalised dataverse_meta consumed by cards and forms
    base["dataverse_meta"] = {
        "stats": {
            "total":    meta_profile.get("total_attributes", len(api_schema)),
            "business": meta_profile.get("custom_business_columns", 0),
            "system":   meta_profile.get("system_columns", 0),
            "shadow":   meta_profile.get("virtual_shadow_columns", 0),
            "rollup":   meta_profile.get("rollup_fields", 0),
            "formula":  meta_profile.get("formula_fields", 0),
            "lookup":   meta_profile.get("lookup_columns", 0),
        },
        "picklist_options": _extract_picklist_options(api_schema),
        "fetched_at": meta_profile.get("api_enriched_at", datetime.now(timezone.utc).isoformat()),
    }
    return base


# ---------------------------------------------------------------------------
# Per-table result panel
# ---------------------------------------------------------------------------

def _render_result(result: dict) -> None:
    if result.get("error"):
        st.error(f"Fetch error: {result['error']}")
        return

    meta = result.get("metadata_profile", {})
    api_schema: list[dict] = result.get("schema", [])

    # ── Stats row ──────────────────────────────────────────────────────────
    stats = {
        "total":    meta.get("total_attributes", len(api_schema)),
        "business": meta.get("custom_business_columns", 0),
        "system":   meta.get("system_columns", 0),
        "shadow":   meta.get("virtual_shadow_columns", 0),
        "rollup":   meta.get("rollup_fields", 0),
        "formula":  meta.get("formula_fields", 0),
        "lookup":   meta.get("lookup_columns", 0),
    }
    cols = st.columns(len(_STAT_LABELS))
    for col, (key, label) in zip(cols, _STAT_LABELS):
        col.metric(label, stats[key])

    # ── Profile summary ───────────────────────────────────────────────────
    centrality = meta.get("centrality_score", "")
    priority = meta.get("migration_priority", "")
    target = meta.get("recommended_target_entity", "")
    if any([centrality, priority, target]):
        st.caption(
            " · ".join(filter(None, [
                f"Centrality: **{centrality}**" if centrality else "",
                f"Priority: **{priority}**" if priority else "",
                f"Target entity: `{target}`" if target else "",
            ]))
        )

    # ── State-machine candidates ──────────────────────────────────────────
    state_cols = [c for c in api_schema if c.get(
        "is_state_machine_candidate") and c.get("option_values")]
    if state_cols:
        with st.expander("State-machine columns (picklist option values)", expanded=False):
            for col in state_cols:
                st.markdown(
                    f"- **`{col['column_name']}`**: {col['option_values']}")

    # ── FK dependencies ───────────────────────────────────────────────────
    fk_cols = [c for c in api_schema if c.get("category") == "Lookup / FK"]
    if fk_cols:
        with st.expander(f"FK dependencies ({len(fk_cols)} Lookup columns)", expanded=False):
            st.dataframe(
                pd.DataFrame([
                    {"column": c["column_name"], "target_entity": c.get(
                        "targets", ""), "sql_type": c.get("sql_type", "")}
                    for c in fk_cols
                ]),
                use_container_width=True, hide_index=True,
            )

    # ── Computed columns ──────────────────────────────────────────────────
    computed = [c for c in api_schema if c.get(
        "category") in ("Rollup", "Formula")]
    if computed:
        with st.expander(f"Computed → dbt ({len(computed)} not persisted in Azure SQL)", expanded=False):
            st.dataframe(
                pd.DataFrame([
                    {"column": c["column_name"], "category": c.get(
                        "category", ""), "type": c.get("attribute_type", "")}
                    for c in computed
                ]),
                use_container_width=True, hide_index=True,
            )

    # ── M:N relationships (only available from fetch_all_custom_entities) ─
    mn = meta.get("many_to_many", [])
    if mn:
        with st.expander(f"Many-to-Many relationships ({len(mn)})", expanded=False):
            st.dataframe(pd.DataFrame(
                mn), use_container_width=True, hide_index=True)

    # ── Shadow columns ────────────────────────────────────────────────────
    shadow = [c for c in api_schema if c.get("category") == "Virtual / Shadow"]
    if shadow:
        names = ", ".join(f"`{c['column_name']}`" for c in shadow[:30])
        if len(shadow) > 30:
            names += f" … and {len(shadow) - 30} more"
        st.caption(
            f"Shadow / virtual columns to drop ({len(shadow)}): {names}")


# ---------------------------------------------------------------------------
# Public renderer
# ---------------------------------------------------------------------------

def render_api_discovery(
    catalog_tables: dict,
    on_merge: Callable[[dict], None],
) -> None:
    st.markdown("### Dataverse API Metadata Discovery")
    st.caption(
        "Uses **DataverseMetadataClient** — 5 API calls per table "
        "(base attrs · lookup targets · string lengths · picklist options · decimal precision) "
        "plus the global relationship graph when fetching all custom tables."
    )

    # ── Connection settings ──────────────────────────────────────────────────
    dv_config = load_dataverse_config()
    env_configured = not dv_config["missing"]

    with st.expander(
        "Connection settings" +
            (" ✓ configured from .env" if env_configured else " ⚠ incomplete .env"),
        expanded=not env_configured,
    ):
        if env_configured:
            st.success(
                f"Using `{dv_config['base_url']}` · auth mode: `{dv_config['auth_mode']}`")

        st.caption("Override any value below (leave blank to use .env):")
        c1, c2, c3 = st.columns(3)
        ov_tenant = c1.text_input("Tenant ID",     value="", key="api_ov_tenant",
                                  placeholder=dv_config.get("tenant_id", "from .env"))
        ov_client_id = c2.text_input("Client ID",     value="", key="api_ov_client_id",
                                     placeholder=dv_config.get("client_id", "from .env"))
        ov_secret = c3.text_input(
            "Client secret", value="", key="api_ov_secret", type="password")
        ov_base_url = st.text_input("Base URL",      value="", key="api_ov_base_url",
                                    placeholder=dv_config.get("base_url", "from .env"))

    def _get_client() -> DataverseMetadataClient:
        overrides = {
            k: v for k, v in {
                "AZURE_TENANT_ID":    ov_tenant,
                "AZURE_CLIENT_ID":    ov_client_id,
                "AZURE_CLIENT_SECRET": ov_secret,
                "DATAVERSE_BASE_URL": ov_base_url,
            }.items() if v.strip()
        }
        return _build_client(**overrides)

    # ── Fetch mode ───────────────────────────────────────────────────────────
    st.session_state.setdefault("api_results", {})
    api_results: dict = st.session_state["api_results"]

    fetch_mode = st.radio(
        "Fetch mode",
        ["Selected tables (from catalog)",
         "All custom Dataverse tables (auto-discover)"],
        horizontal=True,
        key="api_fetch_mode",
    )
    fetch_all = fetch_mode.startswith("All")

    _PREFIX = "hive_"
    selected_keys: list[str] = []
    if not fetch_all:
        if not catalog_tables:
            st.info(
                "No tables loaded yet. Parse XML metadata or refresh from Supabase first.")
            return
        sorted_keys = sorted(
            (k for k in catalog_tables if catalog_tables[k].get("table_name", "").startswith(_PREFIX)),
            key=lambda k: catalog_tables[k]["table_name"].casefold(),
        )
        if not sorted_keys:
            st.info(f"No tables with prefix '{_PREFIX}' found in the catalog.")
            return
        display_names = {k: catalog_tables[k]["table_name"] for k in sorted_keys}
        selected_keys = st.multiselect(
            "Select tables",
            options=sorted_keys,
            format_func=lambda k: display_names[k],
            default=sorted_keys,
            key="api_selected_keys",
        )
        st.caption(
            f"{len(selected_keys)} of {len(sorted_keys)} tables selected.")
    else:
        st.info(
            f"Will fetch all custom entity names, filter to `{_PREFIX}*` in Python, "
            f"then run 5 API calls per matching table (base attrs · lookup · string · picklist · decimal). "
            f"Global relationship graph is skipped for performance."
        )

    # ── Action buttons ───────────────────────────────────────────────────────
    st.markdown('<p class="button-group-label">Actions</p>',
                unsafe_allow_html=True)
    b1, b2, b3 = st.columns(3)

    fetched_keys = [k for k in (selected_keys or list(
        api_results)) if k in api_results and not api_results[k].get("error")]

    run_fetch = b1.button(
        "Fetch from API",
        use_container_width=True,
        disabled=(not fetch_all and not selected_keys),
        key="api_btn_fetch",
    )
    run_merge = b2.button(
        f"Merge {len(fetched_keys)} table(s) into catalog" if fetched_keys else "Merge into catalog",
        use_container_width=True,
        disabled=not fetched_keys,
        key="api_btn_merge",
    )
    run_clear = b3.button(
        "Clear results",
        use_container_width=True,
        disabled=not api_results,
        key="api_btn_clear",
    )

    # ── Fetch ────────────────────────────────────────────────────────────────
    if run_fetch:
        try:
            client = _get_client()
        except DataverseConfigError as exc:
            st.error(f"Dataverse not configured: {exc}")
            st.stop()

        results: dict = {}

        if fetch_all:
            with st.status(f"Fetching '{_PREFIX}*' Dataverse tables…", expanded=True) as s:
                st.write(
                    f"Calling `EntityDefinitions?$filter=IsCustomEntity eq true "
                    f"and startswith(LogicalName,'{_PREFIX}')` …"
                )
                try:
                    profiles = client.fetch_all_custom_entities(name_prefix=_PREFIX)
                    for p in profiles:
                        results[p["table_key"]] = p
                    s.update(
                        label=f"Done — {len(profiles)} '{_PREFIX}*' tables fetched.", state="complete")
                except Exception as exc:
                    s.update(label="Fetch failed", state="error")
                    st.error(str(exc))
                    st.stop()
        else:
            table_names = [catalog_tables[k]["table_name"]
                           for k in selected_keys]
            with st.status(f"Fetching {len(table_names)} table(s)…", expanded=True) as s:
                failed = 0
                for idx, (tkey, tname) in enumerate(zip(selected_keys, table_names), 1):
                    st.write(f"[{idx}/{len(table_names)}] `{tname}`")
                    try:
                        profile = client.fetch_entity_profile(tname)
                        results[tkey] = profile
                        mp = profile.get("metadata_profile", {})
                        st.write(
                            f"  {mp.get('total_attributes', 0)} attrs · "
                            f"{mp.get('custom_business_columns', 0)} business · "
                            f"{mp.get('rollup_fields', 0)} rollup · "
                            f"{mp.get('lookup_columns', 0)} FK · "
                            f"centrality {mp.get('centrality_score', '?')}"
                        )
                    except Exception as exc:
                        results[tkey] = {"table_key": tkey,
                                         "table_name": tname, "error": str(exc)}
                        st.write(f"  ERROR: {exc}")
                        failed += 1

                state = "complete" if failed == 0 else "error"
                s.update(
                    label=f"Done — {len(table_names) - failed} OK, {failed} errors.", state=state)

        st.session_state["api_results"] = results
        st.rerun()

    # ── Merge ────────────────────────────────────────────────────────────────
    if run_merge:
        updated = dict(st.session_state.get("catalog_tables", {}))
        merged_count = 0
        for tkey in fetched_keys:
            profile = api_results[tkey]
            if profile.get("error"):
                continue
            updated[tkey] = _merge_profile_into_catalog(profile, updated)
            merged_count += 1
        on_merge(updated)
        st.success(
            f"Merged {merged_count} table(s) into the catalog. "
            "Schema columns now carry attribute types, source types, column categories, "
            "FK targets, precise SQL types, and centrality scores."
        )

    # ── Clear ────────────────────────────────────────────────────────────────
    if run_clear:
        st.session_state["api_results"] = {}
        st.rerun()

    # ── Results ──────────────────────────────────────────────────────────────
    if not api_results:
        return

    st.divider()
    st.markdown("#### Fetch Results")

    ok = sum(1 for r in api_results.values() if not r.get("error"))
    err = len(api_results) - ok
    total_attrs = sum(
        r.get("metadata_profile", {}).get("total_attributes", 0)
        for r in api_results.values()
        if not r.get("error")
    )
    m1, m2, m3 = st.columns(3)
    m1.metric("Tables OK", ok)
    m2.metric("Errors", err)
    m3.metric("Total attributes", total_attrs)

    st.divider()
    visible = set(selected_keys) & set(
        api_results) if not fetch_all else set(api_results)
    for tkey in sorted(visible, key=lambda k: api_results[k].get("table_name", k).casefold()):
        result = api_results[tkey]
        icon = "" if result.get("error") else ""
        with st.expander(f"{icon} `{result.get('table_name', tkey)}`", expanded=False):
            _render_result(result)
