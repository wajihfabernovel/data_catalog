"""API Discovery tab — fetches Dataverse attribute metadata for all catalog tables."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Callable

import pandas as pd
import streamlit as st

from services.dataverse_api import (
    fetch_entity_metadata,
    obtain_token_client_credentials,
    obtain_token_from_env,
)
from utils.helpers import build_default_table_state


# ---------------------------------------------------------------------------
# Internal helpers
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


def _init_api_state() -> None:
    st.session_state.setdefault("api_results", {})
    st.session_state.setdefault("api_base_url", os.getenv("DATAVERSE_BASE_URL", "").strip())
    st.session_state.setdefault("api_version", "v9.2")


def _resolve_token(base_url: str, manual_token: str, tenant: str, client_id: str, client_secret: str) -> tuple[str, str | None]:
    """Return (token, error_message). Priority: manual > override creds > env creds."""
    if manual_token.strip():
        return manual_token.strip(), None

    if tenant.strip() and client_id.strip() and client_secret.strip():
        try:
            return obtain_token_client_credentials(tenant.strip(), client_id.strip(), client_secret.strip(), base_url), None
        except Exception as exc:
            return "", f"Token acquisition failed: {exc}"

    try:
        token = obtain_token_from_env(base_url)
        if token:
            return token, None
    except Exception as exc:
        return "", f"Env token acquisition failed: {exc}"

    return "", "No bearer token found. Provide one above or configure Azure credentials in .env"


def _merge_result_into_catalog(result: dict, catalog_tables: dict) -> dict:
    """Merge API-fetched attribute data into an existing (or new) catalog entry."""
    tkey = result["table_key"]
    attributes: list[dict] = result.get("attributes", [])

    if tkey in catalog_tables:
        base = dict(catalog_tables[tkey])
    else:
        base = build_default_table_state(
            {
                "table_key": tkey,
                "table_name": result["table_name"],
                "primary_key": result.get("primary_key", ""),
                "schema": [],
            }
        )

    if result.get("primary_key") and not base.get("primary_key"):
        base["primary_key"] = result["primary_key"]

    # Build a name-indexed map of existing schema columns (from XML parse)
    existing_by_name: dict[str, dict] = {
        col["column_name"]: dict(col) for col in base.get("schema", [])
    }

    # Enrich existing columns and add any new ones from the API response
    for attr in attributes:
        name = attr["column_name"]
        enrichment = {
            "attribute_type": attr["attribute_type"],
            "source_type": attr["source_type"],
            "column_category": attr["column_category"],
            "lookup_target": attr["lookup_target"],
            "is_custom": attr["is_custom"],
            "is_valid_odata": attr["is_valid_odata"],
            "max_length": attr.get("max_length"),
            "precision": attr.get("precision"),
        }
        if name in existing_by_name:
            existing_by_name[name].update(enrichment)
            # Prefer the precisely resolved sql_type from the API
            if attr["sql_type"]:
                existing_by_name[name]["sql_type"] = attr["sql_type"]
        else:
            existing_by_name[name] = {
                "column_name": name,
                "edm_type": "",
                "sql_type": attr["sql_type"],
                **enrichment,
            }

    base["schema"] = sorted(
        existing_by_name.values(), key=lambda c: c["column_name"].casefold()
    )
    base["dataverse_meta"] = {
        "stats": result.get("stats", {}),
        "picklist_options": result.get("picklist_options", []),
        "fetched_at": result.get("fetched_at", datetime.now(timezone.utc).isoformat()),
    }
    return base


# ---------------------------------------------------------------------------
# Public renderer
# ---------------------------------------------------------------------------


def render_api_discovery(
    catalog_tables: dict,
    on_merge: Callable[[dict], None],
) -> None:
    """Render the API Metadata Discovery tab."""
    _init_api_state()

    st.markdown("### Dataverse API Metadata Discovery")
    st.caption(
        "Loops over your loaded tables and calls the Dataverse Web API to fetch "
        "full attribute metadata — base attrs, string lengths, picklist option values, "
        "and decimal precision — then merges the enriched data into your catalog schema."
    )

    # ── Connection settings ──────────────────────────────────────────────────
    with st.expander(
        "Connection settings",
        expanded=not bool(st.session_state.get("api_results")),
    ):
        url_col, ver_col = st.columns([4, 1])
        base_url = url_col.text_input(
            "Dataverse base URL",
            value=st.session_state["api_base_url"],
            placeholder="https://your-org.api.crm4.dynamics.com",
            key="api_base_url_input",
        )
        api_version = ver_col.text_input(
            "API version",
            value=st.session_state["api_version"],
            key="api_version_input",
        )

        manual_token = st.text_input(
            "Bearer token (optional — leave blank to use Azure credentials from .env)",
            type="password",
            key="api_manual_token",
            help="Paste an access token, or configure AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET in your .env file.",
        )

        st.caption("Override Azure credentials (leave blank to read from .env):")
        cred1, cred2, cred3 = st.columns(3)
        override_tenant = cred1.text_input(
            "Tenant ID",
            value="",
            key="api_tenant_override",
            placeholder=os.getenv("AZURE_TENANT_ID", "from .env"),
        )
        override_client_id = cred2.text_input(
            "Client ID",
            value="",
            key="api_client_id_override",
            placeholder=os.getenv("AZURE_CLIENT_ID", "from .env"),
        )
        override_secret = cred3.text_input(
            "Client secret",
            type="password",
            value="",
            key="api_secret_override",
        )

    # ── Table selection ──────────────────────────────────────────────────────
    if not catalog_tables:
        st.info("No tables loaded yet. Parse XML metadata or refresh from Supabase first, then come back here.")
        return

    st.markdown('<p class="button-group-label">Tables to fetch</p>', unsafe_allow_html=True)
    sorted_keys = sorted(
        catalog_tables.keys(), key=lambda k: catalog_tables[k]["table_name"].casefold()
    )
    display_names = {k: catalog_tables[k]["table_name"] for k in sorted_keys}

    selected_keys: list[str] = st.multiselect(
        "Select tables",
        options=sorted_keys,
        format_func=lambda k: display_names[k],
        default=sorted_keys,
        placeholder="Choose one or more tables…",
        key="api_selected_keys",
    )

    api_results: dict = st.session_state.get("api_results", {})
    fetched_keys = [k for k in selected_keys if k in api_results and not api_results[k].get("error")]
    error_keys = [k for k in selected_keys if k in api_results and api_results[k].get("error")]

    st.caption(
        f"{len(selected_keys)} selected · {len(fetched_keys)} fetched · {len(error_keys)} errors"
    )

    # ── Action buttons ────────────────────────────────────────────────────────
    st.markdown('<p class="button-group-label">Actions</p>', unsafe_allow_html=True)
    a1, a2, a3 = st.columns([1, 1, 1])

    run_fetch = a1.button(
        "Fetch from API",
        use_container_width=True,
        disabled=not selected_keys,
        key="api_fetch_btn",
    )
    run_merge = a2.button(
        f"Merge {len(fetched_keys)} table(s) into catalog",
        use_container_width=True,
        disabled=not fetched_keys,
        key="api_merge_btn",
    )
    run_clear = a3.button(
        "Clear results",
        use_container_width=True,
        disabled=not api_results,
        key="api_clear_btn",
    )

    # ── Fetch loop ────────────────────────────────────────────────────────────
    if run_fetch:
        _base = (base_url or st.session_state["api_base_url"]).strip()
        _ver = (api_version or "v9.2").strip()

        if not _base:
            st.error("Enter a Dataverse base URL before fetching.")
            st.stop()

        token, token_err = _resolve_token(
            _base,
            manual_token,
            override_tenant,
            override_client_id,
            override_secret,
        )
        if token_err:
            st.error(token_err)
            st.stop()

        results: dict = dict(st.session_state.get("api_results", {}))
        with st.status(
            f"Fetching metadata for {len(selected_keys)} table(s)…", expanded=True
        ) as fetch_status:
            for idx, tkey in enumerate(selected_keys, 1):
                tname = display_names[tkey]
                st.write(f"[{idx}/{len(selected_keys)}] `{tname}`")
                result = fetch_entity_metadata(_base, _ver, tname, token)
                results[tkey] = result

                if result.get("error"):
                    st.write(f"  **Error:** {result['error']}")
                else:
                    s = result["stats"]
                    st.write(
                        f"  {s['total']} attrs · {s['business']} business "
                        f"· {s['rollup']} rollup · {s['formula']} formula "
                        f"· {s['lookup']} FK"
                    )

            ok = sum(1 for r in results.values() if not r.get("error"))
            err = len(results) - ok
            fetch_status.update(
                label=f"Done — {ok} succeeded, {err} errors.",
                state="complete" if err == 0 else "error",
            )

        st.session_state["api_results"] = results
        st.rerun()

    # ── Merge action ──────────────────────────────────────────────────────────
    if run_merge:
        updated = dict(st.session_state.get("catalog_tables", {}))
        for tkey in fetched_keys:
            updated[tkey] = _merge_result_into_catalog(api_results[tkey], updated)
        on_merge(updated)
        st.success(
            f"Merged {len(fetched_keys)} table(s) into the catalog. "
            "Schema columns now carry Dataverse attribute types, source types, "
            "column categories, FK targets, and resolved SQL types."
        )

    # ── Clear action ──────────────────────────────────────────────────────────
    if run_clear:
        st.session_state["api_results"] = {}
        st.rerun()

    # ── Results summary ───────────────────────────────────────────────────────
    if not api_results:
        return

    st.divider()
    st.markdown("#### Fetch Results")

    total_ok = sum(1 for r in api_results.values() if not r.get("error"))
    total_err = len(api_results) - total_ok
    total_attrs = sum(r.get("stats", {}).get("total", 0) for r in api_results.values())

    m1, m2, m3 = st.columns(3)
    m1.metric("Tables OK", total_ok)
    m2.metric("Errors", total_err)
    m3.metric("Total attributes", total_attrs)

    st.divider()

    visible_keys = set(selected_keys) & set(api_results.keys())
    for tkey in sorted(visible_keys, key=lambda k: api_results[k]["table_name"].casefold()):
        result = api_results[tkey]
        status_icon = "" if result.get("error") else ""
        with st.expander(f"{status_icon} `{result['table_name']}`", expanded=False):
            if result.get("error"):
                st.error(f"Fetch error: {result['error']}")
                continue

            stats = result.get("stats", {})
            stat_cols = st.columns(len(_STAT_LABELS))
            for col, (key, label) in zip(stat_cols, _STAT_LABELS):
                col.metric(label, stats.get(key, 0))

            # ── Picklist / state machine columns ──────────────────────────
            pl_options = [p for p in result.get("picklist_options", []) if p.get("options")]
            if pl_options:
                st.markdown("**State-machine columns (Picklist option values)**")
                for pl in pl_options:
                    opts_md = " · ".join(
                        f"`{o['value']}` {o['label']}" for o in pl["options"] if o.get("label")
                    )
                    st.markdown(f"- **`{pl['logical_name']}`**: {opts_md or '(no labels)'}")

            # ── FK dependency graph ────────────────────────────────────────
            fk_attrs = [
                a for a in result.get("attributes", [])
                if a["column_category"] == "LOOKUP"
            ]
            if fk_attrs:
                st.markdown("**FK dependencies (Lookup columns)**")
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "column": a["column_name"],
                                "target_entity": a["lookup_target"],
                                "sql_type": a["sql_type"],
                            }
                            for a in fk_attrs
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

            # ── Computed columns to skip in Azure SQL ─────────────────────
            computed = [
                a for a in result.get("attributes", [])
                if a["column_category"] in ("ROLLUP", "FORMULA")
            ]
            if computed:
                st.markdown("**Computed columns → dbt metrics (not persisted in Azure SQL)**")
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "column": a["column_name"],
                                "category": a["column_category"],
                                "attribute_type": a["attribute_type"],
                            }
                            for a in computed
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

            # ── Shadow columns to drop ────────────────────────────────────
            shadow = [
                a for a in result.get("attributes", [])
                if a["column_category"] == "SHADOW"
            ]
            if shadow:
                st.markdown(f"**Shadow columns to drop** ({len(shadow)} total)")
                shadow_names = ", ".join(f"`{a['column_name']}`" for a in shadow[:20])
                if len(shadow) > 20:
                    shadow_names += f" … and {len(shadow) - 20} more"
                st.caption(shadow_names)
