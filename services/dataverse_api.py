"""Dataverse Web API client — 4-call attribute metadata discovery per table."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

from models.mappings import map_attr_type_to_sql
from utils.helpers import table_key_from_name

load_dotenv()

_SOURCE_TYPE: dict[int, str] = {0: "PERSISTED", 2: "ROLLUP", 3: "FORMULA"}
_LOOKUP_ATTR_TYPES = {"Lookup", "Owner", "Customer", "PartyList"}


# ---------------------------------------------------------------------------
# Authentication helpers
# ---------------------------------------------------------------------------


def obtain_token_client_credentials(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    resource_url: str,
    *,
    timeout: int = 30,
) -> str:
    """Acquire an OAuth2 bearer token via the client-credentials flow."""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": resource_url.rstrip("/"),
    }
    resp = requests.post(token_url, data=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()["access_token"]


def obtain_token_from_env(resource_url: str | None = None) -> str | None:
    """Try to obtain a token from environment variables (client-credentials)."""
    tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()
    client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
    client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()
    if not all([tenant_id, client_id, client_secret]):
        return None
    resource = resource_url or os.getenv("DATAVERSE_BASE_URL", "").strip()
    if not resource:
        return None
    return obtain_token_client_credentials(tenant_id, client_id, client_secret, resource)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _api_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }


def _odata_get(url: str, token: str, *, timeout: int = 30) -> list[dict]:
    """GET an OData endpoint and return the `value` list."""
    resp = requests.get(url, headers=_api_headers(token), timeout=timeout)
    resp.raise_for_status()
    return resp.json().get("value", [])


def _classify_column(attr: dict) -> str:
    """Classify a Dataverse attribute into a display category."""
    is_logical = bool(attr.get("IsLogical", False))
    is_valid_odata = bool(attr.get("IsValidODataAttribute", True))
    source_type = int(attr.get("SourceType") or 0)
    attr_type = attr.get("AttributeType", "")
    is_custom = bool(attr.get("IsCustomAttribute", False))

    if is_logical and not is_valid_odata:
        return "SHADOW"
    if source_type == 2:
        return "ROLLUP"
    if source_type == 3:
        return "FORMULA"
    if attr_type in _LOOKUP_ATTR_TYPES:
        return "LOOKUP"
    if is_custom and is_valid_odata:
        return "BUSINESS"
    return "SYSTEM"


def _parse_base_attr(raw: dict) -> dict[str, Any]:
    source_type_int = int(raw.get("SourceType") or 0)
    targets: list = raw.get("Targets") or []
    return {
        "column_name": raw.get("LogicalName", ""),
        "attribute_type": raw.get("AttributeType", ""),
        "edm_type": "",
        "sql_type": "",
        "source_type": _SOURCE_TYPE.get(source_type_int, "PERSISTED"),
        "is_custom": bool(raw.get("IsCustomAttribute", False)),
        "is_valid_odata": bool(raw.get("IsValidODataAttribute", True)),
        "is_primary": bool(raw.get("IsPrimaryId", False)),
        "is_logical": bool(raw.get("IsLogical", False)),
        "column_category": _classify_column(raw),
        "lookup_target": targets[0] if targets else "",
        "max_length": None,
        "precision": None,
    }


# ---------------------------------------------------------------------------
# Public API: single-table fetch (4 calls)
# ---------------------------------------------------------------------------


def fetch_entity_metadata(
    base_url: str,
    api_version: str,
    table_name: str,
    access_token: str,
    *,
    timeout: int = 30,
) -> dict[str, Any]:
    """
    Fetch complete attribute metadata for one Dataverse entity.

    Makes up to 4 calls:
      1. /Attributes                         — base metadata for every column
      2. /…/StringAttributeMetadata          — MaxLength for NVARCHAR sizing
      3. /…/PicklistAttributeMetadata        — option-set values for state-machine columns
      4. /…/DecimalAttributeMetadata +       — Precision for DECIMAL/MONEY sizing
         /…/MoneyAttributeMetadata
    """
    api_root = f"{base_url.rstrip('/')}/api/data/{api_version}"
    entity_path = f"{api_root}/EntityDefinitions(LogicalName='{table_name}')/Attributes"

    # ── Call 1: base attributes ──────────────────────────────────────────────
    base_select = (
        "LogicalName,AttributeType,IsCustomAttribute,SourceType,"
        "IsPrimaryId,IsValidODataAttribute,IsLogical,Targets"
    )
    try:
        base_attrs = _odata_get(f"{entity_path}?$select={base_select}", access_token, timeout=timeout)
    except requests.RequestException as exc:
        return {
            "table_name": table_name,
            "table_key": table_key_from_name(table_name),
            "primary_key": "",
            "attributes": [],
            "picklist_options": [],
            "stats": {},
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }

    attr_map: dict[str, dict] = {
        a.get("LogicalName", ""): _parse_base_attr(a)
        for a in base_attrs
        if a.get("LogicalName")
    }

    # ── Call 2: string max lengths ───────────────────────────────────────────
    try:
        str_attrs = _odata_get(
            f"{entity_path}/Microsoft.Dynamics.CRM.StringAttributeMetadata"
            f"?$select=LogicalName,MaxLength",
            access_token,
            timeout=timeout,
        )
        for a in str_attrs:
            name = a.get("LogicalName", "")
            if name in attr_map:
                attr_map[name]["max_length"] = a.get("MaxLength")
    except requests.RequestException:
        pass

    # ── Call 3: picklist option values ───────────────────────────────────────
    picklist_options: list[dict] = []
    try:
        pl_attrs = _odata_get(
            f"{entity_path}/Microsoft.Dynamics.CRM.PicklistAttributeMetadata"
            f"?$expand=OptionSet($select=Options)&$select=LogicalName,DefaultFormValue",
            access_token,
            timeout=timeout,
        )
        for a in pl_attrs:
            name = a.get("LogicalName", "")
            raw_opts = (a.get("OptionSet") or {}).get("Options", [])
            options = [
                {
                    "value": opt.get("Value"),
                    "label": (
                        ((opt.get("Label") or {}).get("UserLocalizedLabel") or {})
                        .get("Label", "")
                    ),
                    "color": opt.get("Color") or "",
                }
                for opt in raw_opts
            ]
            picklist_options.append({"logical_name": name, "options": options})
    except requests.RequestException:
        pass

    # ── Call 4: decimal + money precision ────────────────────────────────────
    for type_name in ("DecimalAttributeMetadata", "MoneyAttributeMetadata"):
        try:
            dec_attrs = _odata_get(
                f"{entity_path}/Microsoft.Dynamics.CRM.{type_name}"
                f"?$select=LogicalName,Precision,MinValue,MaxValue",
                access_token,
                timeout=timeout,
            )
            for a in dec_attrs:
                name = a.get("LogicalName", "")
                if name in attr_map:
                    attr_map[name]["precision"] = a.get("Precision")
        except requests.RequestException:
            pass

    # ── Resolve final SQL types ──────────────────────────────────────────────
    for attr in attr_map.values():
        attr["sql_type"] = map_attr_type_to_sql(
            attr["attribute_type"],
            max_length=attr["max_length"],
            precision=attr["precision"],
        )

    # ── Summary stats ────────────────────────────────────────────────────────
    attributes = list(attr_map.values())
    primary_key = next(
        (
            name
            for name, a in attr_map.items()
            if a["is_primary"] and a["attribute_type"] == "Uniqueidentifier"
        ),
        "",
    )
    stats: dict[str, int] = {
        "total": len(attributes),
        "business": sum(1 for a in attributes if a["column_category"] == "BUSINESS"),
        "system": sum(1 for a in attributes if a["column_category"] == "SYSTEM"),
        "shadow": sum(1 for a in attributes if a["column_category"] == "SHADOW"),
        "rollup": sum(1 for a in attributes if a["column_category"] == "ROLLUP"),
        "formula": sum(1 for a in attributes if a["column_category"] == "FORMULA"),
        "lookup": sum(1 for a in attributes if a["column_category"] == "LOOKUP"),
    }

    return {
        "table_name": table_name,
        "table_key": table_key_from_name(table_name),
        "primary_key": primary_key,
        "attributes": attributes,
        "picklist_options": picklist_options,
        "stats": stats,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }
