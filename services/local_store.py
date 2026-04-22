"""Local JSON persistence for offline or pre-SharePoint use."""

from __future__ import annotations

import json
from pathlib import Path

from utils.helpers import parse_date, serialize_date


DRAFT_DIR = Path(".data_catalog_drafts")
DRAFT_PATH = DRAFT_DIR / "latest.json"


def _serialize_table(table: dict) -> dict:
    payload = dict(table)
    signoff = dict(payload.get("signoff", {}))
    signoff["date_approved"] = serialize_date(signoff.get("date_approved"))
    payload["signoff"] = signoff
    return payload


def _deserialize_table(table: dict) -> dict:
    payload = dict(table)
    signoff = dict(payload.get("signoff", {}))
    signoff["date_approved"] = parse_date(signoff.get("date_approved"))
    payload["signoff"] = signoff
    return payload


def save_local_catalog_state(tables: dict[str, dict]) -> Path:
    DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    serialized = {
        "tables": {table_key: _serialize_table(table) for table_key, table in tables.items()}
    }
    DRAFT_PATH.write_text(json.dumps(serialized, indent=2), encoding="utf-8")
    return DRAFT_PATH


def load_local_catalog_state() -> dict[str, dict]:
    if not DRAFT_PATH.exists():
        raise FileNotFoundError(f"No local draft found at {DRAFT_PATH}.")

    payload = json.loads(DRAFT_PATH.read_text(encoding="utf-8"))
    tables = payload.get("tables", {})
    return {table_key: _deserialize_table(table) for table_key, table in tables.items()}
