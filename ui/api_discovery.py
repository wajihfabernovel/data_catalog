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
