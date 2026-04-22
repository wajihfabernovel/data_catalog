"""XML parsing utilities for Dataverse metadata payloads."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from models.mappings import map_edm_to_sql
from utils.helpers import normalize_table_names, table_key_from_name


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _iter_children(element: ET.Element, child_name: str) -> list[ET.Element]:
    return [child for child in element if _local_name(child.tag) == child_name]


def _parse_entity(entity: ET.Element) -> dict:
    table_name = entity.attrib.get("Name", "").strip()

    primary_key = ""
    key_nodes = _iter_children(entity, "Key")
    if key_nodes:
        property_refs = _iter_children(key_nodes[0], "PropertyRef")
        if property_refs:
            primary_key = property_refs[0].attrib.get("Name", "").strip()

    schema = []
    for child in entity:
        if _local_name(child.tag) != "Property":
            continue

        column_name = child.attrib.get("Name", "").strip()
        edm_type = child.attrib.get("Type", "").strip()
        schema.append(
            {
                "column_name": column_name,
                "edm_type": edm_type,
                "sql_type": map_edm_to_sql(edm_type),
            }
        )

    return {
        "table_key": table_key_from_name(table_name),
        "table_name": table_name,
        "primary_key": primary_key,
        "schema": schema,
    }


def parse_dataverse_xml(xml_payload: str, requested_tables: list[str]) -> list[dict]:
    """Parse Dataverse metadata XML and return matching EntityType definitions."""
    if not xml_payload or not xml_payload.strip():
        raise ValueError("XML payload is empty.")

    try:
        root = ET.fromstring(xml_payload)
    except ET.ParseError as exc:
        raise ValueError(f"Invalid XML payload: {exc}") from exc

    normalized_requested = normalize_table_names(",".join(requested_tables))
    requested_lookup = {table.casefold() for table in normalized_requested}

    tables = []
    for entity in root.iter():
        if _local_name(entity.tag) != "EntityType":
            continue

        parsed = _parse_entity(entity)
        if requested_lookup and parsed["table_name"].casefold() not in requested_lookup:
            continue
        tables.append(parsed)

    return sorted(tables, key=lambda item: item["table_name"].casefold())
