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


def _include_column(column_name: str) -> bool:
    if column_name.startswith("_") and not column_name.startswith("_hive_"):
        return False
    return True


def _extract_relationships(entity: ET.Element, primary_key: str, schema: list[dict]) -> dict:
    guid_columns = {
        column["column_name"]
        for column in schema
        if column["edm_type"] == "Edm.Guid" and column["column_name"] != primary_key
    }
    references_by_fk: dict[str, dict] = {
        column_name: {
            "fk_column": column_name,
            "references_table": "",
            "references_column": "",
            "cardinality": "",
            "mandatory": False,
        }
        for column_name in sorted(guid_columns, key=str.casefold)
    }

    for nav_property in _iter_children(entity, "NavigationProperty"):
        nav_name = nav_property.attrib.get("Name", "").strip()
        nullable = nav_property.attrib.get("Nullable", "").strip().lower()
        mandatory = nullable == "false"
        constraints = _iter_children(nav_property, "ReferentialConstraint")
        for constraint in constraints:
            fk_column = constraint.attrib.get("Property", "").strip()
            if fk_column not in references_by_fk:
                continue

            references_by_fk[fk_column].update(
                {
                    "references_table": nav_name,
                    "references_column": constraint.attrib.get("ReferencedProperty", "").strip(),
                    "cardinality": "Many-to-One",
                    "mandatory": mandatory,
                }
            )

    return {
        "references": list(references_by_fk.values()),
        "referenced_by": [],
    }


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
        if not _include_column(column_name):
            continue
        edm_type = child.attrib.get("Type", "").strip()
        schema.append(
            {
                "column_name": column_name,
                "edm_type": edm_type,
                "sql_type": map_edm_to_sql(edm_type),
            }
        )

    relationships = _extract_relationships(entity, primary_key, schema)

    return {
        "table_key": table_key_from_name(table_name),
        "table_name": table_name,
        "primary_key": primary_key,
        "schema": schema,
        "relationships": relationships,
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
