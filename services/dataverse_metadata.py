"""Dataverse metadata ingestion via the Web API."""

from __future__ import annotations

import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

from models.mappings import map_dataverse_attribute_to_sql
from utils.helpers import normalize_table_names, table_key_from_name

load_dotenv()


class DataverseConfigError(RuntimeError):
    """Raised when Dataverse configuration is missing."""


def load_dataverse_config() -> dict[str, Any]:
    base_url = os.getenv("DATAVERSE_BASE_URL", "").strip().rstrip("/")
    tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()
    client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
    client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()
    username = os.getenv("DATAVERSE_USERNAME", "").strip()
    password = os.getenv("DATAVERSE_PASSWORD", "").strip()
    missing = []
    if not base_url:
        missing.append("DATAVERSE_BASE_URL")
    if not tenant_id:
        missing.append("AZURE_TENANT_ID")
    if not client_id:
        missing.append("AZURE_CLIENT_ID")

    has_client_credentials = bool(client_secret)
    has_password_flow = bool(username and password)
    if not has_client_credentials and not has_password_flow:
        missing.append("AZURE_CLIENT_SECRET or DATAVERSE_USERNAME+DATAVERSE_PASSWORD")
    return {
        "base_url": base_url,
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "auth_mode": "client_credentials" if has_client_credentials else "password",
        "missing": missing,
    }


class DataverseMetadataClient:
    """Fetch and normalize Dataverse entity attribute metadata."""

    def __init__(self) -> None:
        config = load_dataverse_config()
        if config["missing"]:
            raise DataverseConfigError(
                "Missing Dataverse configuration: " + ", ".join(config["missing"])
            )
        self.config = config
        self.session = requests.Session()
        self._access_token: str | None = None

    def _token(self) -> str:
        if self._access_token:
            return self._access_token
        if self.config["auth_mode"] == "client_credentials":
            token_url = f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/v2.0/token"
            response = self.session.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.config["client_id"],
                    "client_secret": self.config["client_secret"],
                    "scope": f"{self.config['base_url']}/.default",
                },
                timeout=30,
            )
        else:
            token_url = f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/token"
            response = self.session.post(
                token_url,
                data={
                    "grant_type": "password",
                    "client_id": self.config["client_id"],
                    "resource": self.config["base_url"],
                    "username": self.config["username"],
                    "password": self.config["password"],
                },
                timeout=30,
            )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload["access_token"]
        return self._access_token

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.session.get(
            f"{self.config['base_url']}/api/data/v9.2/{path}",
            headers={
                "Authorization": f"Bearer {self._token()}",
                "Accept": "application/json",
                "OData-Version": "4.0",
                "OData-MaxVersion": "4.0",
            },
            params=params or {},
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _safe_value(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _source_type_label(source_type: Any) -> str:
        mapping = {"0": "Base", "1": "Calculated", "2": "Rollup", "3": "Formula"}
        return mapping.get(str(source_type), "")

    @staticmethod
    def _format_option_values(options: list[dict[str, Any]]) -> str:
        values = []
        for option in options or []:
            label = (
                ((option.get("Label") or {}).get("UserLocalizedLabel") or {}).get("Label")
                or ""
            )
            value = option.get("Value")
            if label or value is not None:
                values.append(f"{value}={label}" if label else str(value))
        return "; ".join(values)

    @staticmethod
    def _attribute_category(attr: dict[str, Any]) -> str:
        logical_name = attr.get("logical_name", "")
        if attr.get("is_primary_id"):
            return "Primary Key"
        if attr.get("attribute_type") in {"Lookup", "Owner", "Customer"}:
            return "Lookup / FK"
        if attr.get("attribute_type") == "MultiSelectPicklist":
            return "MultiSelect"
        if attr.get("source_type") == 2:
            return "Rollup"
        if attr.get("source_type") == 3:
            return "Formula"
        if attr.get("is_custom_attribute") and attr.get("is_valid_odata_attribute"):
            return "Custom Business"
        if (attr.get("is_valid_odata_attribute") is False
                or logical_name.endswith("name")
                or logical_name.endswith("yominame")):
            return "Virtual / Shadow"
        return "System"

    @staticmethod
    def _modeling_action(attr: dict[str, Any]) -> str:
        category = attr.get("category", "")
        if category == "Rollup":
            return "Model as dbt metric, do not persist"
        if category == "Formula":
            return "Model as dbt expression, do not persist"
        if category == "Lookup / FK":
            return "Persist as FK"
        if category == "MultiSelect":
            return "Consider junction table or NVARCHAR(MAX)"
        if category == "Virtual / Shadow":
            return "Drop from target model"
        return "Persist"

    @staticmethod
    def _is_state_machine_candidate(attr: dict[str, Any]) -> bool:
        return attr.get("attribute_type") in {"Picklist", "State", "Status"} or (
            attr.get("logical_name", "").casefold() in {"statecode", "statuscode"}
        )

    def _fetch_base_attributes(self, table_name: str) -> list[dict[str, Any]]:
        payload = self._get(
            f"EntityDefinitions(LogicalName='{table_name}')/Attributes",
            params={
                "$select": ",".join(
                    [
                        "LogicalName",
                        "AttributeType",
                        "AttributeTypeName",
                        "IsCustomAttribute",
                        "IsValidODataAttribute",
                        "IsPrimaryId",
                        "IsPrimaryName",
                        "IsLogical",
                        "RequiredLevel",
                        "SourceType",
                    ]
                )
            },
        )
        return payload.get("value", [])

    def _fetch_all_custom_entities_expanded(self) -> list[dict[str, Any]]:
        payload = self._get(
            "EntityDefinitions",
            params={
                "$select": "LogicalName,DisplayName,PrimaryIdAttribute",
                "$expand": (
                    "Attributes($select="
                    "LogicalName,DisplayName,AttributeType,AttributeTypeName,IsPrimaryId,"
                    "IsPrimaryName,IsCustomAttribute,IsValidODataAttribute,IsLogical,RequiredLevel,SourceType)"
                ),
                "$filter": "IsCustomEntity eq true",
            },
        )
        return payload.get("value", [])

    def _fetch_lookup_metadata(self, table_name: str) -> dict[str, dict[str, Any]]:
        payload = self._get(
            f"EntityDefinitions(LogicalName='{table_name}')/Attributes/Microsoft.Dynamics.CRM.LookupAttributeMetadata",
            params={"$select": "LogicalName,Targets"},
        )
        return {row["LogicalName"]: row for row in payload.get("value", []) if row.get("LogicalName")}

    def _fetch_string_metadata(self, table_name: str) -> dict[str, dict[str, Any]]:
        payload = self._get(
            f"EntityDefinitions(LogicalName='{table_name}')/Attributes/Microsoft.Dynamics.CRM.StringAttributeMetadata",
            params={"$select": "LogicalName,MaxLength"},
        )
        return {row["LogicalName"]: row for row in payload.get("value", []) if row.get("LogicalName")}

    def _fetch_picklist_metadata(self, table_name: str) -> dict[str, dict[str, Any]]:
        payload = self._get(
            f"EntityDefinitions(LogicalName='{table_name}')"
            "/Attributes/Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
            params={"$select": "LogicalName,DefaultFormValue", "$expand": "OptionSet($select=Options)"},
        )
        return {row["LogicalName"]: row for row in payload.get("value", []) if row.get("LogicalName")}

    def _fetch_custom_entity_names(self) -> set[str]:
        payload = self._get(
            "EntityDefinitions",
            params={"$select": "LogicalName", "$filter": "IsCustomEntity eq true"},
        )
        return {
            self._safe_value(row.get("LogicalName"))
            for row in payload.get("value", [])
            if row.get("LogicalName")
        }

    def _fetch_decimal_metadata(self, table_name: str) -> dict[str, dict[str, Any]]:
        payload = self._get(
            f"EntityDefinitions(LogicalName='{table_name}')"
            "/Attributes/Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            params={"$select": "LogicalName,Precision,MinValue,MaxValue"},
        )
        return {row["LogicalName"]: row for row in payload.get("value", []) if row.get("LogicalName")}

    def _fetch_one_to_many_relationships(self) -> list[dict[str, Any]]:
        payload = self._get(
            "RelationshipDefinitions/Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            params={
                "$select": ",".join(
                    [
                        "SchemaName",
                        "ReferencedEntity",
                        "ReferencedAttribute",
                        "ReferencingEntity",
                        "ReferencingAttribute",
                        "ReferencingEntityNavigationPropertyName",
                        "ReferencedEntityNavigationPropertyName",
                    ]
                )
            },
        )
        return payload.get("value", [])

    def _fetch_many_to_many_relationships(self) -> list[dict[str, Any]]:
        payload = self._get(
            "RelationshipDefinitions/Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata",
            params={
                "$select": ",".join(
                    [
                        "SchemaName",
                        "Entity1LogicalName",
                        "Entity1IntersectAttribute",
                        "Entity2LogicalName",
                        "Entity2IntersectAttribute",
                        "IntersectEntityName",
                    ]
                )
            },
        )
        return payload.get("value", [])

    @staticmethod
    def _required_level_label(required_level: Any) -> str:
        if isinstance(required_level, dict):
            return str(required_level.get("Value") or "").strip()
        return str(required_level or "").strip()

    @staticmethod
    def _display_label(display_name: Any) -> str:
        if not isinstance(display_name, dict):
            return ""
        return str(
            ((display_name.get("UserLocalizedLabel") or {}).get("Label"))
            or ""
        ).strip()

    def _build_entity_profile(
        self,
        *,
        table_name: str,
        base_attributes: list[dict[str, Any]],
        lookup_meta: dict[str, dict[str, Any]],
        string_meta: dict[str, dict[str, Any]],
        picklist_meta: dict[str, dict[str, Any]],
        decimal_meta: dict[str, dict[str, Any]],
        custom_entity_names: set[str] | None = None,
    ) -> dict[str, Any]:
        schema: list[dict[str, Any]] = []
        primary_key = ""
        relationships = {"references": [], "referenced_by": []}
        counts = Counter()

        for row in base_attributes:
            logical_name = self._safe_value(row.get("LogicalName"))
            attribute_type = self._safe_value(row.get("AttributeType"))
            attribute_type_name = self._safe_value(
                (row.get("AttributeTypeName") or {}).get("Value")
                if isinstance(row.get("AttributeTypeName"), dict)
                else row.get("AttributeTypeName")
            )
            max_length = string_meta.get(logical_name, {}).get("MaxLength")
            precision = decimal_meta.get(logical_name, {}).get("Precision")
            option_values = self._format_option_values(
                ((picklist_meta.get(logical_name, {}) or {}).get("OptionSet") or {}).get("Options") or []
            )
            attr = {
                "column_name": logical_name,
                "display_name": self._display_label(row.get("DisplayName")),
                "edm_type": attribute_type_name or attribute_type,
                "attribute_type": attribute_type,
                "attribute_type_name": attribute_type_name,
                "is_custom_attribute": bool(row.get("IsCustomAttribute")),
                "is_valid_odata_attribute": bool(row.get("IsValidODataAttribute")),
                "is_logical": bool(row.get("IsLogical")),
                "required_level": self._required_level_label(row.get("RequiredLevel")),
                "source_type": row.get("SourceType"),
                "source_type_label": self._source_type_label(row.get("SourceType")),
                "max_length": max_length if max_length is not None else "",
                "precision": precision if precision is not None else "",
                "min_value": self._safe_value(decimal_meta.get(logical_name, {}).get("MinValue")),
                "max_value": self._safe_value(decimal_meta.get(logical_name, {}).get("MaxValue")),
                "targets": ", ".join((lookup_meta.get(logical_name, {}) or {}).get("Targets") or []),
                "option_values": option_values,
                "is_primary_id": bool(row.get("IsPrimaryId")),
                "is_primary_name": bool(row.get("IsPrimaryName")),
            }
            attr["sql_type"] = map_dataverse_attribute_to_sql(
                attribute_type,
                max_length=max_length if isinstance(max_length, int) else None,
                precision=precision if isinstance(precision, int) else None,
                is_primary_id=attr["is_primary_id"],
            )
            attr["category"] = self._attribute_category({"logical_name": logical_name, **attr})
            attr["modeling_action"] = self._modeling_action(attr)
            attr["is_state_machine_candidate"] = self._is_state_machine_candidate(attr)
            schema.append(attr)
            counts[attr["category"]] += 1

            if attr["is_primary_id"]:
                primary_key = logical_name
            if attribute_type in {"Lookup", "Owner", "Customer"}:
                raw_targets = [t.strip() for t in attr["targets"].split(",") if t.strip()] if attr["targets"] else []
                if custom_entity_names is not None and not any(t in custom_entity_names for t in raw_targets):
                    continue
                relationships["references"].append(
                    {
                        "fk_column": logical_name,
                        "references_table": attr["targets"],
                        "references_column": "",
                        "cardinality": "Many-to-One",
                        "mandatory": attr["required_level"] in {"SystemRequired", "ApplicationRequired"},
                    }
                )

        target_entity = f"PLM_{table_name.upper()}"
        lookup_count = counts["Lookup / FK"]
        metadata_profile = {
            "source_mode": "dataverse_api",
            "api_enriched_at": datetime.now(timezone.utc).isoformat(),
            "total_attributes": len(schema),
            "custom_business_columns": counts["Custom Business"],
            "system_columns": counts["System"],
            "virtual_shadow_columns": counts["Virtual / Shadow"],
            "rollup_fields": counts["Rollup"],
            "formula_fields": counts["Formula"],
            "lookup_columns": lookup_count,
            "multiselect_columns": counts["MultiSelect"],
            "state_machine_candidates": ", ".join(
                sorted(
                    [
                        column["column_name"]
                        for column in schema
                        if column.get("is_state_machine_candidate")
                    ]
                )
            ),
            "recommended_target_entity": target_entity,
            "centrality_score": "HIGH" if lookup_count >= 10 else "MEDIUM" if lookup_count >= 3 else "LOW",
            "migration_priority": "P0 - Critical" if lookup_count >= 10 else "P1 - High",
            "notes": (
                "Central domain aggregate candidate with broad FK dependency graph"
                if lookup_count >= 10
                else "Review lookup and computed fields for target modeling"
            ),
        }
        return {
            "table_key": table_key_from_name(table_name),
            "table_name": table_name,
            "primary_key": primary_key,
            "schema": sorted(schema, key=lambda item: item["column_name"].casefold()),
            "relationships": relationships,
            "metadata_profile": metadata_profile,
        }

    def fetch_entity_profile(self, table_name: str) -> dict[str, Any]:
        base_attributes = self._fetch_base_attributes(table_name)
        lookup_meta = self._fetch_lookup_metadata(table_name)
        string_meta = self._fetch_string_metadata(table_name)
        picklist_meta = self._fetch_picklist_metadata(table_name)
        decimal_meta = self._fetch_decimal_metadata(table_name)
        custom_entity_names = self._fetch_custom_entity_names()
        return self._build_entity_profile(
            table_name=table_name,
            base_attributes=base_attributes,
            lookup_meta=lookup_meta,
            string_meta=string_meta,
            picklist_meta=picklist_meta,
            decimal_meta=decimal_meta,
            custom_entity_names=custom_entity_names,
        )

    def fetch_entities(self, table_names: list[str]) -> list[dict[str, Any]]:
        normalized = normalize_table_names(",".join(table_names))
        custom_entity_names = self._fetch_custom_entity_names()
        return [
            self._build_entity_profile(
                table_name=name,
                base_attributes=self._fetch_base_attributes(name),
                lookup_meta=self._fetch_lookup_metadata(name),
                string_meta=self._fetch_string_metadata(name),
                picklist_meta=self._fetch_picklist_metadata(name),
                decimal_meta=self._fetch_decimal_metadata(name),
                custom_entity_names=custom_entity_names,
            )
            for name in normalized
        ]

    def fetch_all_custom_entities(self) -> list[dict[str, Any]]:
        expanded = self._fetch_all_custom_entities_expanded()
        entities: list[dict[str, Any]] = []
        entity_names = {
            self._safe_value(row.get("LogicalName"))
            for row in expanded
            if self._safe_value(row.get("LogicalName"))
        }
        referenced_by: dict[str, list[dict[str, Any]]] = defaultdict(list)
        many_to_many_by_entity: dict[str, list[dict[str, Any]]] = defaultdict(list)
        one_to_many_rows = self._fetch_one_to_many_relationships()
        many_to_many_rows = self._fetch_many_to_many_relationships()

        for row in expanded:
            table_name = self._safe_value(row.get("LogicalName"))
            if not table_name:
                continue
            entity = self._build_entity_profile(
                table_name=table_name,
                base_attributes=row.get("Attributes") or [],
                lookup_meta=self._fetch_lookup_metadata(table_name),
                string_meta=self._fetch_string_metadata(table_name),
                picklist_meta=self._fetch_picklist_metadata(table_name),
                decimal_meta=self._fetch_decimal_metadata(table_name),
                custom_entity_names=entity_names,
            )
            entity["display_name"] = self._display_label(row.get("DisplayName")) or table_name
            entity["primary_key"] = entity.get("primary_key") or self._safe_value(row.get("PrimaryIdAttribute"))
            entities.append(entity)

        entities_by_name = {entity["table_name"]: entity for entity in entities}

        for row in one_to_many_rows:
            referenced = self._safe_value(row.get("ReferencedEntity"))
            referencing = self._safe_value(row.get("ReferencingEntity"))
            if referenced not in entity_names and referencing not in entity_names:
                continue
            if referencing in entities_by_name and referenced in entity_names:
                entities_by_name[referencing]["relationships"]["references"].append(
                    {
                        "fk_column": self._safe_value(row.get("ReferencingAttribute")),
                        "references_table": referenced,
                        "references_column": self._safe_value(row.get("ReferencedAttribute")),
                        "cardinality": "Many-to-One",
                        "mandatory": False,
                    }
                )
            if referenced in entity_names and referencing in entity_names:
                referenced_by[referenced].append(
                    {
                        "table_name": referencing,
                        "via_column": self._safe_value(row.get("ReferencingAttribute")),
                        "cardinality": "One-to-Many",
                    }
                )

        for row in many_to_many_rows:
            entity1 = self._safe_value(row.get("Entity1LogicalName"))
            entity2 = self._safe_value(row.get("Entity2LogicalName"))
            if entity1 not in entity_names and entity2 not in entity_names:
                continue
            entry = {
                "schema_name": self._safe_value(row.get("SchemaName")),
                "entity1": entity1,
                "entity2": entity2,
                "intersect_entity_name": self._safe_value(row.get("IntersectEntityName")),
                "entity1_intersect_attribute": self._safe_value(row.get("Entity1IntersectAttribute")),
                "entity2_intersect_attribute": self._safe_value(row.get("Entity2IntersectAttribute")),
            }
            if entity1 in entity_names:
                many_to_many_by_entity[entity1].append(entry)
            if entity2 in entity_names:
                many_to_many_by_entity[entity2].append(entry)

        for entity in entities:
            entity["relationships"]["referenced_by"] = sorted(
                referenced_by.get(entity["table_name"], []),
                key=lambda item: (item.get("table_name", "").casefold(), item.get("via_column", "").casefold()),
            )
            # Deduplicate relationship references after merging inferred and official metadata.
            seen_refs: set[tuple[str, str, str]] = set()
            deduped_refs = []
            for ref in entity["relationships"]["references"]:
                key = (
                    self._safe_value(ref.get("fk_column")),
                    self._safe_value(ref.get("references_table")),
                    self._safe_value(ref.get("references_column")),
                )
                if key in seen_refs:
                    continue
                seen_refs.add(key)
                deduped_refs.append(ref)
            entity["relationships"]["references"] = sorted(
                deduped_refs,
                key=lambda item: (
                    self._safe_value(item.get("references_table")).casefold(),
                    self._safe_value(item.get("fk_column")).casefold(),
                ),
            )
            entity["metadata_profile"]["incoming_relationships"] = len(entity["relationships"]["referenced_by"])
            entity["metadata_profile"]["outgoing_relationships"] = len(entity["relationships"]["references"])
            entity["metadata_profile"]["many_to_many"] = many_to_many_by_entity.get(entity["table_name"], [])
        return sorted(entities, key=lambda item: item["table_name"].casefold())
