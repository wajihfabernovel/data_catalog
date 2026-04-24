"""Dataverse metadata ingestion via the Web API."""

from __future__ import annotations

import os
from collections import Counter
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
        if attr.get("is_valid_odata_attribute") is False or logical_name.endswith("name") or logical_name.endswith("yominame"):
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
                        "SourceType",
                    ]
                )
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
            f"EntityDefinitions(LogicalName='{table_name}')/Attributes/Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
            params={"$select": "LogicalName,DefaultFormValue", "$expand": "OptionSet($select=Options)"},
        )
        return {row["LogicalName"]: row for row in payload.get("value", []) if row.get("LogicalName")}

    def _fetch_decimal_metadata(self, table_name: str) -> dict[str, dict[str, Any]]:
        payload = self._get(
            f"EntityDefinitions(LogicalName='{table_name}')/Attributes/Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
            params={"$select": "LogicalName,Precision,MinValue,MaxValue"},
        )
        return {row["LogicalName"]: row for row in payload.get("value", []) if row.get("LogicalName")}

    def fetch_entity_profile(self, table_name: str) -> dict[str, Any]:
        base_attributes = self._fetch_base_attributes(table_name)
        lookup_meta = self._fetch_lookup_metadata(table_name)
        string_meta = self._fetch_string_metadata(table_name)
        picklist_meta = self._fetch_picklist_metadata(table_name)
        decimal_meta = self._fetch_decimal_metadata(table_name)

        schema: list[dict[str, Any]] = []
        primary_key = ""
        relationships = {"references": [], "referenced_by": []}
        counts = Counter()

        for row in base_attributes:
            logical_name = self._safe_value(row.get("LogicalName"))
            attribute_type = self._safe_value(row.get("AttributeType"))
            attribute_type_name = self._safe_value(
                (row.get("AttributeTypeName") or {}).get("Value")
            )
            max_length = string_meta.get(logical_name, {}).get("MaxLength")
            precision = decimal_meta.get(logical_name, {}).get("Precision")
            option_values = self._format_option_values(
                ((picklist_meta.get(logical_name, {}) or {}).get("OptionSet") or {}).get("Options") or []
            )
            attr = {
                "column_name": logical_name,
                "edm_type": attribute_type_name or attribute_type,
                "attribute_type": attribute_type,
                "attribute_type_name": attribute_type_name,
                "is_custom_attribute": bool(row.get("IsCustomAttribute")),
                "is_valid_odata_attribute": bool(row.get("IsValidODataAttribute")),
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
                relationships["references"].append(
                    {
                        "fk_column": logical_name,
                        "references_table": attr["targets"],
                        "references_column": "",
                        "cardinality": "Many-to-One",
                        "mandatory": False,
                    }
                )

        target_entity = f"PLM_{table_name.upper()}"
        metadata_profile = {
            "source_mode": "dataverse_api",
            "api_enriched_at": datetime.now(timezone.utc).isoformat(),
            "total_attributes": len(schema),
            "custom_business_columns": counts["Custom Business"],
            "system_columns": counts["System"],
            "virtual_shadow_columns": counts["Virtual / Shadow"],
            "rollup_fields": counts["Rollup"],
            "formula_fields": counts["Formula"],
            "lookup_columns": counts["Lookup / FK"],
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
            "migration_priority": "P0 - Critical" if counts["Lookup / FK"] >= 5 else "P1 - High",
            "notes": (
                "Central domain aggregate candidate"
                if counts["Lookup / FK"] >= 5
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

    def fetch_entities(self, table_names: list[str]) -> list[dict[str, Any]]:
        normalized = normalize_table_names(",".join(table_names))
        return [self.fetch_entity_profile(name) for name in normalized]
