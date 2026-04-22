"""SharePoint list access through Microsoft Graph."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import requests

from utils.helpers import normalize_graph_item, parse_date, serialize_date, table_key_from_name


class SharePointConfigError(RuntimeError):
    """Raised when required SharePoint configuration is missing."""


class SharePointService:
    """Thin Graph client for SharePoint-backed metadata storage."""

    graph_base_url = "https://graph.microsoft.com/v1.0"

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.hostname = os.getenv("SHAREPOINT_HOSTNAME", "").strip()
        self.site_path = os.getenv("SHAREPOINT_SITE_PATH", "").strip()
        self.tables_list_name = os.getenv("SHAREPOINT_TABLES_LIST_NAME", "CatalogTables").strip()
        self.columns_list_name = os.getenv("SHAREPOINT_COLUMNS_LIST_NAME", "CatalogColumns").strip()

        missing = []
        if not self.hostname:
            missing.append("SHAREPOINT_HOSTNAME")
        if not self.site_path:
            missing.append("SHAREPOINT_SITE_PATH")
        if missing:
            raise SharePointConfigError(
                "Missing SharePoint configuration: " + ", ".join(missing)
            )

        self._site_id: str | None = None
        self._list_ids: dict[str, str] = {}

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, **kwargs) -> dict:
        response = requests.request(method, url, headers=self.headers, timeout=30, **kwargs)
        if not response.ok:
            try:
                payload = response.json()
                error = payload.get("error", {})
                message = error.get("message") or response.text
            except ValueError:
                message = response.text
            raise RuntimeError(f"SharePoint request failed ({response.status_code}): {message}")
        if response.status_code == 204:
            return {}
        return response.json()

    def _site_resource_path(self) -> str:
        path = self.site_path if self.site_path.startswith("/") else f"/{self.site_path}"
        return f"{self.hostname}:{path}"

    def get_site_id(self) -> str:
        if self._site_id:
            return self._site_id
        url = f"{self.graph_base_url}/sites/{self._site_resource_path()}"
        payload = self._request("GET", url)
        self._site_id = payload["id"]
        return self._site_id

    def get_list_id(self, list_name: str) -> str:
        if list_name in self._list_ids:
            return self._list_ids[list_name]

        site_id = self.get_site_id()
        url = f"{self.graph_base_url}/sites/{site_id}/lists"
        payload = self._request("GET", url)
        for item in payload.get("value", []):
            if item.get("displayName") == list_name:
                self._list_ids[list_name] = item["id"]
                return item["id"]
        raise RuntimeError(f"SharePoint list '{list_name}' was not found on the configured site.")

    def _list_items(self, list_name: str, select_fields: list[str]) -> list[dict]:
        site_id = self.get_site_id()
        list_id = self.get_list_id(list_name)
        select_clause = ",".join(select_fields)
        url = (
            f"{self.graph_base_url}/sites/{site_id}/lists/{list_id}/items"
            f"?$expand=fields($select={select_clause})&$top=999"
        )
        items = []
        while url:
            payload = self._request("GET", url)
            items.extend(normalize_graph_item(item) for item in payload.get("value", []))
            url = payload.get("@odata.nextLink")
        return items

    def fetch_catalog_state(self) -> dict[str, dict]:
        table_items = self._list_items(
            self.tables_list_name,
            [
                "TableKey",
                "TableName",
                "PrimaryKey",
                "ExtractByPipeline",
                "DeltaExtractionColumn",
                "FeedPowerBI",
                "KeyMetricsOrDimensions",
                "WritePath",
                "NullableIssues",
                "FormatInconsistencies",
                "DuplicateRecords",
                "OrphanRecords",
                "HardDeleteInUse",
                "OverallQualityRating",
                "QualityNotes",
                "Recommendation",
                "MergeWith",
                "SplitInto",
                "ReplacedBy",
                "MissingColumns",
                "MissingConstraints",
                "CompletedBy",
                "ReviewedBy",
                "ReviewedByBusiness",
                "Status",
                "DateApproved",
                "Notes",
                "LastSyncedAt",
                "LastModifiedBy",
            ],
        )
        column_items = self._list_items(
            self.columns_list_name,
            [
                "TableKey",
                "TableName",
                "ColumnName",
                "EdmType",
                "SqlType",
            ],
        )

        catalog_state: dict[str, dict] = {}
        for item in table_items:
            fields = item["fields"]
            table_key = fields.get("TableKey") or table_key_from_name(fields.get("TableName", ""))
            if not table_key:
                continue
            catalog_state[table_key] = {
                "item_id": item["item_id"],
                "table_key": table_key,
                "table_name": fields.get("TableName", ""),
                "primary_key": fields.get("PrimaryKey", ""),
                "schema": [],
                "data_quality": {
                    "nullable_issues": fields.get("NullableIssues", ""),
                    "format_inconsistencies": fields.get("FormatInconsistencies", ""),
                    "duplicate_records": fields.get("DuplicateRecords", "NO"),
                    "orphan_records": fields.get("OrphanRecords", "NO"),
                    "hard_delete_in_use": fields.get("HardDeleteInUse", "NO"),
                    "overall_quality_rating": fields.get("OverallQualityRating", "ACCEPTABLE"),
                    "quality_notes": fields.get("QualityNotes", ""),
                },
                "pipeline": {
                    "extract_by_pipeline": fields.get("ExtractByPipeline", "UNSURE"),
                    "delta_extraction_column": fields.get("DeltaExtractionColumn", ""),
                    "feed_power_bi": fields.get("FeedPowerBI", "UNSURE"),
                    "key_metrics_or_dimensions": fields.get("KeyMetricsOrDimensions", ""),
                    "write_path": fields.get("WritePath", "UNKNOWN"),
                },
                "target_model": {
                    "recommendation": fields.get("Recommendation", "KEEP AS IS"),
                    "merge_with": fields.get("MergeWith", ""),
                    "split_into": fields.get("SplitInto", ""),
                    "replaced_by": fields.get("ReplacedBy", ""),
                    "missing_columns": fields.get("MissingColumns", ""),
                    "missing_constraints": fields.get("MissingConstraints", ""),
                },
                "signoff": {
                    "completed_by": fields.get("CompletedBy", ""),
                    "reviewed_by": fields.get("ReviewedBy", ""),
                    "reviewed_by_business": fields.get("ReviewedByBusiness", ""),
                    "status": fields.get("Status", "DRAFT"),
                    "date_approved": parse_date(fields.get("DateApproved")),
                    "notes": fields.get("Notes", ""),
                },
            }

        for item in column_items:
            fields = item["fields"]
            table_key = fields.get("TableKey") or table_key_from_name(fields.get("TableName", ""))
            if table_key not in catalog_state:
                catalog_state[table_key] = {
                    "item_id": None,
                    "table_key": table_key,
                    "table_name": fields.get("TableName", ""),
                    "primary_key": "",
                    "schema": [],
                    "data_quality": {},
                    "pipeline": {},
                    "target_model": {},
                    "signoff": {},
                }
            catalog_state[table_key].setdefault("schema", []).append(
                {
                    "item_id": item["item_id"],
                    "column_name": fields.get("ColumnName", ""),
                    "edm_type": fields.get("EdmType", ""),
                    "sql_type": fields.get("SqlType", ""),
                }
            )

        for table in catalog_state.values():
            table["schema"] = sorted(table.get("schema", []), key=lambda col: col["column_name"].casefold())

        return catalog_state

    def _create_list_item(self, list_name: str, fields: dict) -> dict:
        site_id = self.get_site_id()
        list_id = self.get_list_id(list_name)
        url = f"{self.graph_base_url}/sites/{site_id}/lists/{list_id}/items"
        return self._request("POST", url, json={"fields": fields})

    def _update_list_item(self, list_name: str, item_id: str, fields: dict) -> dict:
        site_id = self.get_site_id()
        list_id = self.get_list_id(list_name)
        url = f"{self.graph_base_url}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
        return self._request("PATCH", url, json=fields)

    def save_tables(self, tables: list[dict], user_display_name: str) -> None:
        existing_state = self.fetch_catalog_state()
        existing_columns_by_table = {
            key: {column["column_name"]: column for column in value.get("schema", [])}
            for key, value in existing_state.items()
        }

        timestamp = datetime.now(timezone.utc).isoformat()
        for table in tables:
            table_key = table["table_key"]
            existing_table = existing_state.get(table_key)
            table_fields = {
                "Title": table["table_name"],
                "TableKey": table_key,
                "TableName": table["table_name"],
                "PrimaryKey": table.get("primary_key", ""),
                "ExtractByPipeline": table["pipeline"]["extract_by_pipeline"],
                "DeltaExtractionColumn": table["pipeline"]["delta_extraction_column"],
                "FeedPowerBI": table["pipeline"]["feed_power_bi"],
                "KeyMetricsOrDimensions": table["pipeline"]["key_metrics_or_dimensions"],
                "WritePath": table["pipeline"]["write_path"],
                "NullableIssues": table["data_quality"]["nullable_issues"],
                "FormatInconsistencies": table["data_quality"]["format_inconsistencies"],
                "DuplicateRecords": table["data_quality"]["duplicate_records"],
                "OrphanRecords": table["data_quality"]["orphan_records"],
                "HardDeleteInUse": table["data_quality"]["hard_delete_in_use"],
                "OverallQualityRating": table["data_quality"]["overall_quality_rating"],
                "QualityNotes": table["data_quality"]["quality_notes"],
                "Recommendation": table["target_model"]["recommendation"],
                "MergeWith": table["target_model"]["merge_with"],
                "SplitInto": table["target_model"]["split_into"],
                "ReplacedBy": table["target_model"]["replaced_by"],
                "MissingColumns": table["target_model"]["missing_columns"],
                "MissingConstraints": table["target_model"]["missing_constraints"],
                "CompletedBy": table["signoff"]["completed_by"],
                "ReviewedBy": table["signoff"]["reviewed_by"],
                "ReviewedByBusiness": table["signoff"]["reviewed_by_business"],
                "Status": table["signoff"]["status"],
                "DateApproved": serialize_date(table["signoff"]["date_approved"]),
                "Notes": table["signoff"]["notes"],
                "LastSyncedAt": timestamp,
                "LastModifiedBy": user_display_name,
            }

            if existing_table and existing_table.get("item_id"):
                self._update_list_item(self.tables_list_name, existing_table["item_id"], table_fields)
            else:
                created = self._create_list_item(self.tables_list_name, table_fields)
                table["item_id"] = created.get("id")

            existing_columns = existing_columns_by_table.get(table_key, {})
            for column in table.get("schema", []):
                column_fields = {
                    "Title": f"{table['table_name']}::{column['column_name']}",
                    "TableKey": table_key,
                    "TableName": table["table_name"],
                    "ColumnName": column["column_name"],
                    "EdmType": column["edm_type"],
                    "SqlType": column["sql_type"],
                }
                existing_column = existing_columns.get(column["column_name"])
                if existing_column and existing_column.get("item_id"):
                    self._update_list_item(
                        self.columns_list_name,
                        existing_column["item_id"],
                        column_fields,
                    )
                else:
                    self._create_list_item(self.columns_list_name, column_fields)
