import unittest

from services.dataverse_metadata import DataverseMetadataClient
from utils.helpers import (
    DEFAULT_COLUMN_REMARK,
    REMARK_OPTIONS,
    TEAM_OPTIONS,
    build_default_table_state,
    normalize_schema_remarks,
)


class DataverseMetadataClientTests(unittest.TestCase):
    def test_fetch_all_custom_entities_filters_prefix_after_supported_name_discovery(self):
        class SpyClient(DataverseMetadataClient):
            def __init__(self):
                self.get_calls = []
                self.fetched_names = None

            def _get(self, path, params=None):
                self.get_calls.append((path, params or {}))
                return {
                    "value": [
                        {"LogicalName": "new_unrelated"},
                        {"LogicalName": "hive_product"},
                        {"LogicalName": "hive_sample"},
                    ]
                }

            def fetch_entities(self, table_names):
                self.fetched_names = table_names
                return []

        client = SpyClient()

        client.fetch_all_custom_entities(name_prefix="hive_")

        self.assertEqual(client.fetched_names, ["hive_product", "hive_sample"])
        self.assertEqual(
            client.get_calls[0][1]["$filter"],
            "IsCustomEntity eq true",
        )


class CatalogDefaultsTests(unittest.TestCase):
    def test_team_options_include_none(self):
        self.assertIn("None", TEAM_OPTIONS)

    def test_schema_remarks_default_to_maintain(self):
        schema = [
            {"column_name": "owningteam", "edm_type": "LookupType", "sql_type": "UNIQUEIDENTIFIER"},
            {"column_name": "statecode", "remarks": "to be removed"},
        ]

        normalized = normalize_schema_remarks(schema)

        self.assertEqual(REMARK_OPTIONS, ["to be added", "to be removed", "to maintain"])
        self.assertEqual(DEFAULT_COLUMN_REMARK, "to maintain")
        self.assertEqual(normalized[0]["remarks"], DEFAULT_COLUMN_REMARK)
        self.assertEqual(normalized[1]["remarks"], "to be removed")

    def test_default_table_state_adds_schema_remarks(self):
        state = build_default_table_state(
            {
                "table_key": "sample",
                "table_name": "sample",
                "schema": [{"column_name": "owningteam", "edm_type": "LookupType", "sql_type": "UNIQUEIDENTIFIER"}],
            }
        )

        self.assertEqual(state["schema"][0]["remarks"], DEFAULT_COLUMN_REMARK)


if __name__ == "__main__":
    unittest.main()
