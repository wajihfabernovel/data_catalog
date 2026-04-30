import unittest

from app import APP_SECTIONS, _filter_catalog_tables, _render_selected_section


class AppNavigationTests(unittest.TestCase):
    def test_render_selected_section_calls_only_requested_renderer(self):
        calls = []
        renderers = {
            section: (lambda section=section: calls.append(section))
            for section in APP_SECTIONS
        }

        _render_selected_section("Relationships", renderers)

        self.assertEqual(calls, ["Relationships"])

    def test_late_sections_are_available_in_main_navigation(self):
        self.assertIn("Relationships", APP_SECTIONS)
        self.assertIn("Modeling Summary", APP_SECTIONS)
        self.assertIn("Batch", APP_SECTIONS)
        self.assertIn("User Journey Mapping", APP_SECTIONS)

    def test_filter_catalog_tables_can_filter_by_signoff_status(self):
        tables = {
            "draft": {
                "table_name": "hive_draft",
                "owning_team": "D&IG",
                "schema": [{"column_name": "id"}],
                "signoff": {"status": "DRAFT"},
            },
            "approved": {
                "table_name": "hive_approved",
                "owning_team": "Strategy",
                "schema": [{"column_name": "id"}],
                "signoff": {"status": "APPROVED"},
            },
            "review": {
                "table_name": "hive_review",
                "owning_team": "D&IG",
                "schema": [{"column_name": "id"}],
                "signoff": {"status": "IN REVIEW"},
            },
        }

        visible = _filter_catalog_tables(
            tables,
            search_query="",
            team_filters=set(),
            column_bucket_filters=set(),
            status_filters={"APPROVED", "IN REVIEW"},
        )

        self.assertEqual([table["table_name"] for table in visible], ["hive_approved", "hive_review"])


if __name__ == "__main__":
    unittest.main()
