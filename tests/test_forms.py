import unittest

from ui.forms import SCHEMA_DISPLAY_API_COLS, TABLE_FORM_SECTIONS


class FormConfigurationTests(unittest.TestCase):
    def test_schema_api_columns_match_requested_business_view(self):
        self.assertNotIn("source_type", SCHEMA_DISPLAY_API_COLS)
        self.assertNotIn("column_category", SCHEMA_DISPLAY_API_COLS)
        self.assertNotIn("lookup_target", SCHEMA_DISPLAY_API_COLS)
        self.assertIn("target_column_name", SCHEMA_DISPLAY_API_COLS)
        self.assertIn("target_table_name", SCHEMA_DISPLAY_API_COLS)

    def test_removed_metadata_sections_are_not_rendered(self):
        self.assertNotIn("data_quality", TABLE_FORM_SECTIONS)
        self.assertNotIn("migration", TABLE_FORM_SECTIONS)
        self.assertNotIn("state_machine", TABLE_FORM_SECTIONS)


if __name__ == "__main__":
    unittest.main()
