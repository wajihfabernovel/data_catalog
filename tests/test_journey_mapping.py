import unittest
from unittest.mock import patch

from services.business_flows import (
    BUSINESS_FLOW_SECTIONS,
    MODELING_RULE_COLUMNS,
    SCENARIO_COLUMNS,
    TABLE_ROLE_COLUMNS,
    available_scrum_teams,
    section_dataframe,
)
from ui.journeys import _render_selected_business_flow_section, render_journey_mapping


class JourneyMappingTests(unittest.TestCase):
    def test_user_journey_mapping_sections_match_business_flow_workbook(self):
        self.assertEqual(
            BUSINESS_FLOW_SECTIONS,
            (
                "Scenarios",
                "Table Roles",
                "End-to-End Summary",
                "Key Modeling Rules",
            ),
        )
        self.assertNotIn("Capture Journey", BUSINESS_FLOW_SECTIONS)
        self.assertNotIn("View Journeys", BUSINESS_FLOW_SECTIONS)
        self.assertNotIn("State Machines", BUSINESS_FLOW_SECTIONS)

    def test_dig_scenarios_match_workbook_shape_and_content(self):
        df = section_dataframe("D&IG", "Scenarios")

        self.assertEqual(list(df.columns), SCENARIO_COLUMNS)
        self.assertEqual(len(df), 22)
        self.assertEqual(df.iloc[0]["Scenario Name"], "Journey -> Discover -> Define/Create -> Idea")
        self.assertIn("dig_journey_participant", df.iloc[0]["Main Tables"])
        self.assertIn("Master data creates instances", set(df["Scenario Name"]))

    def test_dig_table_roles_match_workbook_shape_and_content(self):
        df = section_dataframe("D&IG", "Table Roles")

        self.assertEqual(list(df.columns), TABLE_ROLE_COLUMNS)
        self.assertEqual(len(df), 26)
        self.assertEqual(df.iloc[0]["Target Table"], "dig_app_user")
        self.assertEqual(df.iloc[-1]["Concept"], "Technical Log Event")

    def test_summary_and_rules_are_scoped_by_scrum_team(self):
        self.assertEqual(available_scrum_teams(), ["D&IG"])

        summary = section_dataframe("D&IG", "End-to-End Summary")
        rules = section_dataframe("D&IG", "Key Modeling Rules")

        self.assertEqual(list(rules.columns), MODELING_RULE_COLUMNS)
        self.assertIn("With Journey", set(summary["Path"]))
        self.assertIn("Journey is optional", set(rules["Rule"]))

    def test_render_selected_business_flow_section_displays_requested_table(self):
        with (
            patch("ui.journeys.st.subheader") as subheader,
            patch("ui.journeys._render_flow_table") as render_table,
        ):
            df = _render_selected_business_flow_section("D&IG", "Key Modeling Rules")

        subheader.assert_called_once_with("Key Modeling Rules - D&IG")
        render_table.assert_called_once()
        self.assertEqual(list(df.columns), MODELING_RULE_COLUMNS)

    def test_render_journey_mapping_uses_scrum_team_and_section_controls(self):
        with (
            patch("ui.journeys.st.caption"),
            patch("ui.journeys.st.selectbox", return_value="D&IG") as selectbox,
            patch("ui.journeys._selected_section", return_value="Table Roles") as selected_section,
            patch("ui.journeys._render_selected_business_flow_section") as render_section,
            patch("ui.journeys.st.download_button"),
        ):
            render_section.return_value = section_dataframe("D&IG", "Table Roles")

            render_journey_mapping({}, "tester")

        selectbox.assert_called_once()
        selected_section.assert_called_once_with("Scenarios")
        render_section.assert_called_once_with("D&IG", "Table Roles")


if __name__ == "__main__":
    unittest.main()
