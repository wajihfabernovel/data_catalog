import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from services.business_flows import (
    BUSINESS_FLOW_SECTIONS,
    EDITABLE_ROW_ID,
    EDITABLE_SCOPE_COLUMN,
    MODELING_RULE_COLUMNS,
    SCENARIO_COLUMNS,
    TABLE_ROLE_COLUMNS,
    available_scrum_teams,
    build_initial_templates,
    filter_template_rows,
    load_template_draft,
    merge_editor_rows,
    save_template_draft,
    section_dataframe,
)
from ui.journeys import TEMPLATES_STATE_KEY, _ensure_template_state, _render_template_editor, render_journey_mapping


class NoopContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


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

    def test_initial_templates_are_editable_and_scoped_to_dig(self):
        templates = build_initial_templates()
        scenario = templates["Scenarios"][0]

        self.assertIn(EDITABLE_ROW_ID, scenario)
        self.assertEqual(scenario[EDITABLE_SCOPE_COLUMN], "D&IG")
        self.assertEqual(scenario["Scenario Name"], "Journey -> Discover -> Define/Create -> Idea")

    def test_filter_template_rows_supports_all_global_and_team_scope(self):
        rows = [
            {EDITABLE_ROW_ID: "1", EDITABLE_SCOPE_COLUMN: "", "Scenario Name": "Global"},
            {EDITABLE_ROW_ID: "2", EDITABLE_SCOPE_COLUMN: "D&IG", "Scenario Name": "D&IG row"},
            {EDITABLE_ROW_ID: "3", EDITABLE_SCOPE_COLUMN: "Finance", "Scenario Name": "Finance row"},
        ]

        self.assertEqual([r["Scenario Name"] for r in filter_template_rows(rows, "All templates")], ["Global", "D&IG row", "Finance row"])
        self.assertEqual([r["Scenario Name"] for r in filter_template_rows(rows, "Global / unassigned")], ["Global"])
        self.assertEqual([r["Scenario Name"] for r in filter_template_rows(rows, "D&IG")], ["Global", "D&IG row"])

    def test_merge_editor_rows_updates_deletes_and_preserves_hidden_rows(self):
        current_rows = [
            {EDITABLE_ROW_ID: "keep", EDITABLE_SCOPE_COLUMN: "Finance", "Scenario Name": "Hidden"},
            {EDITABLE_ROW_ID: "edit", EDITABLE_SCOPE_COLUMN: "D&IG", "Scenario Name": "Old"},
            {EDITABLE_ROW_ID: "delete", EDITABLE_SCOPE_COLUMN: "D&IG", "Scenario Name": "Delete me"},
        ]
        edited_rows = [
            {EDITABLE_ROW_ID: "edit", EDITABLE_SCOPE_COLUMN: "D&IG", "Scenario Name": "Updated", "Delete": False},
            {EDITABLE_ROW_ID: "delete", EDITABLE_SCOPE_COLUMN: "D&IG", "Scenario Name": "Delete me", "Delete": True},
            {EDITABLE_ROW_ID: "", EDITABLE_SCOPE_COLUMN: "", "Scenario Name": "New global", "Delete": False},
        ]

        merged = merge_editor_rows(
            current_rows=current_rows,
            visible_row_ids={"edit", "delete"},
            edited_rows=edited_rows,
            columns=[EDITABLE_SCOPE_COLUMN, "Scenario Name"],
        )

        self.assertEqual([row["Scenario Name"] for row in merged], ["Hidden", "Updated", "New global"])
        self.assertTrue(all(EDITABLE_ROW_ID in row and row[EDITABLE_ROW_ID] for row in merged))

    def test_template_drafts_round_trip_to_local_json(self):
        templates = build_initial_templates()
        templates["Scenarios"][0]["Scenario Name"] = "Edited scenario"

        with TemporaryDirectory(dir="/private/tmp") as tmpdir:
            path = Path(tmpdir) / "business_flow_templates.json"
            saved_path = save_template_draft(templates, path=path)
            loaded = load_template_draft(path=path)

        self.assertEqual(saved_path, path)
        self.assertEqual(loaded["Scenarios"][0]["Scenario Name"], "Edited scenario")

    def test_summary_and_rules_are_scoped_by_scrum_team(self):
        self.assertEqual(available_scrum_teams(), ["D&IG"])

        summary = section_dataframe("D&IG", "End-to-End Summary")
        rules = section_dataframe("D&IG", "Key Modeling Rules")

        self.assertEqual(list(rules.columns), MODELING_RULE_COLUMNS)
        self.assertIn("With Journey", set(summary["Path"]))
        self.assertIn("Journey is optional", set(rules["Rule"]))

    def test_ensure_template_state_seeds_session_once(self):
        state = {}

        with patch("ui.journeys.st.session_state", state):
            _ensure_template_state()
            first_templates = state["business_flow_templates"]
            _ensure_template_state()

        self.assertIs(state["business_flow_templates"], first_templates)
        self.assertEqual(first_templates["Scenarios"][0][EDITABLE_SCOPE_COLUMN], "D&IG")

    def test_render_template_editor_uses_filtered_rows(self):
        state = {TEMPLATES_STATE_KEY: build_initial_templates()}

        with (
            patch("ui.journeys.st.session_state", state),
            patch("ui.journeys.st.subheader"),
            patch("ui.journeys.st.columns", return_value=[NoopContext(), NoopContext(), NoopContext()]),
            patch("ui.journeys.st.button", return_value=False),
            patch("ui.journeys.st.data_editor") as data_editor,
            patch("ui.journeys.st.download_button"),
        ):
            data_editor.side_effect = lambda df, **kwargs: df
            df = _render_template_editor("Key Modeling Rules", "D&IG")

        self.assertEqual(list(df.columns), MODELING_RULE_COLUMNS)

    def test_render_journey_mapping_uses_scrum_team_and_section_controls(self):
        state = {TEMPLATES_STATE_KEY: build_initial_templates()}

        with (
            patch("ui.journeys.st.session_state", state),
            patch("ui.journeys.st.caption"),
            patch("ui.journeys.st.selectbox", return_value="D&IG") as selectbox,
            patch("ui.journeys._selected_section", return_value="Table Roles") as selected_section,
            patch("ui.journeys._render_template_editor") as render_editor,
        ):
            render_editor.return_value = section_dataframe("D&IG", "Table Roles")

            render_journey_mapping({}, "tester")

        selectbox.assert_called_once()
        selected_section.assert_called_once_with("Scenarios")
        render_editor.assert_called_once_with("Table Roles", "D&IG")


if __name__ == "__main__":
    unittest.main()
