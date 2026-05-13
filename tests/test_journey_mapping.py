import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from services.business_flows import (
    BUSINESS_FLOW_SECTIONS,
    EDITABLE_ROW_ID,
    EDITABLE_SCOPE_COLUMN,
    GENERATED_FROM_FIELD,
    GENERATED_KIND_FIELD,
    MODELING_RULE_COLUMNS,
    SCENARIO_COLUMNS,
    TABLE_ROLE_COLUMNS,
    USER_CREATED_FIELD,
    available_scrum_teams,
    build_initial_templates,
    filter_template_rows,
    load_template_draft,
    merge_editor_rows,
    save_template_draft,
    section_dataframe,
    synchronize_linked_templates,
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

    def test_scenario_rows_implicitly_generate_table_roles_and_summary(self):
        templates = {section: [] for section in BUSINESS_FLOW_SECTIONS}
        templates["Scenarios"] = [
            {
                EDITABLE_ROW_ID: "scenario-1",
                EDITABLE_SCOPE_COLUMN: "Alpha",
                USER_CREATED_FIELD: True,
                "Scenario Name": "Alpha approval",
                "Business Meaning": "Alpha users approve records.",
                "Flow": "User -> Alpha Request -> Approval",
                "Main Tables": "alpha_request; alpha_approval",
            }
        ]

        synced = synchronize_linked_templates(templates)

        target_tables = {row["Target Table"] for row in synced["Table Roles"]}
        self.assertEqual(target_tables, {"alpha_request", "alpha_approval"})
        self.assertTrue(all(row[EDITABLE_SCOPE_COLUMN] == "Alpha" for row in synced["Table Roles"]))
        self.assertTrue(all(row[GENERATED_FROM_FIELD] == "scenario-1" for row in synced["Table Roles"]))
        self.assertEqual(synced["End-to-End Summary"][0]["Path"], "Alpha approval")
        self.assertEqual(synced["End-to-End Summary"][0]["End-to-End Flow"], "User -> Alpha Request -> Approval")

    def test_implicit_sync_refreshes_generated_rows_when_source_changes(self):
        templates = {section: [] for section in BUSINESS_FLOW_SECTIONS}
        templates["Scenarios"] = [
            {
                EDITABLE_ROW_ID: "scenario-1",
                EDITABLE_SCOPE_COLUMN: "Alpha",
                USER_CREATED_FIELD: True,
                "Scenario Name": "Alpha approval",
                "Business Meaning": "",
                "Flow": "Old flow",
                "Main Tables": "old_table",
            }
        ]
        first_sync = synchronize_linked_templates(templates)
        first_sync["Scenarios"][0]["Flow"] = "New flow"
        first_sync["Scenarios"][0]["Main Tables"] = "new_table"

        second_sync = synchronize_linked_templates(first_sync)

        self.assertEqual([row["Target Table"] for row in second_sync["Table Roles"]], ["new_table"])
        self.assertEqual(second_sync["End-to-End Summary"][0]["End-to-End Flow"], "New flow")
        self.assertEqual(second_sync["End-to-End Summary"][0][GENERATED_KIND_FIELD], "scenario_summary")

    def test_user_created_table_roles_implicitly_generate_modeling_rules(self):
        templates = {section: [] for section in BUSINESS_FLOW_SECTIONS}
        templates["Table Roles"] = [
            {
                EDITABLE_ROW_ID: "role-1",
                EDITABLE_SCOPE_COLUMN: "Alpha",
                USER_CREATED_FIELD: True,
                "Order": 1,
                "Concept": "Alpha Request",
                "Role in Flow": "Stores approval request header",
                "Target Table": "alpha_request",
                "Notes": "Header table",
            }
        ]

        synced = synchronize_linked_templates(templates)

        self.assertEqual(synced["Key Modeling Rules"][0]["Rule"], "Alpha Request usage")
        self.assertEqual(synced["Key Modeling Rules"][0]["Implementation"], "alpha_request")
        self.assertEqual(synced["Key Modeling Rules"][0][GENERATED_FROM_FIELD], "role-1")

    def test_user_created_modeling_rules_implicitly_generate_table_roles(self):
        templates = {section: [] for section in BUSINESS_FLOW_SECTIONS}
        templates["Key Modeling Rules"] = [
            {
                EDITABLE_ROW_ID: "rule-1",
                EDITABLE_SCOPE_COLUMN: "",
                USER_CREATED_FIELD: True,
                "Rule": "Alpha request history",
                "Explanation": "Persist request history.",
                "Implementation": "alpha_request_history stores history rows.",
            }
        ]

        synced = synchronize_linked_templates(templates)

        self.assertEqual(synced["Table Roles"][0]["Target Table"], "alpha_request_history")
        self.assertEqual(synced["Table Roles"][0][EDITABLE_SCOPE_COLUMN], "")
        self.assertEqual(synced["Table Roles"][0][GENERATED_KIND_FIELD], "rule_table_role")

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
