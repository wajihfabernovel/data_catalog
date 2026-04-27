import unittest
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd

from ui.journeys import (
    JOURNEY_SECTIONS,
    _load_view_editor,
    _render_selected_journey_section,
    _save_view_editor_journey,
    render_journey_mapping,
)


class JourneyMappingTests(unittest.TestCase):
    def test_render_selected_journey_section_calls_only_requested_renderer(self):
        calls = []
        renderers = {
            section: (lambda section=section: calls.append(section))
            for section in JOURNEY_SECTIONS
        }

        _render_selected_journey_section("State Machines", renderers)

        self.assertEqual(calls, ["State Machines"])

    def test_render_journey_mapping_shows_fetch_errors_without_crashing(self):
        store = Mock()
        store.fetch_journeys.side_effect = RuntimeError("relation journeys does not exist")

        with (
            patch("ui.journeys._journey_store", return_value=store),
            patch("ui.journeys.st.caption"),
            patch("ui.journeys.st.error") as error,
        ):
            render_journey_mapping({}, "supabase_app")

        error.assert_called_once()
        self.assertIn("relation journeys does not exist", error.call_args[0][0])

    def test_load_view_editor_replaces_stale_step_action_state(self):
        state = {
            "journey_view_step_0_user_action": "stale action",
            "journey_view_step_0_write_op_hive_old": "DELETE",
            "journey_view_steps": [{"step_number": 1, "user_action": "stale action"}],
        }
        journey = {
            "journey_id": "J001",
            "journey_name": "Knowledge Summary",
            "module_domain": "D&IG",
            "primary_user_role": "Product Manager",
            "frequency": "Daily",
            "complexity": "Medium",
            "interview_date": "2026-04-27",
            "interviewer": "Wajih",
            "steps": [
                {
                    "step_number": 1,
                    "user_action": "Updated action",
                    "screen_component": "Dialog",
                    "validation_rules": "Required title",
                    "business_rules": "Create only once",
                    "notes": "Edit test",
                    "table_refs": [
                        {
                            "table_name": "hive_summary",
                            "access_mode": "WRITE",
                            "is_catalog_table": True,
                            "write_operation": "UPSERT",
                        }
                    ],
                    "transitions": [],
                }
            ],
        }

        with patch("ui.journeys.st.session_state", state):
            _load_view_editor(journey)

        self.assertNotIn("journey_view_step_0_write_op_hive_old", state)
        self.assertEqual(state["journey_view_editor_journey_id"], "J001")
        self.assertEqual(state["journey_view_steps"][0]["user_action"], "Updated action")
        self.assertEqual(state["journey_view_steps"][0]["write_operations"]["hive_summary"], "UPSERT")

    def test_save_view_editor_journey_persists_modified_steps_and_actions(self):
        state = {
            "journey_view_editor_journey_id": "J001",
            "journey_view_editor_journey_name": "Knowledge Summary",
            "journey_view_editor_module_domain": "D&IG",
            "journey_view_editor_user_roles": ["Product Manager"],
            "journey_view_editor_frequency": "Daily",
            "journey_view_editor_complexity": "Medium",
            "journey_view_editor_interview_date": date(2026, 4, 27),
            "journey_view_editor_interviewer": "Wajih",
            "journey_view_steps": [{"step_number": 1}],
            "journey_view_step_0_step_number": 1,
            "journey_view_step_0_user_action": "User updates Knowledge Summary",
            "journey_view_step_0_screen_component": "Summary dialog",
            "journey_view_step_0_tables_read_known": ["hive_account"],
            "journey_view_step_0_tables_read_extra": "legacy_reference",
            "journey_view_step_0_tables_written_known": ["hive_summary"],
            "journey_view_step_0_tables_written_extra": "",
            "journey_view_step_0_write_op_hive_summary": "UPDATE",
            "journey_view_step_0_validation_rules": "Title is mandatory",
            "journey_view_step_0_business_rules": "Only owner can update",
            "journey_view_step_0_notes": "Inline edit",
            "journey_view_step_0_transitions_data": pd.DataFrame(
                [
                    {
                        "entity_table": "hive_summary",
                        "status_field_name": "statuscode",
                        "from_state": "Draft",
                        "to_state": "Published",
                        "trigger_action": "Save",
                        "user_role_required": "Product Manager",
                        "validation_rules": "Title is mandatory",
                        "side_effects": "Notify owner",
                    }
                ]
            ),
        }
        catalog_tables = {
            "hive_account": {"table_key": "hive_account", "table_name": "hive_account"},
            "hive_summary": {"table_key": "hive_summary", "table_name": "hive_summary"},
        }
        store = Mock()

        with patch("ui.journeys.st.session_state", state):
            errors = _save_view_editor_journey(store, catalog_tables, "tester")

        self.assertEqual(errors, [])
        store.save_journey.assert_called_once()
        kwargs = store.save_journey.call_args.kwargs
        self.assertEqual(kwargs["journey"]["journey_id"], "J001")
        self.assertEqual(kwargs["steps"][0]["user_action"], "User updates Knowledge Summary")
        self.assertEqual(kwargs["steps"][0]["status_field_changes"], "hive_summary.statuscode: Draft → Published")
        self.assertIn(
            {
                "journey_id": "J001",
                "step_number": 1,
                "table_name": "hive_summary",
                "access_mode": "WRITE",
                "write_operation": "UPDATE",
                "is_catalog_table": True,
                "catalog_table_key": "hive_summary",
            },
            kwargs["step_tables"],
        )
        self.assertEqual(kwargs["transitions"][0]["to_state"], "Published")


if __name__ == "__main__":
    unittest.main()
