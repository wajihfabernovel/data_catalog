import unittest
from unittest.mock import Mock, patch

from ui.journeys import (
    JOURNEY_SECTIONS,
    _render_selected_journey_section,
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


if __name__ == "__main__":
    unittest.main()
