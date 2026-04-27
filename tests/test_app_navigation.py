import unittest

from app import APP_SECTIONS, _render_selected_section


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


if __name__ == "__main__":
    unittest.main()
