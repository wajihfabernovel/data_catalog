import unittest

from postgrest import APIError

from services.supabase_store import SupabaseStore


class SupabaseStoreTests(unittest.TestCase):
    def test_schema_cache_error_message_includes_missing_column_and_migration_sql(self):
        error = APIError(
            {
                "message": "Could not find the 'metadata_profile_json' column of 'catalog_tables' in the schema cache",
                "code": "PGRST204",
                "hint": None,
                "details": None,
            }
        )

        message = SupabaseStore._schema_cache_error_message(error)

        self.assertIn("metadata_profile_json", message)
        self.assertIn("alter table if exists catalog_tables", message)
        self.assertIn("add column if not exists metadata_profile_json text", message)


if __name__ == "__main__":
    unittest.main()
