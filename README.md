# Dataverse Data Catalog

Streamlit application for parsing Dataverse metadata XML, collecting shared table-level catalog metadata, storing the shared state in Supabase, and exporting a formatted Excel workbook with one worksheet per Dataverse table.

## Features

- Parse Dataverse `EntityType` XML metadata
- Filter by a comma-separated set of table names
- Render each table in an expandable card
- Capture relationships, data quality, pipeline, target model, and sign-off metadata
- Read and write shared state through Supabase
- Save and reload local JSON drafts as a fallback
- Export a formatted workbook with one sheet per table and a real schema Excel table

## Project Structure

```text
.
├── app.py
├── parser/xml_parser.py
├── models/mappings.py
├── services/export.py
├── services/local_store.py
├── services/supabase_store.py
├── ui/cards.py
├── ui/forms.py
├── utils/helpers.py
├── assets/styles.css
├── requirements.txt
└── .env.example
```

## Prerequisites

- Python 3.11+
- A Supabase project
- A backend secret key for that project

## Supabase Setup

Create these tables:

- `catalog_tables`
- `catalog_columns`
- `catalog_relationships_fk`
- `catalog_relationships_referenced_by`
- `journeys`
- `journey_steps`
- `journey_step_tables`
- `state_transitions`

Run `supabase_catalog_metadata_migration.sql` for Dataverse API metadata columns and
`supabase_journey_mapping_migration.sql` for the journey mapping tables.

## Configuration

Copy `.env.example` values into `.env` and replace them with your Supabase project details.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Notes

- Supabase is the shared system of record. Excel workbooks are export artifacts only.
- Local draft mode writes to `.data_catalog_drafts/latest.json`.
- Refresh from Supabase to pull collaborators' latest persisted changes.
