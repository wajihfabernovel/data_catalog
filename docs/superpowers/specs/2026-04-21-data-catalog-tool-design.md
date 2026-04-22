# Data Catalog Tool Design

## Summary

Build a production-ready Streamlit application at the repository root that parses Dataverse metadata XML, renders editable table metadata cards, authenticates users with company Microsoft accounts, persists shared catalog data in SharePoint Lists, and exports a formatted Excel workbook with one worksheet per Dataverse table.

This implementation satisfies the required scope from [CONTEXT.md](/Users/wajih.arfaoui/Library/CloudStorage/OneDrive-EY/Desktop/Data%20Catalog/CONTEXT.md) with the following approved adjustments:

- shared online collaboration through SharePoint
- company-email user login
- SharePoint Lists as the source of truth
- one worksheet per Dataverse table in the Excel export
- XML file upload and search/filter included
- DDL generation excluded

## Goals

- Parse Dataverse `EntityType` metadata from XML using `xml.etree.ElementTree`
- Filter dynamically by user-supplied table names
- Authenticate users with company Microsoft accounts
- Read and write shared catalog metadata through SharePoint Lists
- Show each matched table in a clean expandable card UI
- Collect structured metadata across data quality, pipeline relevance, target model recommendation, and sign-off sections
- Keep transient edits in `st.session_state`
- Export a formatted Excel workbook with one worksheet per table, including schema and editable metadata sections

## Non-Goals

- Local JSON draft persistence as the durable store
- Excel re-import back into the app
- Database-backed persistence outside SharePoint
- Automated DDL creation
- Real-time collaborative cursors or live presence

## Repository Layout

The repository root will be the application root.

```text
.
├── app.py
├── parser/
│   └── xml_parser.py
├── models/
│   └── mappings.py
├── services/
│   ├── export.py
│   ├── sharepoint.py
│   └── auth.py
├── ui/
│   ├── cards.py
│   └── forms.py
├── utils/
│   └── helpers.py
├── assets/
│   └── styles.css
├── requirements.txt
├── README.md
├── .env.example
└── .gitignore
```

Design documentation remains in:

```text
docs/superpowers/specs/2026-04-21-data-catalog-tool-design.md
```

## Architecture

### `app.py`

`app.py` remains orchestration-only. Its responsibilities:

- page configuration and CSS loading
- user login entry point and authenticated session flow
- input collection for XML and table names
- parse and sync actions
- session state initialization
- table filtering
- card rendering loop
- save-to-SharePoint action wiring
- export action wiring

No XML parsing, SharePoint API logic, or Excel formatting logic will live in this file.

### `parser/xml_parser.py`

Responsibilities:

- parse raw XML safely with `xml.etree.ElementTree`
- locate `EntityType` nodes regardless of namespace usage
- extract table name, primary key, and `Property` fields
- ignore `NavigationProperty`
- filter parsed entities against requested table names

This module will expose a parse entry point that is safe to cache during a session.

### `models/mappings.py`

Responsibilities:

- define the EDM-to-Azure SQL mapping table
- provide a helper to convert EDM types to SQL types

Required mappings:

- `Edm.String` -> `NVARCHAR(MAX)`
- `Edm.Int32` -> `INT`
- `Edm.Int64` -> `BIGINT`
- `Edm.Decimal` -> `DECIMAL(18,6)`
- `Edm.Guid` -> `UNIQUEIDENTIFIER`
- `Edm.DateTimeOffset` -> `DATETIMEOFFSET`
- `Edm.Binary` -> `VARBINARY(MAX)`

Unmapped types will resolve to `NVARCHAR(MAX)` to keep the app resilient.

### `services/auth.py`

Responsibilities:

- handle Microsoft Entra ID user sign-in for company accounts
- manage token acquisition for delegated SharePoint or Microsoft Graph access
- expose authenticated user identity for display and auditing

The app will assume tenant-controlled sign-in with company email addresses.

### `services/sharepoint.py`

Responsibilities:

- read and write SharePoint List items
- fetch table-level catalog entries
- fetch column-level schema rows
- create missing table records
- create or update column records
- save form edits back to SharePoint

Preferred integration path:

- Microsoft Graph with delegated permissions

Fallback only if tenant constraints require it:

- SharePoint REST API

### `ui/cards.py`

Responsibilities:

- render each table card summary
- display table name, primary key, and column count
- wrap detail sections in an expander or equivalent bordered container

### `ui/forms.py`

Responsibilities:

- render the schema dataframe
- render all metadata form sections
- bind inputs to stable, unique `st.session_state` keys per table
- emit and update the normalized per-table state object used for SharePoint saves and Excel export

### `services/export.py`

Responsibilities:

- generate a workbook in memory
- create one worksheet per Dataverse table
- render schema as a proper Excel table
- render metadata sections in a template-like human-readable layout
- produce a downloadable `.xlsx` artifact

### `utils/helpers.py`

Responsibilities:

- normalize comma-separated table names
- create stable per-table session keys
- build default section payloads
- sanitize Excel sheet names
- provide table filtering helpers
- support SharePoint row normalization

## SharePoint Data Model

The shared system of record will be two SharePoint Lists.

### `CatalogTables`

One item per Dataverse table.

Core fields:

- `TableKey`
- `TableName`
- `PrimaryKey`
- `ExtractByPipeline`
- `DeltaExtractionColumn`
- `FeedPowerBI`
- `KeyMetricsOrDimensions`
- `WritePath`
- `NullableIssues`
- `FormatInconsistencies`
- `DuplicateRecords`
- `OrphanRecords`
- `HardDeleteInUse`
- `OverallQualityRating`
- `QualityNotes`
- `Recommendation`
- `MergeWith`
- `SplitInto`
- `ReplacedBy`
- `MissingColumns`
- `MissingConstraints`
- `CompletedBy`
- `ReviewedBy`
- `ReviewedByBusiness`
- `Status`
- `DateApproved`
- `Notes`
- `LastSyncedAt`
- `LastModifiedBy`

### `CatalogColumns`

One item per column.

Core fields:

- `TableKey`
- `TableName`
- `ColumnName`
- `EdmType`
- `SqlType`

`TableKey` will be the stable link between table-level and column-level data. It should be deterministic from the parsed Dataverse table name unless the tenant requires a different unique identifier pattern.

## Data Model in the App

Each table will be tracked in session state using a normalized key derived from table name. The logical payload is:

```python
{
    "table_key": str,
    "table_name": str,
    "primary_key": str,
    "schema": [
        {
            "column_name": str,
            "edm_type": str,
            "sql_type": str,
        }
    ],
    "data_quality": {
        "nullable_issues": str,
        "format_inconsistencies": str,
        "duplicate_records": str,
        "orphan_records": str,
        "hard_delete_in_use": str,
        "overall_quality_rating": str,
        "quality_notes": str,
    },
    "pipeline": {
        "extract_by_pipeline": str,
        "delta_extraction_column": str,
        "feed_power_bi": str,
        "key_metrics_or_dimensions": str,
        "write_path": str,
    },
    "target_model": {
        "recommendation": str,
        "merge_with": str,
        "split_into": str,
        "replaced_by": str,
        "missing_columns": str,
        "missing_constraints": str,
    },
    "signoff": {
        "completed_by": str,
        "reviewed_by": str,
        "reviewed_by_business": str,
        "status": str,
        "date_approved": str | None,
        "notes": str,
    },
}
```

`st.session_state` is transient UI state only. SharePoint is the durable shared source of truth.

## Authentication and Access

Users will authenticate with company email accounts through Microsoft Entra ID.

Expected behavior:

- the user is prompted to sign in
- the app acquires delegated access tokens
- SharePoint reads and writes occur in the signed-in user context
- permissions are governed by existing tenant and SharePoint access controls

This enables all authorized users to see the same saved catalog data.

## Input and Sync Flow

### Inputs

The app will provide:

- XML payload text area
- XML file uploader
- comma-separated table names input
- parse and sync button

Behavior:

- if a file is uploaded, its contents can populate or override the XML source
- table names constrain the parse scope
- parsing runs only when explicitly triggered

### XML Parsing Rules

- use `xml.etree.ElementTree.fromstring`
- find `EntityType` nodes by local-name aware traversal
- read `Name` from entity attributes
- read primary key from `Key/PropertyRef`
- read columns from `Property`
- ignore `NavigationProperty`

### SharePoint Sync Rules

For each parsed table:

- load any existing `CatalogTables` item by `TableKey`
- load matching `CatalogColumns` items by `TableKey`
- if the table does not exist, create it
- if columns do not exist, create them
- if columns already exist, update type metadata if needed
- populate the Streamlit form from SharePoint-backed values when available

Saving from the UI writes back only the editable table-level metadata plus any schema updates that are in scope.

## UI Design

### Top-Level Layout

The page will include:

- title and short description
- authenticated user context
- input block for XML and table names
- actions for parse/sync, save, refresh from SharePoint, and export
- optional search/filter input shown after data load

### Table Cards

Each matched table renders inside `st.container(border=True)` and shows:

- table name
- primary key
- number of columns
- expandable detail section

### Expanded Sections

Each expanded card contains:

1. Schema section
2. Data quality observations
3. Pipeline relevance
4. Target model recommendation
5. Sign-off

The schema section uses a dataframe with:

- `column_name`
- `edm_type`
- `sql_type`

Search/filter narrows visible cards by table name only. It does not delete state.

## Save and Refresh Behavior

### Save

When the user saves:

- current table form values are written to `CatalogTables`
- schema rows are written to `CatalogColumns` if new or updated
- `LastModifiedBy` and `LastSyncedAt` are updated where possible

### Refresh

When the user refreshes:

- the app reloads current values from SharePoint
- local transient state is updated to match shared persisted state

This gives users a clear way to pull collaborators' edits without treating the app as a real-time editor.

## Excel Export

The export will produce a single workbook with one worksheet per Dataverse table.

Each worksheet will include:

- a table title and summary block
- primary key and table metadata
- schema rendered as a proper Excel table
- editable metadata sections laid out in a readable template style aligned with the example provided by the user

The worksheet should be optimized for human review, not just raw machine export.

Expected sections in each worksheet:

- Schema
- Data Quality Observations
- Pipeline Relevance
- Target Model Recommendation
- Sign Off

SharePoint remains the source of truth. Edited Excel files are not re-imported.

## Styling

Custom styling in `assets/styles.css` will provide:

- cleaner page spacing
- stronger visual separation between cards
- readable section headers
- compact metric presentation for table summaries

The app should feel intentional and internal-tool quality without diverging from Streamlit strengths.

## Validation and Error Handling

The app will provide practical user-facing feedback for:

- invalid or empty XML
- empty table name input
- authentication failures
- SharePoint connectivity failures
- missing SharePoint configuration
- no matching tables found
- export requested with no loaded data
- save conflicts or write failures

Errors should be concise and actionable.

## Performance

- parsing will be cached per session against XML payload content and normalized table names
- explicit parse and refresh actions prevent unnecessary network calls
- SharePoint reads should batch where practical
- Excel export runs in memory on demand

This is sufficient for v1 without introducing heavier infrastructure.

## Implementation Decision Record

### Selected Approach

Streamlit front end with Microsoft Entra ID user login and SharePoint Lists as the durable shared backend.

### Alternatives Considered

1. Local JSON draft persistence
   - rejected because it does not support shared online editing

2. SharePoint file storage as the durable store
   - simpler, but weak for concurrent collaborative updates

3. Single SharePoint list with serialized schema per table
   - simpler schema, but poor for querying, synchronization, and maintainability

4. Excel as both store and export format
   - rejected because SharePoint should remain the source of truth

### Reason for Choice

Two SharePoint Lists provide a cleaner relational model for shared metadata, safer concurrent editing than a single file, and straightforward export generation while keeping Streamlit as the editing experience.

## Acceptance Criteria

The implementation is complete when:

- the repo root contains the requested modular structure
- the app supports company-account login
- the app accepts XML via text area and file upload
- the app accepts table names dynamically
- the parser extracts table name, primary key, and column list correctly
- SharePoint list reads and writes work for authorized users
- each table renders as an expandable card
- all required metadata sections are editable
- saved changes are available to other authorized users through SharePoint-backed reloads
- search/filter narrows visible cards
- Excel export generates one worksheet per table with schema and metadata sections in a readable template layout

