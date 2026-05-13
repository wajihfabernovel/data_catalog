"""Workbook-style business flow reference data for the journey mapping page."""

from __future__ import annotations

from typing import Any

import pandas as pd


SCENARIO_COLUMNS = ["Scenario Name", "Business Meaning", "Flow", "Main Tables"]
TABLE_ROLE_COLUMNS = ["Order", "Concept", "Role in Flow", "Target Table", "Notes"]
END_TO_END_COLUMNS = ["Path", "End-to-End Flow"]
MODELING_RULE_COLUMNS = ["Rule", "Explanation", "Implementation"]

BUSINESS_FLOW_SECTIONS = (
    "Scenarios",
    "Table Roles",
    "End-to-End Summary",
    "Key Modeling Rules",
)

SECTION_DATA_KEYS = {
    "Scenarios": "scenarios",
    "Table Roles": "table_roles",
    "End-to-End Summary": "end_to_end_summary",
    "Key Modeling Rules": "key_modeling_rules",
}

SECTION_COLUMNS = {
    "Scenarios": SCENARIO_COLUMNS,
    "Table Roles": TABLE_ROLE_COLUMNS,
    "End-to-End Summary": END_TO_END_COLUMNS,
    "Key Modeling Rules": MODELING_RULE_COLUMNS,
}

BUSINESS_FLOW_DATA: dict[str, dict[str, list[dict[str, Any]]]] = {
    "D&IG": {
        "scenarios": [
            {
                "Scenario Name": "Journey -> Discover -> Define/Create -> Idea",
                "Business Meaning": (
                    "User creates a Journey, starts from Discover, then progresses through modules "
                    "until Ideas are created."
                ),
                "Flow": (
                    "User creates Journey -> assigns participants -> creates Discover module instance -> "
                    "links Journey to Module -> creates submodules/tasks from master data -> completes "
                    "tasks -> Create task creates Idea -> assessment flow continues."
                ),
                "Main Tables": (
                    "dig_journey; dig_journey_participant; dig_module_instance; "
                    "dig_journey_module_link; dig_submodule_instance; dig_task_instance; dig_idea; "
                    "dig_task_idea_link; dig_module_idea_link; dig_journey_idea_link"
                ),
            },
            {
                "Scenario Name": "Journey starts from Define",
                "Business Meaning": "User creates a Journey but skips Discover and starts directly from Define.",
                "Flow": (
                    "User creates Journey -> creates Define module instance -> links Journey to Define -> "
                    "creates submodules/tasks -> may continue to Create -> creates Ideas -> assessment flow continues."
                ),
                "Main Tables": (
                    "dig_journey; dig_module_instance; dig_journey_module_link; "
                    "dig_submodule_instance; dig_task_instance; dig_idea"
                ),
            },
            {
                "Scenario Name": "Journey starts from Create",
                "Business Meaning": "User creates a Journey but starts directly from Create.",
                "Flow": (
                    "User creates Journey -> creates Create module instance -> links Journey to Create -> "
                    "creates Create submodules/tasks -> task creates Idea -> Idea linked to Task, Module, "
                    "and optionally Journey -> assessment flow continues."
                ),
                "Main Tables": (
                    "dig_journey; dig_module_instance; dig_journey_module_link; dig_task_instance; "
                    "dig_idea; dig_task_idea_link; dig_module_idea_link; dig_journey_idea_link"
                ),
            },
            {
                "Scenario Name": "Journey with multiple modules",
                "Business Meaning": "One Journey can have multiple module instances of the same type.",
                "Flow": (
                    "Journey A -> Discover A/B/C -> Define A/B -> Create A. Each module is a separate "
                    "dig_module_instance row and linked using dig_journey_module_link."
                ),
                "Main Tables": "dig_journey; dig_module_instance; dig_journey_module_link",
            },
            {
                "Scenario Name": "Reusable module in multiple Journeys",
                "Business Meaning": "Same module instance can be linked to multiple Journeys.",
                "Flow": (
                    "Existing Discover A is linked to Journey 1 and Journey 2. Do not duplicate module; "
                    "create additional dig_journey_module_link rows."
                ),
                "Main Tables": "dig_module_instance; dig_journey_module_link",
            },
            {
                "Scenario Name": "No Journey, start directly from Module",
                "Business Meaning": "User skips Journey and starts from Discover/Define/Create directly.",
                "Flow": (
                    "User creates module instance -> no Journey row -> no Journey Module Link -> "
                    "submodules/tasks created -> Create may produce Ideas -> assessment flow continues."
                ),
                "Main Tables": (
                    "dig_module_instance; dig_submodule_instance; dig_task_instance; dig_idea; "
                    "dig_task_idea_link; dig_module_idea_link"
                ),
            },
            {
                "Scenario Name": "Direct Idea creation",
                "Business Meaning": "User creates an Idea without Journey or Module.",
                "Flow": (
                    "User creates Idea directly -> created_from_source = DIRECT -> no task/module/journey "
                    "link required -> Idea is globally visible -> assessment flow continues."
                ),
                "Main Tables": "dig_idea",
            },
            {
                "Scenario Name": "Idea created from exact Task",
                "Business Meaning": "Idea is created from a specific Task inside a Submodule inside a Module.",
                "Flow": (
                    "User works on Create task -> creates Idea -> system creates dig_task_idea_link -> "
                    "module and journey links may also be created -> lineage is Task -> Submodule -> Module."
                ),
                "Main Tables": (
                    "dig_task_instance; dig_submodule_instance; dig_module_instance; dig_idea; "
                    "dig_task_idea_link; dig_module_idea_link"
                ),
            },
            {
                "Scenario Name": "Existing Idea linked to Module/Journey",
                "Business Meaning": "An existing global Idea can be linked later to a Module, Task, or Journey.",
                "Flow": (
                    "Idea already exists -> user links it to a context -> system inserts link row -> "
                    "Idea is not duplicated."
                ),
                "Main Tables": "dig_idea; dig_module_idea_link; dig_task_idea_link; dig_journey_idea_link",
            },
            {
                "Scenario Name": "Assessment initiated for Idea",
                "Business Meaning": "Any user can initiate an Assessment for a global Idea.",
                "Flow": (
                    "User initiates Assessment -> system creates Assessment header -> creates frozen Idea "
                    "Snapshot -> assigns D/V/F assessors -> sends notifications."
                ),
                "Main Tables": (
                    "dig_idea_assessment; dig_assessment_idea_snapshot; "
                    "dig_assessor_assignment; dig_notification"
                ),
            },
            {
                "Scenario Name": "Three D/V/F assessor assignments",
                "Business Meaning": "Assessment assigns Desirability, Viability, and Feasibility assessors.",
                "Flow": (
                    "Initiator selects one assessor per criterion -> system creates one "
                    "dig_assessor_assignment row per criterion."
                ),
                "Main Tables": "dig_assessor_assignment; dig_ref_assessment_criterion",
            },
            {
                "Scenario Name": "Assessors submit scores",
                "Business Meaning": "Assessors score the frozen Idea Snapshot, not the live Idea.",
                "Flow": (
                    "Assessor opens assigned assessment -> reviews snapshot -> submits score -> "
                    "dig_assessment_score row created -> assessment becomes ready when required scores are submitted."
                ),
                "Main Tables": (
                    "dig_assessment_score; dig_assessment_idea_snapshot; dig_assessor_assignment"
                ),
            },
            {
                "Scenario Name": "Prioritisation = Proceed",
                "Business Meaning": "Proceed decision updates Idea and may create Project Details.",
                "Flow": (
                    "Scores completed -> initiator chooses Proceed -> decision row created -> Idea status "
                    "becomes PROCEED -> project_details row may be created -> project linked to Idea."
                ),
                "Main Tables": "dig_prioritisation_decision; dig_idea; project_details; dig_project_idea_link",
            },
            {
                "Scenario Name": "Prioritisation = Revisit",
                "Business Meaning": "Revisit sends Idea back for refinement; no Project is created.",
                "Flow": (
                    "Scores completed -> initiator chooses Revisit -> decision row created -> Idea status "
                    "becomes REVISIT -> Idea may be refined and reassessed later with a new snapshot."
                ),
                "Main Tables": (
                    "dig_prioritisation_decision; dig_idea; dig_idea_assessment; "
                    "dig_assessment_idea_snapshot"
                ),
            },
            {
                "Scenario Name": "Prioritisation = Archive",
                "Business Meaning": "Archive keeps historical data but stops active progression.",
                "Flow": (
                    "Scores completed -> initiator chooses Archive -> decision row created -> Idea status "
                    "becomes ARCHIVE -> no Project is created."
                ),
                "Main Tables": "dig_prioritisation_decision; dig_idea; dig_business_audit_event",
            },
            {
                "Scenario Name": "Project created from Idea",
                "Business Meaning": "A generic Project Details record can be linked to one or more Ideas.",
                "Flow": (
                    "Create project_details row -> project_source_type = IDEA -> create "
                    "dig_project_idea_link row."
                ),
                "Main Tables": "project_details; dig_project_idea_link; dig_idea",
            },
            {
                "Scenario Name": "Project created from SKU/Product",
                "Business Meaning": "A generic Project Details record can be linked to one or more SKUs.",
                "Flow": (
                    "Create project_details row -> project_source_type = PRODUCT_SKU -> create "
                    "dig_project_sku_link row."
                ),
                "Main Tables": "project_details; dig_project_sku_link; dig_product_sku",
            },
            {
                "Scenario Name": "Project created from scratch",
                "Business Meaning": "Project can be created with no Idea and no SKU source.",
                "Flow": (
                    "Create project_details row -> project_source_type = SCRATCH -> no project source link required."
                ),
                "Main Tables": "project_details",
            },
            {
                "Scenario Name": "Notifications",
                "Business Meaning": "System alerts users about business events requiring awareness or action.",
                "Flow": (
                    "Business event occurs -> dig_notification row created -> user reads notification -> "
                    "is_read/read_at updated."
                ),
                "Main Tables": "dig_notification; dig_app_user",
            },
            {
                "Scenario Name": "Business audit history",
                "Business Meaning": "Records who did what, to which business object, and when.",
                "Flow": (
                    "Business action occurs -> dig_business_audit_event row created with entity_type, "
                    "entity_id, action_code, old/new JSON."
                ),
                "Main Tables": "dig_business_audit_event",
            },
            {
                "Scenario Name": "Technical/system logs",
                "Business Meaning": "Records backend jobs, failures, integrations, ETL and operational events.",
                "Flow": (
                    "System process runs -> success/error/info row written to dig_technical_log_event "
                    "with process, severity, correlation and payload."
                ),
                "Main Tables": "dig_technical_log_event",
            },
            {
                "Scenario Name": "Master data creates instances",
                "Business Meaning": (
                    "Master tables define standard modules, submodules and tasks; instance tables store "
                    "actual user work."
                ),
                "Flow": (
                    "Admin defines master records -> user starts module -> system creates "
                    "module/submodule/task instances from master definitions."
                ),
                "Main Tables": (
                    "dig_master_module; dig_master_submodule; dig_master_task; "
                    "dig_module_instance; dig_submodule_instance; dig_task_instance"
                ),
            },
        ],
        "table_roles": [
            {"Order": 1, "Concept": "User", "Role in Flow": "Creates Journey / Module / Idea / Assessment", "Target Table": "dig_app_user", "Notes": "Core user reference"},
            {"Order": 2, "Concept": "Journey", "Role in Flow": "Optional container for D&IG work", "Target Table": "dig_journey", "Notes": "Can be skipped"},
            {"Order": 3, "Concept": "Journey Participants", "Role in Flow": "Assign Owner / Contributor / Creator roles", "Target Table": "dig_journey_participant", "Notes": "Scoped user roles"},
            {"Order": 4, "Concept": "Master Module", "Role in Flow": "Defines Discover / Define / Create", "Target Table": "dig_master_module", "Notes": "Template/master data"},
            {"Order": 5, "Concept": "Module Instance", "Role in Flow": "Actual Discover A / Define B / Create C", "Target Table": "dig_module_instance", "Notes": "Can exist without Journey"},
            {"Order": 6, "Concept": "Journey Module Link", "Role in Flow": "Links/reuses Modules in Journeys", "Target Table": "dig_journey_module_link", "Notes": "Resolves Journey <-> Module M:N"},
            {"Order": 7, "Concept": "Master Submodule", "Role in Flow": "Defines standard submodules", "Target Table": "dig_master_submodule", "Notes": "Template/master data"},
            {"Order": 8, "Concept": "Submodule Instance", "Role in Flow": "Actual submodule under a Module", "Target Table": "dig_submodule_instance", "Notes": "Execution data"},
            {"Order": 9, "Concept": "Master Task", "Role in Flow": "Defines standard tasks/questions/templates", "Target Table": "dig_master_task", "Notes": "Template/master data"},
            {"Order": 10, "Concept": "Task Instance", "Role in Flow": "Actual user task", "Target Table": "dig_task_instance", "Notes": "Execution data"},
            {"Order": 11, "Concept": "Idea", "Role in Flow": "Global idea visible to everyone", "Target Table": "dig_idea", "Notes": "Can be created directly or from Task/Module"},
            {"Order": 12, "Concept": "Task Idea Link", "Role in Flow": "Links Idea to exact creation Task", "Target Table": "dig_task_idea_link", "Notes": "Task -> Submodule -> Module lineage"},
            {"Order": 13, "Concept": "Module Idea Link", "Role in Flow": "Links Idea to Module context", "Target Table": "dig_module_idea_link", "Notes": "Module-level lineage"},
            {"Order": 14, "Concept": "Journey Idea Link", "Role in Flow": "Optional Journey-Idea link", "Target Table": "dig_journey_idea_link", "Notes": "Journey context"},
            {"Order": 15, "Concept": "Assessment", "Role in Flow": "Assessment header initiated by user", "Target Table": "dig_idea_assessment", "Notes": "Starts scoring process"},
            {"Order": 16, "Concept": "Snapshot", "Role in Flow": "Frozen Idea version for assessment", "Target Table": "dig_assessment_idea_snapshot", "Notes": "Scores snapshot, not live Idea"},
            {"Order": 17, "Concept": "Assessor Assignment", "Role in Flow": "Assign D/V/F assessors", "Target Table": "dig_assessor_assignment", "Notes": "One active assessor per criterion recommended"},
            {"Order": 18, "Concept": "Assessment Score", "Role in Flow": "Submitted score", "Target Table": "dig_assessment_score", "Notes": "Score grain = snapshot + assignment + criterion"},
            {"Order": 19, "Concept": "Prioritisation Decision", "Role in Flow": "Proceed/Revisit/Archive", "Target Table": "dig_prioritisation_decision", "Notes": "Updates Idea status"},
            {"Order": 20, "Concept": "Project Details", "Role in Flow": "Generic shared project table", "Target Table": "project_details", "Notes": "No dig_ prefix"},
            {"Order": 21, "Concept": "Project Idea Link", "Role in Flow": "Links Project to Idea(s)", "Target Table": "dig_project_idea_link", "Notes": "Supports one or many Ideas"},
            {"Order": 22, "Concept": "Product/SKU", "Role in Flow": "Product/SKU reference", "Target Table": "dig_product_sku", "Notes": "For SKU-origin projects"},
            {"Order": 23, "Concept": "Project SKU Link", "Role in Flow": "Links Project to SKU(s)", "Target Table": "dig_project_sku_link", "Notes": "Supports one or many SKUs"},
            {"Order": 24, "Concept": "Notification", "Role in Flow": "User-facing alerts", "Target Table": "dig_notification", "Notes": "Not technical log"},
            {"Order": 25, "Concept": "Business Audit Event", "Role in Flow": "Business timeline/history", "Target Table": "dig_business_audit_event", "Notes": "Who did what and when"},
            {"Order": 26, "Concept": "Technical Log Event", "Role in Flow": "Operational/system logs", "Target Table": "dig_technical_log_event", "Notes": "Backend/ETL/API monitoring"},
        ],
        "end_to_end_summary": [
            {
                "Path": "With Journey",
                "End-to-End Flow": (
                    "User -> Journey -> Module -> Submodule -> Task -> Idea -> Assessment -> "
                    "Snapshot -> Assessor Assignment -> Score -> Prioritisation Decision -> Project"
                ),
            },
            {
                "Path": "Without Journey",
                "End-to-End Flow": (
                    "User -> Module -> Submodule -> Task -> Idea -> Assessment -> Snapshot -> "
                    "Assessor Assignment -> Score -> Prioritisation Decision -> Project"
                ),
            },
            {
                "Path": "Direct Idea",
                "End-to-End Flow": (
                    "User -> Idea -> Assessment -> Snapshot -> Assessor Assignment -> Score -> "
                    "Prioritisation Decision -> Project"
                ),
            },
            {
                "Path": "Project from SKU",
                "End-to-End Flow": "User -> Project Details -> Project SKU Link -> Product/SKU",
            },
            {"Path": "Project from scratch", "End-to-End Flow": "User -> Project Details only"},
        ],
        "key_modeling_rules": [
            {
                "Rule": "Journey is optional",
                "Explanation": "User may start with Journey, Module or Idea.",
                "Implementation": "dig_module_instance and dig_idea do not require journey_id.",
            },
            {
                "Rule": "Module is reusable",
                "Explanation": "Same module can belong to multiple Journeys.",
                "Implementation": "dig_journey_module_link bridge table.",
            },
            {
                "Rule": "Ideas are global",
                "Explanation": "Ideas become visible for assessment regardless of creation path.",
                "Implementation": "dig_idea is independent; optional link tables add context.",
            },
            {
                "Rule": "Task-level idea lineage",
                "Explanation": "Idea can be traced to exact Task, then Submodule, then Module.",
                "Implementation": (
                    "dig_task_idea_link -> dig_task_instance -> dig_submodule_instance -> "
                    "dig_module_instance."
                ),
            },
            {
                "Rule": "Assessment snapshot",
                "Explanation": "Assessment must score the Idea as it existed at initiation time.",
                "Implementation": "dig_assessment_idea_snapshot stores frozen copy.",
            },
            {
                "Rule": "D/V/F scoring",
                "Explanation": "Desirability, Viability and Feasibility are criteria, not hardcoded columns.",
                "Implementation": (
                    "dig_ref_assessment_criterion, dig_assessor_assignment, dig_assessment_score."
                ),
            },
            {
                "Rule": "Project is generic",
                "Explanation": "Project can be from Idea, SKU/Product or scratch.",
                "Implementation": "project_details plus dig_project_idea_link and dig_project_sku_link.",
            },
            {
                "Rule": "Business audit vs technical log",
                "Explanation": (
                    "Business audit records user/business actions; technical log records "
                    "system/process issues."
                ),
                "Implementation": "dig_business_audit_event and dig_technical_log_event.",
            },
        ],
    }
}


def available_scrum_teams() -> list[str]:
    """Return the scrum teams that have business flow workbook content."""
    return sorted(BUSINESS_FLOW_DATA.keys(), key=str.casefold)


def section_dataframe(scrum_team: str, section: str) -> pd.DataFrame:
    """Build a dataframe for a scrum team's selected business flow section."""
    if scrum_team not in BUSINESS_FLOW_DATA:
        raise ValueError(f"Unknown scrum team: {scrum_team}")
    if section not in SECTION_DATA_KEYS:
        raise ValueError(f"Unknown business flow section: {section}")

    data_key = SECTION_DATA_KEYS[section]
    columns = SECTION_COLUMNS[section]
    rows = BUSINESS_FLOW_DATA[scrum_team][data_key]
    return pd.DataFrame(rows, columns=columns)
