create table if not exists journeys (
  journey_id text primary key,
  journey_name text not null,
  module_domain text not null,
  primary_user_role text,
  frequency text,
  complexity text,
  interview_date date,
  interviewer text,
  scrum_team text,
  total_steps integer default 0,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  last_modified_by text
);

create table if not exists journey_steps (
  id bigint generated always as identity primary key,
  journey_id text not null references journeys(journey_id) on delete cascade,
  step_number integer not null,
  user_action text not null,
  screen_component text,
  status_field_changes text,
  validation_rules text,
  business_rules text,
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (journey_id, step_number)
);

create table if not exists journey_step_tables (
  id bigint generated always as identity primary key,
  journey_id text not null references journeys(journey_id) on delete cascade,
  step_number integer not null,
  table_name text not null,
  access_mode text not null,
  write_operation text,
  is_catalog_table boolean default false,
  catalog_table_key text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists state_transitions (
  id bigint generated always as identity primary key,
  journey_id text not null references journeys(journey_id) on delete cascade,
  step_number integer not null,
  entity_table text not null,
  status_field_name text not null,
  from_state text,
  to_state text not null,
  trigger_action text not null,
  user_role_required text,
  validation_rules text,
  side_effects text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);
