alter table if exists catalog_tables
add column if not exists owning_team text,
add column if not exists metadata_profile_json text;

alter table if exists catalog_columns
add column if not exists attribute_type text,
add column if not exists attribute_type_name text,
add column if not exists is_custom_attribute boolean,
add column if not exists is_valid_odata_attribute boolean,
add column if not exists source_type integer,
add column if not exists source_type_label text,
add column if not exists max_length integer,
add column if not exists precision integer,
add column if not exists min_value text,
add column if not exists max_value text,
add column if not exists targets text,
add column if not exists option_values text,
add column if not exists category text,
add column if not exists modeling_action text,
add column if not exists is_primary_id boolean,
add column if not exists is_primary_name boolean,
add column if not exists is_state_machine_candidate boolean;
