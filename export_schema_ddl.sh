#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 user/password@database output_directory" >&2
  echo "Example: $0 scott/tiger@orclpdb /tmp/scott_ddl" >&2
  exit 2
fi

if ! command -v exec_sql >/dev/null 2>&1; then
  echo "Error: exec_sql function or command is not available." >&2
  echo "Define exec_sql so it accepts: exec_sql connect_string script_path" >&2
  exit 127
fi

CONNECT_STRING="$1"
OUTPUT_DIR="$2"
WORK_DIR="$(mktemp -d)"
SQL_SCRIPT="$WORK_DIR/export_schema_ddl.sql"

cleanup() {
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

cat > "$SQL_SCRIPT" <<'SQL'
-- Export the currently connected Oracle user's DDL to one client-side file per object.
-- Files are written by SQL*Plus SPOOL on the client machine.

set echo off
set verify off
set feedback off
set heading off
set pagesize 0
set linesize 32767
set long 1000000000
set longchunksize 32767
set trimspool on
set termout off

define output_dir = '__OUTPUT_DIR__'
define runner_file = 'export_schema_ddl_run.sql'

column ddl_command format a32767

spool &&runner_file

prompt set echo off
prompt set verify off
prompt set feedback off
prompt set heading off
prompt set pagesize 0
prompt set linesize 32767
prompt set long 1000000000
prompt set longchunksize 32767
prompt set trimspool on
prompt set termout on
prompt column ddl format a32767
prompt
prompt exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'PRETTY', true);
prompt exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SQLTERMINATOR', true);
prompt exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'CONSTRAINTS', false);
prompt exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'REF_CONSTRAINTS', false);
prompt exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'CONSTRAINTS_AS_ALTER', true);
prompt exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', true);
prompt exec dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'STORAGE', true);
prompt

with params as (
  select user as owner,
         rtrim(replace('&&output_dir', '\', '/'), '/') as output_dir
  from dual
),
objects_to_export as (
  select p.owner,
         o.object_type,
         o.object_name,
         case o.object_type
           when 'DATABASE LINK' then 'DB_LINK'
           when 'MATERIALIZED VIEW' then 'MATERIALIZED_VIEW'
           when 'PACKAGE BODY' then 'PACKAGE_BODY'
           when 'TYPE BODY' then 'TYPE_BODY'
           else replace(o.object_type, ' ', '_')
         end as metadata_type,
         null as filename_suffix,
         10 as sort_group
  from user_objects o
  cross join params p
  where o.object_type in (
    'TABLE',
    'INDEX',
    'VIEW',
    'SEQUENCE',
    'PROCEDURE',
    'FUNCTION',
    'PACKAGE',
    'PACKAGE BODY',
    'TRIGGER',
    'TYPE',
    'TYPE BODY',
    'SYNONYM',
    'MATERIALIZED VIEW',
    'DATABASE LINK'
  )
  and o.object_name not like 'BIN$%'
  union all
  select p.owner,
         'CONSTRAINT' as object_type,
         c.constraint_name as object_name,
         case c.constraint_type
           when 'R' then 'REF_CONSTRAINT'
           else 'CONSTRAINT'
         end as metadata_type,
         case c.constraint_type
           when 'R' then '_fk'
           else null
         end as filename_suffix,
         20 as sort_group
  from user_constraints c
  cross join params p
  where c.constraint_type in ('C', 'P', 'U', 'R')
  and c.constraint_name not like 'BIN$%'
),
file_commands as (
  select e.sort_group,
         e.object_type,
         e.object_name,
         e.metadata_type,
         p.output_dir || '/' ||
           regexp_replace(
             lower(e.owner || '_' || e.object_type || '_' || e.object_name || e.filename_suffix || '.sql'),
             '[^[:alnum:]_.-]+',
             '_'
           ) as file_path
  from objects_to_export e
  join params p on p.owner = e.owner
)
select 'prompt Writing ' || file_path || chr(10) ||
       'spool "' || file_path || '"' || chr(10) ||
       'select dbms_metadata.get_ddl(''' ||
          metadata_type || ''', ''' ||
          replace(object_name, '''', '''''') || ''') as ddl from dual;' || chr(10) ||
       'spool off' || chr(10) as ddl_command
from file_commands
order by sort_group, object_type, object_name;

prompt
prompt prompt Done.
prompt exit

spool off

set termout on
prompt Generated &&runner_file
prompt Exporting current schema to client directory &&output_dir

@&&runner_file
SQL

escaped_output_dir="$(printf '%s' "$OUTPUT_DIR" | sed "s/[\/&]/\\\\&/g; s/'/''/g")"
sed "s/__OUTPUT_DIR__/$escaped_output_dir/g" "$SQL_SCRIPT" > "$SQL_SCRIPT.tmp"
mv "$SQL_SCRIPT.tmp" "$SQL_SCRIPT"

cd "$WORK_DIR"
exec_sql "$CONNECT_STRING" "$SQL_SCRIPT"
