#!/usr/bin/env bash
# Init script: create the Hive Metastore DB + role on first PG volume init.
# Mounted into postgres:/docker-entrypoint-initdb.d/.
# Runs BEFORE 02-init-research-schema.sh.

set -euo pipefail

: "${HMS_DB_NAME:=hive_metastore}"
: "${HMS_DB_USER:=hive}"
: "${HMS_DB_PASSWORD:=hive_password}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
DO
\$\$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = '${HMS_DB_USER}'
    ) THEN
        CREATE ROLE ${HMS_DB_USER} LOGIN PASSWORD '${HMS_DB_PASSWORD}';
    END IF;
END
\$\$;

SELECT 'CREATE DATABASE ${HMS_DB_NAME} OWNER ${HMS_DB_USER}'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${HMS_DB_NAME}')
\gexec

GRANT ALL PRIVILEGES ON DATABASE ${HMS_DB_NAME} TO ${HMS_DB_USER};
EOSQL