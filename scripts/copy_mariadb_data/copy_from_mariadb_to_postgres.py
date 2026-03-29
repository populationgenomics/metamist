# /// script
# dependencies = [
#   "duckdb",
#   "click",
#   "google-cloud-secret-manager",
# ]
# ///


import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import duckdb
from google.cloud import secretmanager


@dataclass
class ColumnInfo:
    """Information about a database column."""

    source_name: str  # mariadb column name
    is_json: bool = False  # whether the column contains JSON data
    dest_name: str | None = None  # postgres column name
    is_deleted: bool = False  # whether the column isn't present in postgres
    is_added: bool = False  # whether the column is new in postgres
    to_lowercase: bool = False  # whether to convert value to lowercase


@dataclass
class TableInfo:
    """Information about a database table."""

    name: str
    columns: list[ColumnInfo]
    has_system_versioning: bool = False


@dataclass
class DbCreds:
    """Connection details for database"""

    database: str
    username: str
    password: str
    port: int
    host: str

    @staticmethod
    def from_secret(gcp_project: str, secret_name: str):
        name = f'projects/{gcp_project}/secrets/{secret_name}/versions/latest'
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={'name': name})
        data: dict[str, Any] = json.loads(response.payload.data.decode('UTF-8'))

        return DbCreds(
            database=data.get('database'),
            password=data.get('password'),
            username=data.get('username'),
            port=int(data.get('port')),
            host=data.get('host'),
        )


duck = duckdb.connect(database=':memory:')


def setup_secrets(gcp_project: str):  # noqa: D103

    mariadb_creds = DbCreds.from_secret(gcp_project, 'data-copy-mariadb-creds')
    postgres_creds = DbCreds.from_secret(gcp_project, 'data-copy-postgres-creds')

    duck.execute(f"""
    CREATE SECRET (
       TYPE mysql,
       HOST '{mariadb_creds.host}',
       PORT {mariadb_creds.port},
       DATABASE '{mariadb_creds.database}',
       USER '{mariadb_creds.username}',
       PASSWORD '{mariadb_creds.password}'
    )
    """)

    duck.execute(f"""
    CREATE SECRET (
       TYPE postgres,
       HOST '{postgres_creds.host}',
       PORT {postgres_creds.port},
       DATABASE '{postgres_creds.database}',
       USER '{postgres_creds.username}',
       PASSWORD '{postgres_creds.password}'
    )
    """)

    return mariadb_creds, postgres_creds


tables = [
    TableInfo(
        name='analysis',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='type'),
            ColumnInfo(source_name='output'),
            ColumnInfo(source_name='status'),
            ColumnInfo(source_name='timestamp_completed'),
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='active'),
            ColumnInfo(source_name='on_behalf_of'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='analysis_cohort',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='cohort_id'),
            ColumnInfo(source_name='analysis_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='analysis_outputs',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='analysis_id'),
            ColumnInfo(source_name='file_id'),
            ColumnInfo(source_name='output'),
            ColumnInfo(source_name='json_structure'),
            ColumnInfo(source_name='audit_log_id', is_added=True),
        ],
    ),
    TableInfo(
        name='analysis_runner',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='ar_guid'),
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='timestamp'),
            ColumnInfo(source_name='access_level'),
            ColumnInfo(source_name='repository'),
            ColumnInfo(source_name='commit'),
            ColumnInfo(source_name='output_path'),
            ColumnInfo(source_name='script'),
            ColumnInfo(source_name='description'),
            ColumnInfo(source_name='driver_image'),
            ColumnInfo(source_name='config_path'),
            ColumnInfo(source_name='cwd'),
            ColumnInfo(source_name='environment'),
            ColumnInfo(source_name='hail_version'),
            ColumnInfo(source_name='batch_url'),
            ColumnInfo(source_name='submitting_user'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='analysis_sequencing_group',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='analysis_id'),
            ColumnInfo(source_name='sequencing_group_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='analysis_type',
        has_system_versioning=False,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='assay',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='sample_id'),
            ColumnInfo(source_name='type'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='assay_comment',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='comment_id'),
            ColumnInfo(source_name='assay_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='assay_external_id',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='assay_id'),
            ColumnInfo(source_name='external_id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='assay_type',
        has_system_versioning=False,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='audit_log',
        has_system_versioning=False,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='timestamp'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='on_behalf_of'),
            ColumnInfo(source_name='ar_guid'),
            ColumnInfo(source_name='comment'),
            ColumnInfo(source_name='auth_project'),
            ColumnInfo(source_name='meta', is_json=True),
        ],
    ),
    TableInfo(
        name='cohort',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='template_id'),
            ColumnInfo(source_name='description'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='timestamp'),
            ColumnInfo(source_name='audit_log_id'),
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='status'),
        ],
    ),
    TableInfo(
        name='cohort_sequencing_group',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='cohort_id'),
            ColumnInfo(source_name='sequencing_group_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='cohort_template',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='description'),
            ColumnInfo(source_name='criteria', is_json=True),
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='comment',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='parent_id'),
            ColumnInfo(source_name='content'),
            ColumnInfo(source_name='status'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='family',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(
                source_name='_external_id_unused',
                is_deleted=True,
            ),
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='description'),
            ColumnInfo(source_name='coded_phenotype'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='family_comment',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='comment_id'),
            ColumnInfo(source_name='family_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='family_external_id',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='family_id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='external_id'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='family_participant',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='family_id'),
            ColumnInfo(source_name='participant_id'),
            ColumnInfo(source_name='paternal_participant_id'),
            ColumnInfo(source_name='maternal_participant_id'),
            ColumnInfo(source_name='affected'),
            ColumnInfo(source_name='notes'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='group',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='group_member',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='group_id'),
            ColumnInfo(source_name='member'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='output_file',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='path'),
            ColumnInfo(source_name='basename'),
            ColumnInfo(source_name='dirname'),
            ColumnInfo(source_name='nameroot'),
            ColumnInfo(source_name='nameext'),
            ColumnInfo(source_name='file_checksum'),
            ColumnInfo(source_name='size'),
            ColumnInfo(
                source_name='meta', is_json=True
            ),  # VARCHAR in MariaDB, JSONB in Postgres
            ColumnInfo(source_name='valid'),
            ColumnInfo(source_name='parent_id'),
            ColumnInfo(source_name='audit_log_id', is_added=True),
        ],
    ),
    TableInfo(
        name='participant',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(
                source_name='_external_id_unused',
                is_deleted=True,
            ),
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='reported_sex'),
            ColumnInfo(source_name='reported_gender'),
            ColumnInfo(source_name='karyotype'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='participant_comment',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='comment_id'),
            ColumnInfo(source_name='participant_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='participant_external_id',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='participant_id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='external_id'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='participant_phenotypes',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='participant_id'),
            ColumnInfo(source_name='hpo_term'),
            ColumnInfo(source_name='description'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='value', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='project',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='dataset'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='project_comment',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='comment_id'),
            ColumnInfo(source_name='project_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='project_member',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='project_id'),
            ColumnInfo(source_name='member'),
            ColumnInfo(source_name='role'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sample',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(
                source_name='_external_id_unused',
                is_deleted=True,
            ),
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='participant_id'),
            ColumnInfo(source_name='active'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='type', to_lowercase=True),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='audit_log_id'),
            ColumnInfo(source_name='sample_root_id'),
            ColumnInfo(source_name='sample_parent_id'),
        ],
    ),
    TableInfo(
        name='sample_comment',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='comment_id'),
            ColumnInfo(source_name='sample_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sample_external_id',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='sample_id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='external_id'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sample_type',
        has_system_versioning=False,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sequencing_group',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='sample_id'),
            ColumnInfo(source_name='type'),
            ColumnInfo(source_name='technology'),
            ColumnInfo(source_name='platform'),
            ColumnInfo(source_name='meta', is_json=True),
            ColumnInfo(source_name='archived'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sequencing_group_assay',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='sequencing_group_id'),
            ColumnInfo(source_name='assay_id'),
            ColumnInfo(source_name='author'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sequencing_group_comment',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='comment_id'),
            ColumnInfo(source_name='sequencing_group_id'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sequencing_group_external_id',
        has_system_versioning=True,
        columns=[
            ColumnInfo(source_name='project'),
            ColumnInfo(source_name='sequencing_group_id'),
            ColumnInfo(source_name='external_id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='author'),
            ColumnInfo(
                source_name='nullIfInactive', dest_name='null_if_archived'
            ),  # Column renamed
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sequencing_platform',
        has_system_versioning=False,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sequencing_technology',
        has_system_versioning=False,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
    TableInfo(
        name='sequencing_type',
        has_system_versioning=False,
        columns=[
            ColumnInfo(source_name='id'),
            ColumnInfo(source_name='name'),
            ColumnInfo(source_name='audit_log_id'),
        ],
    ),
]


def download_mariadb_data(mariadb_creds: DbCreds):  # noqa: D103
    duck.execute(f"""
    ATTACH 'database={mariadb_creds.database}' AS mysql_db (TYPE mysql);
    """)

    for table in tables:
        print(f'getting {table.name} table...')

        columns: list[str] = []

        for col in table.columns:
            if col.is_deleted or col.is_added:
                continue

            source_name = col.source_name
            dest_name = col.dest_name or col.source_name
            # There's a bit of an issue with DuckDB's handling of JSON columns from mariadb.
            # double quotes get escaped as \x22 which can't then be parsed as JSON in Postgres.
            # To get around this cast the column as binary which retrieves the raw data.
            col_select = (
                f'CAST({source_name} AS binary)' if col.is_json else source_name
            )

            if col.to_lowercase:
                col_select = f'LOWER({col_select})'

            columns.append(f'{col_select} AS {dest_name}')

        if table.has_system_versioning:
            columns.append('row_start')
            columns.append('row_end')

        system_time_condition = (
            ' FOR SYSTEM_TIME ALL' if table.has_system_versioning else ''
        )

        select_text = ', '.join(columns)

        duck.execute(f"""
            COPY (
                SELECT * FROM mysql_query(
                    'mysql_db',
                    'select {select_text} from {mariadb_creds.database}.{table.name}{system_time_condition};'
                )
            ) TO './data/{table.name}.parquet';
        """)


skip_tables = []

# Tables with IDENTITY columns that need sequence resets
identity_tables = [
    ('analysis', 'id'),
    ('assay', 'id'),
    ('audit_log', 'id'),
    ('cohort', 'id'),
    ('cohort_template', 'id'),
    ('comment', 'id'),
    ('family', 'id'),
    ('"group"', 'id'),
    ('output_file', 'id'),
    ('participant', 'id'),
    ('project', 'id'),
    ('sample', 'id'),
    ('sequencing_group', 'id'),
]


def reset_identity_sequences():
    """Reset PostgreSQL identity sequences to the max value in each table."""
    # duck.execute("""
    # ATTACH '' AS pg_db (TYPE postgres);
    # """)

    for table_name, column_name in identity_tables:
        print(f'Resetting sequence for {table_name}.{column_name}...')
        # Use pg_get_serial_sequence to get the sequence name and setval to reset it
        duck.execute(f"""
            SELECT * FROM postgres_query(
                'pg_db',
                'SELECT setval(pg_get_serial_sequence(''main.{table_name}'', ''{column_name}''), COALESCE((SELECT MAX({column_name}) FROM main.{table_name}), 1))'
            );
        """)

    print('All identity sequences reset successfully.')


def insert_postgres_data():  # noqa: D103

    for table in tables:
        if table.name in skip_tables:
            continue
        print(f'inserting {table.name} table...')

        # Build column lists for INSERT and SELECT
        insert_columns = []
        select_columns = []

        for col in table.columns:
            if col.is_deleted:
                continue

            dest_name = col.dest_name or col.source_name
            insert_columns.append(dest_name)

            if col.is_added:
                # For added columns, select NULL
                select_columns.append(f'NULL as {dest_name}')
            else:
                # For existing columns, select from parquet
                col_expr = dest_name
                if col.to_lowercase:
                    col_expr = f'LOWER({col_expr})'
                select_columns.append(f'{col_expr} as {dest_name}')
        if not table.has_system_versioning:
            insert_cols_str = '(' + ', '.join(insert_columns) + ')'
            select_cols_str = ', '.join(select_columns)

            duck.execute(f"""
                INSERT INTO pg_db.main.{table.name} {insert_cols_str}
                SELECT {select_cols_str}
                FROM './data/{table.name}.parquet';
            """)

        else:
            # For system versioned tables, add sys_period to columns
            insert_columns_with_period = insert_columns + ['sys_period']
            insert_cols_str = '(' + ', '.join(insert_columns_with_period) + ')'
            select_cols_str = ', '.join(select_columns)

            # We've made changes to the unique index for the analysis_outputs table to
            # fix a bug where we'd get duplicate analysis outputs due to null values in
            # the json_structure column. The updated index coalesces the json_structure
            # value to '' (COALESCE(json_structure, '')) to avoid these duplicates.
            # The mariadb database still has duplicates so we need to remove them here
            # This uses duckdb's qualify clause to ensure there is only one row for each
            # unique analysis output combo: https://duckdb.org/docs/stable/sql/query_syntax/qualify
            qualify_clause = ''
            if table.name == 'analysis_outputs':
                qualify_clause = "QUALIFY ROW_NUMBER() OVER (PARTITION BY analysis_id, file_id, output, COALESCE(json_structure, '') ORDER BY row_start DESC) = 1"

            print('inserting main table data...')
            duck.execute(f"""
                INSERT INTO pg_db.main.{table.name} {insert_cols_str}
                SELECT {select_cols_str}, '["' || row_start || '",)' as sys_period
                FROM './data/{table.name}.parquet'
                WHERE row_end > now()
                {qualify_clause};
            """)

            print('inserting history table data...')
            duck.execute(f"""
                INSERT INTO pg_db.history.{table.name}_history {insert_cols_str}
                SELECT {select_cols_str}, '["' || row_start || '","' || row_end || '")' as sys_period
                FROM './data/{table.name}.parquet'
                WHERE row_end <= now();
            """)


def run_dbmate(
    command: list[str],
    db_url: str,
    dbmate_path: str = 'dbmate',
    migrations_dir: str = '../../db/migrations',
):
    """Run a dbmate command."""
    env = os.environ.copy()
    env['DATABASE_URL'] = db_url
    cmd = [dbmate_path, '--migrations-dir', migrations_dir] + command
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        click.secho(f'Error running {" ".join(cmd)}', fg='red', err=True)
        click.echo(result.stdout, err=True)
        click.echo(result.stderr, err=True)
        sys.exit(1)
    return result.stdout


def get_applied_migrations(
    db_url: str,
    dbmate_path: str = 'dbmate',
    migrations_dir: str = '../../db/migrations',
):
    """Get the list of applied migration versions (timestamps)."""
    output = run_dbmate(['status'], db_url, dbmate_path, migrations_dir)
    applied: list[str] = []
    for line in output.splitlines():
        if line.strip().startswith('[X]'):
            # e.g., "[X] 20260110061420_create_roles.sql"
            parts = line.strip().split()
            if len(parts) >= 2:  # noqa: PLR2004
                # parts[1] is the filename, e.g., "20260110061420_create_roles.sql"
                applied.append(parts[1].split('_')[0])
    applied.sort()
    return applied


def drop_other_connections(postgres_creds: DbCreds):
    """Drop all other active connections to the Postgres database."""
    # Use pg_terminate_backend to force other clients to reconnect
    duck.execute(f"""
        SELECT * FROM postgres_query(
            'pg_db',
            'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = ''{postgres_creds.database}'' AND pid <> pg_backend_pid() AND backend_type = ''client backend'''
        );
    """)


@click.command()
@click.option(
    '--project', required=True, help='GCP Project ID where secrets are stored'
)
@click.option(
    '--use-local-data', is_flag=True, help='Use existing parquet files in ./data/'
)
@click.option('--dbmate-path', default='dbmate', help='Path to dbmate executable')
@click.option(
    '--migrations-dir',
    default='../../db/migrations',
    help='Path to migrations directory',
)
def main(
    project: str,
    use_local_data: bool,
    dbmate_path: str,
    migrations_dir: str,
):
    """Migrate data from MariaDB to PostgreSQL."""
    click.echo('Fetching credentials from Secret Manager...')
    mariadb_creds, postgres_creds = setup_secrets(project)

    # 1. Print connection details
    click.echo('--- Database Connection Details ---')
    click.echo(
        f'Source (MariaDB):  {mariadb_creds.host} / {mariadb_creds.database} (User: {mariadb_creds.username})'
    )
    click.echo(
        f'Dest (PostgreSQL): {postgres_creds.host} / {postgres_creds.database} (User: {postgres_creds.username})'
    )
    click.echo('-----------------------------------')

    # 2. User Confirmation
    if not click.confirm(
        'Are these connection details correct? This will DELETE ALL DATA in the destination Postgres database.',
        default=False,
    ):
        click.echo('Aborted.')
        return

    # Prepare Postgres URL for dbmate
    pg_url = f'postgres://{postgres_creds.username}:{urllib.parse.quote(postgres_creds.password)}@{postgres_creds.host}:{postgres_creds.port}/{postgres_creds.database}?search_path=public,main,history'

    # 3. Preparation (Local Data)
    data_dir = Path('./data')
    if not use_local_data:
        if data_dir.exists():
            click.echo('Clearing local data directory...')
            for f in data_dir.glob('*.parquet'):
                f.unlink()
        else:
            data_dir.mkdir(parents=True, exist_ok=True)
    elif not data_dir.exists():
        click.echo('Error: --use-local-data set but ./data/ directory does not exist.')
        sys.exit(1)

    # 4. Preparation (Database State)
    # Target versions (timestamps)
    # 20260120061420_create_tables.sql
    # 20260120061532_create_history_tables.sql
    create_tables_version = '20260120061420'
    history_tables_version = '20260120061532'

    click.echo('Checking database migration status...')
    applied = get_applied_migrations(pg_url, dbmate_path, migrations_dir)

    # Migrate DOWN until create_tables is undone
    while create_tables_version in applied:
        click.echo(f'Rolling back migration: {applied[-1]}...')
        run_dbmate(['down'], pg_url, dbmate_path, migrations_dir)
        applied = get_applied_migrations(pg_url, dbmate_path, migrations_dir)

    # Migrate UP until history_tables is applied
    # Temporarily move later migrations out of the way
    migrations_path = Path(migrations_dir)
    all_migrations = sorted(migrations_path.glob('*.sql'))
    later_migrations = [
        m for m in all_migrations if m.name.split('_')[0] > history_tables_version
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        for m in later_migrations:
            shutil.move(str(m), str(tmp_path / m.name))

        try:
            click.echo(f'Applying migrations up to {history_tables_version}...')
            run_dbmate(['up'], pg_url, dbmate_path, migrations_dir)
        finally:
            # Move migrations back
            for f in tmp_path.glob('*.sql'):
                shutil.move(str(f), str(migrations_path / f.name))

    # 5. Data Extraction
    if not use_local_data:
        click.echo('Starting download from MariaDB...')
        download_mariadb_data(mariadb_creds=mariadb_creds)
    else:
        click.echo('Using local data (skipping MariaDB download)...')

    # 6. Data Insertion
    # attach postgres database
    click.echo('Attaching Postgres database to DuckDB...')
    duck.execute("ATTACH '' AS pg_db (TYPE postgres);")

    click.echo('Inserting data into Postgres...')

    insert_postgres_data()

    # 7. Finalization (Identity Sequences)
    click.echo('Resetting identity sequences...')
    reset_identity_sequences()

    # 8. Final DB Migrations
    if click.confirm(
        'Data migration complete. Do you want to apply the remaining database migrations (triggers, indexes, etc.)?',
        default=True,
    ):
        click.echo('Applying remaining migrations...')
        run_dbmate(['up'], pg_url, dbmate_path, migrations_dir)
        click.echo('Migrations applied successfully.')
    else:
        click.echo('Skipped final migrations.')

    # 9. Force client reconnection, otherwise psycopg will have the wrong OIDs registered
    # for enums
    drop_other_connections(postgres_creds)

    click.echo('Done!')


if __name__ == '__main__':
    main()
