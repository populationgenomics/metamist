from io import BytesIO, StringIO
from typing import Any, Callable

# Unfortunately some of these libs have partially or completely missing
# type annotations so mypy will have a few red underlines in this file :/
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from databases.interfaces import Record

from db.python.tables.base import DbBase
from models.models import PRIMARY_EXTERNAL_ORG

# Use this value to replace the PRIMARY_EXTERNAL_ORG value in the json object returned
# from the query. DuckDB doesn't support column names which are an empty string.
EXTERNAL_ORG_SENTINEL = '__PRIMARY__EXTERNAL__ORG__'


class MetaTable(DbBase):
    """
    Generate a flat table with metadata for all entity tables
    """

    def external_id_query(self, table_alias: str):
        """
        Returns the query partial to aggregate external ids in to a json object
        including replacing the primary external org with a sentinel value. This is
        required because duckdb doesn't support column names which are an empty string.
        """
        return f"""
            JSON_OBJECTAGG(
                CASE
                    WHEN {table_alias}.name = :primary_external_org
                    THEN :primary_external_org_sentinel
                    ELSE {table_alias}.name
                END,
                {table_alias}.external_id
            ) AS external_ids
        """

    async def entity_meta_table(
        self,
        project: int,
        query: str,
        row_getter: Callable[[Record], dict[str, Any]],
        has_external_ids: bool,
        has_meta: bool,
    ):
        """
        Return a flat tabular parquet file for the provied query. Optionally include
        metadata and external_ids columns. This returns a BytesIO object with the
        parquet file.
        """

        rows = await self.connection.fetch_all(
            query,
            {
                'project': project,
                'primary_external_org': PRIMARY_EXTERNAL_ORG,
                'primary_external_org_sentinel': EXTERNAL_ORG_SENTINEL,
            },
        )

        if len(rows) == 0:
            return None

        duck = duckdb.connect()
        meta_rows_str = StringIO()
        external_id_rows_str = StringIO()
        main_rows: list[dict[str, Any]] = []

        for row in rows:
            if has_meta:
                meta_rows_str.write((row['meta'] or 'null') + '\n')

            if has_external_ids:
                external_id_rows_str.write(
                    (row['external_ids'] or f'{{"{EXTERNAL_ORG_SENTINEL}": null}}')
                    + '\n'
                )

            main_rows.append(row_getter(row))

        duck.register('main_table', pa.Table.from_pylist(main_rows))

        # Seek str buffers back to beginning
        meta_rows_str.seek(0)
        external_id_rows_str.seek(0)

        # parse the json for the meta table
        meta_columns = ''
        meta_join = ''
        if has_meta:
            duck.register(
                'meta_table',
                duck.read_json(
                    meta_rows_str,
                    map_inference_threshold=-1,
                    format='newline_delimited',
                ).arrow(),
            )
            # Prefix all meta columns with `meta_` to avoid clashes with the main table
            meta_columns = """
                ,COLUMNS(mt.*) as 'meta_\\0'
            """
            meta_join = 'POSITIONAL JOIN meta_table mt'

        # parse the json for the external_id table
        external_id_columns = ''
        external_id_join = ''
        if has_external_ids:
            duck.register(
                'external_id_table',
                duck.read_json(
                    external_id_rows_str,
                    format='newline_delimited',
                ).arrow(),
            )

            # call the primary external_id column `external_id` and prefix all other
            # columns with `external_id_` to avoid clashes with the main table
            external_id_columns = f"""
                ,et.{EXTERNAL_ORG_SENTINEL} as external_id
                ,COLUMNS(et.* EXCLUDE {EXTERNAL_ORG_SENTINEL}) as 'external_id_\\0'
            """
            external_id_join = 'POSITIONAL JOIN external_id_table et'

        # combine the three tables together, if they are populated
        combined = duck.query(f"""
            SELECT
                m.*
                {external_id_columns}
                {meta_columns}

            FROM main_table m
            {external_id_join}
            {meta_join}
        """)

        buffer = BytesIO()
        pq.write_table(combined.arrow(), buffer)

        duck.close()
        return buffer
