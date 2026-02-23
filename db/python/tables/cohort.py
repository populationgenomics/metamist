import dataclasses
import datetime

from psycopg import sql
from psycopg.rows import class_row, scalar_row
from psycopg.types.json import Jsonb

from db.python.filters import GenericFilter, GenericFilterModel
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError
from models.base import parse_sql_bool
from models.enums.cohort import CohortStatus, CohortUpdateStatus
from models.models.cohort import (
    CohortCriteriaInternal,
    CohortInternal,
    CohortTemplateInternal,
    NewCohortInternal,
)
from models.models.project import ProjectId


@dataclasses.dataclass(kw_only=True)
class CohortFilter(GenericFilterModel):
    """
    Filters for Cohort
    """

    id: GenericFilter[int] | None = None
    name: GenericFilter[str] | None = None
    author: GenericFilter[str] | None = None
    template_id: GenericFilter[int] | None = None
    timestamp: GenericFilter[datetime.datetime] | None = None
    project: GenericFilter[ProjectId] | None = None
    status: GenericFilter[CohortStatus] | None = None


@dataclasses.dataclass(kw_only=True)
class CohortTemplateFilter(GenericFilterModel):
    """
    Filters for CohortTemplate
    """

    id: GenericFilter[int] | None = None
    name: GenericFilter[str] | None = None
    description: GenericFilter[str] | None = None
    criteria: GenericFilter[dict] | None = None
    project: GenericFilter[ProjectId] | None = None


class CohortTable(DbBase):
    """
    Capture Cohort table operations and queries
    """

    async def query(
        self, filter_: CohortFilter
    ) -> tuple[list[CohortInternal], set[ProjectId]]:
        """Query Cohorts"""

        filter_status = filter_.status
        filter_.status = None  # reset filter and use to filter on the rows fetched

        where_params = filter_.to_sql(
            field_overrides={
                'id': 'c.id',
                'name': 'c.name',
                'template_id': 'c.template_id',
                'author': 'c.author',
                'project': 'c.project',
            }
        )
        if where_params is None:
            raise ValueError(f'Invalid filter: {filter_}')

        _query = t"""
        SELECT
        id, name, template_id, description, author, project, status,
        exists (
            select 1
            from cohort_sequencing_group csg
            join sequencing_group sg
            on sg.id = csg.sequencing_group_id
            join sample s
            on s.id = sg.sample_id
            where csg.cohort_id = c.id
            and (sg.archived or not s.active)
        ) as is_invalid
        FROM cohort c
        WHERE {where_params:q}
        """

        rows = await (await self.connection.pg_connection.execute(_query)).fetchall()
        cohorts_list = []
        for cohort_row in rows:
            is_active = cohort_row['status'] == CohortStatus.active.value
            is_invalid = cohort_row.pop('is_invalid')

            if is_active:
                cohort_status = (
                    CohortStatus.invalid if is_invalid else CohortStatus.active
                )
            else:
                cohort_status = CohortStatus.archived

            if _custom_matches_filter(cohort_status, filter_status):
                cohort_row['status'] = cohort_status
                cohorts_list.append(CohortInternal(**cohort_row))

        projects = {c.project for c in cohorts_list}
        return cohorts_list, projects

    async def get_cohort_sequencing_group_ids(self, cohort_id: int) -> list[int]:
        """
        Return all sequencing group IDs for the given cohort.
        """
        _query = t'SELECT sequencing_group_id FROM cohort_sequencing_group WHERE cohort_id = {cohort_id}'

        rows = await (await self.connection.pg_connection.execute(_query)).fetchall()
        return [row['sequencing_group_id'] for row in rows]

    async def query_cohort_templates(
        self, filter_: CohortTemplateFilter
    ) -> tuple[set[ProjectId], list[CohortTemplateInternal]]:
        """Query CohortTemplates"""

        wheres_params = filter_.to_sql(field_overrides={})

        if wheres_params is None:
            raise ValueError(f'Invalid filter: {filter_}')

        _query = t'SELECT id, name, description, criteria, project FROM cohort_template WHERE {wheres_params:q}'

        async with self.connection.pg_connection.cursor(
            row_factory=class_row(CohortTemplateInternal)
        ) as cur:
            cohort_templates = await (await cur.execute(_query)).fetchall()

        projects = {c.project for c in cohort_templates}
        return projects, cohort_templates

    async def get_cohort_template(self, template_id: int) -> CohortTemplateInternal:
        """
        Get a cohort template by ID
        """
        _query = t'SELECT id, name, description, criteria, project FROM cohort_template WHERE id = {template_id}'
        row = await (await self.connection.pg_connection.execute(_query)).fetchone()
        if not row:
            raise NotFoundError(f'Cohort template with ID {template_id} not found')

        row['criteria'] = row.get('criteria') or {}
        return CohortTemplateInternal(**row)

    async def create_cohort_template(
        self,
        name: str,
        description: str,
        criteria: CohortCriteriaInternal,
        project: ProjectId,
    ) -> int:
        """
        Create new cohort template
        """

        audit_log_id = await self.audit_log_id()
        criteria_param = Jsonb(dict(criteria))

        _query = t"""
        INSERT INTO cohort_template (name, description, criteria, project, audit_log_id)
        VALUES ({name}, {description}, {criteria_param}, {project}, {audit_log_id}) RETURNING id;
        """

        async with self.connection.pg_connection.cursor(row_factory=scalar_row) as cur:
            cohort_template_id = await (await cur.execute(_query)).fetchone()

        return cohort_template_id

    async def create_cohort(
        self,
        project: int,
        cohort_name: str,
        sequencing_group_ids: list[int],
        description: str,
        template_id: int,
    ) -> NewCohortInternal:
        """
        Create a new cohort
        """
        audit_log_id = await self.audit_log_id()

        _query = t"""
        INSERT INTO cohort (name, template_id, author, description, project, timestamp, status, audit_log_id)
        VALUES ({cohort_name}, {template_id}, {self.author}, {description}, {project}, {datetime.datetime.now()},
        {CohortStatus.active.value}, {audit_log_id})
        RETURNING id
        """

        _query_insert_many = """
        INSERT INTO cohort_sequencing_group (cohort_id, sequencing_group_id, audit_log_id)
        VALUES (%(cohort_id)s, %(sequencing_group_id)s, %(audit_log_id)s)
        """

        # Use an atomic transaction for a multi-part insert query to prevent the database being
        # left in an incomplete state if the query fails part way through.
        conn = self.connection.pg_connection
        async with conn.transaction(), conn.cursor(row_factory=scalar_row) as cur:
            cohort_id = await (await cur.execute(_query)).fetchone()

            await cur.executemany(
                _query_insert_many,
                [
                    {
                        'cohort_id': cohort_id,
                        'sequencing_group_id': sg,
                        'audit_log_id': audit_log_id,
                    }
                    for sg in sequencing_group_ids
                ],
            )

        return NewCohortInternal(
            dry_run=False,
            cohort_id=cohort_id,
            sequencing_group_ids=sequencing_group_ids,
        )

    async def get_template_by_cohort_by_id(self, cohort_id: int) -> int:
        """
        Get the cohort template_id given its ID
        """
        _query = t'SELECT template_id FROM cohort WHERE id = {cohort_id}'

        async with self.connection.pg_connection.cursor(row_factory=scalar_row) as cur:
            template_id = await (await cur.execute(_query)).fetchone()
        if not template_id:
            raise ValueError(f'Cohort with ID {cohort_id} not found')
        return template_id

    async def update_cohort(
        self,
        cohort_id: int,
        name: str | None,
        description: str | None,
        status: CohortUpdateStatus | None,
    ):
        """
        Update the cohort given its ID
        """
        update_columns = []

        # The following fields are allowed to update
        if name is not None:
            update_columns.append(t'name={name}')
        if description is not None:
            update_columns.append(t'description={description}')
        if status is not None:
            update_columns.append(t'status={status.value}')

        if not update_columns:
            raise ValueError(f'No field to update')

        audit_log_id = await self.audit_log_id()
        update_columns.append(t'audit_log_id={audit_log_id}')

        joined_query = sql.SQL(',').join(update_columns)
        query = t"""
            UPDATE cohort
            SET {joined_query:q}
            WHERE id = {cohort_id}
        """
        await self.connection.pg_connection.execute(query)

    async def is_cohort_sample_sg_invalid(self, cohort_id: int) -> bool:
        """Query sample and sequencing group status for cohort"""

        _query = t"""
            SELECT
            exists (
                select 1
                from cohort_sequencing_group csg
                join sequencing_group sg
                on sg.id = csg.sequencing_group_id
                join sample s
                on s.id = sg.sample_id
                where csg.cohort_id = c.id
                and (sg.archived or not s.active)
            ) as is_invalid
            FROM cohort c
            WHERE c.id = {cohort_id}
            """

        row = await (await self.connection.pg_connection.execute(_query)).fetchone()
        if row:
            return parse_sql_bool(row['is_invalid'])
        return False


def _custom_matches_filter(
    status: CohortStatus, filter_: GenericFilter[CohortStatus]
) -> bool:
    """
    Util method to filter based on cohort status
    """

    if filter_ is None:
        return True

    if filter_.eq is not None and status != filter_.eq:
        return False
    if filter_.neq is not None and status == filter_.neq:
        return False
    if filter_.in_ is not None and status not in filter_.in_:
        return False
    if filter_.nin is not None and status in filter_.nin:  # noqa: SIM103
        return False

    return True
