# pylint: disable=too-many-instance-attributes
import dataclasses
import datetime

from db.python.tables.base import DbBase
from db.python.tables.project import ProjectId
from db.python.utils import GenericFilter, GenericFilterModel, NotFoundError, to_db_json
from models.models.cohort import (
    CohortCriteriaInternal,
    CohortInternal,
    CohortTemplateInternal,
    NewCohortInternal,
)


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

    table_name = 'cohort'
    common_get_keys = [
        'id',
        'name',
        'template_id',
        'description',
        'author',
        'project',
    ]

    template_keys = ['id', 'name', 'description', 'criteria', 'project']

    async def query(
        self, filter_: CohortFilter
    ) -> tuple[list[CohortInternal], set[ProjectId]]:
        """Query Cohorts"""
        wheres, values = filter_.to_sql(field_overrides={})
        if not wheres:
            raise ValueError(f'Invalid filter: {filter_}')

        common_get_keys_str = ','.join(self.common_get_keys)
        _query = f"""
        SELECT {common_get_keys_str}
        FROM cohort
        WHERE {wheres}
        """

        rows = await self.connection.fetch_all(_query, values)
        cohorts = [CohortInternal.from_db(dict(row)) for row in rows]
        projects = {c.project for c in cohorts}
        return cohorts, projects

    async def get_cohort_sequencing_group_ids(self, cohort_id: int) -> list[int]:
        """
        Return all sequencing group IDs for the given cohort.
        """

        _query = """
        SELECT sequencing_group_id FROM cohort_sequencing_group WHERE cohort_id = :cohort_id
        """
        rows = await self.connection.fetch_all(_query, {'cohort_id': cohort_id})
        return [row['sequencing_group_id'] for row in rows]

    async def query_cohort_templates(
        self, filter_: CohortTemplateFilter
    ) -> tuple[set[ProjectId], list[CohortTemplateInternal]]:
        """Query CohortTemplates"""
        wheres, values = filter_.to_sql(field_overrides={})
        if not wheres:
            raise ValueError(f'Invalid filter: {filter_}')

        common_get_keys_str = ','.join(self.template_keys)
        _query = f"""
        SELECT {common_get_keys_str}
        FROM cohort_template
        WHERE {wheres}
        """

        rows = await self.connection.fetch_all(_query, values)
        cohort_templates = [CohortTemplateInternal.from_db(dict(row)) for row in rows]
        projects = {c.project for c in cohort_templates}
        return projects, cohort_templates

    async def get_cohort_template(self, template_id: int) -> CohortTemplateInternal:
        """
        Get a cohort template by ID
        """
        _query = """
        SELECT id as id, name, description, criteria, project FROM cohort_template WHERE id = :template_id
        """
        template = await self.connection.fetch_one(_query, {'template_id': template_id})

        if not template:
            raise NotFoundError(f'Cohort template with ID {template_id} not found')

        cohort_template = CohortTemplateInternal.from_db(dict(template))

        return cohort_template

    async def create_cohort_template(
        self,
        name: str,
        description: str,
        criteria: CohortCriteriaInternal,
        project: ProjectId,
    ):
        """
        Create new cohort template
        """
        _query = """
        INSERT INTO cohort_template (name, description, criteria, project, audit_log_id)
        VALUES (:name, :description, :criteria, :project, :audit_log_id) RETURNING id;
        """
        cohort_template_id = await self.connection.fetch_val(
            _query,
            {
                'name': name,
                'description': description,
                'criteria': to_db_json(dict(criteria)),
                'project': project,
                'audit_log_id': await self.audit_log_id(),
            },
        )

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

        # Use an atomic transaction for a multi-part insert query to prevent the database being
        # left in an incomplete state if the query fails part way through.
        async with self.connection.transaction():
            audit_log_id = await self.audit_log_id()

            _query = """
            INSERT INTO cohort (name, template_id, author, description, project, timestamp, audit_log_id)
            VALUES (:name, :template_id, :author, :description, :project, :timestamp, :audit_log_id)
            RETURNING id
            """

            cohort_id = await self.connection.fetch_val(
                _query,
                {
                    'template_id': template_id,
                    'author': self.author,
                    'description': description,
                    'project': project,
                    'name': cohort_name,
                    'timestamp': datetime.datetime.now(),
                    'audit_log_id': audit_log_id,
                },
            )

            _query = """
            INSERT INTO cohort_sequencing_group (cohort_id, sequencing_group_id, audit_log_id)
            VALUES (:cohort_id, :sequencing_group_id, :audit_log_id)
            """

            await self.connection.execute_many(
                _query,
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

    async def get_cohort_by_id(self, cohort_id: int) -> CohortInternal:
        """
        Get the cohort by its ID
        """
        _query = """
        SELECT id, name, template_id, author, description, project, timestamp
        FROM cohort
        WHERE id = :cohort_id
        """

        cohort = await self.connection.fetch_one(_query, {'cohort_id': cohort_id})
        if not cohort:
            raise ValueError(f'Cohort with ID {cohort_id} not found')

        return CohortInternal.from_db(dict(cohort))
