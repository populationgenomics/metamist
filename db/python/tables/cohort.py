# pylint: disable=too-many-instance-attributes
import dataclasses
import datetime
from collections import defaultdict
from typing import DefaultDict

from db.python.filters import GenericFilter, GenericFilterModel
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, to_db_json
from models.enums.cohort import CohortStatus
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

    table_name = 'cohort'
    cohort_get_keys = [
        'c.id as c_id',
        'c.name as c_name',
        'c.template_id as c_template_id',
        'c.description as c_description',
        'c.author as c_author',
        'c.project as c_project',
        'c.status as c_status',
        's.active as s_active',
        'sg.archived as sg_archived',
    ]

    template_keys = ['id', 'name', 'description', 'criteria', 'project']

    async def query(
        self, filter_: CohortFilter
    ) -> tuple[list[CohortInternal], set[ProjectId]]:
        """Query Cohorts"""
        wheres, values = filter_.to_sql(field_overrides={'id':'c.id', 'name':'c.name',
                                                         'template_id':'c.template_id',
                                                         'author':'c.author',
                                                         'project':'c.project',
                                                         'status':'c.status'})
        if not wheres:
            raise ValueError(f'Invalid filter: {filter_}')

        cohort_get_keys_str = ','.join(self.cohort_get_keys)

        # TODO: check how the status filter work as it is not injected to the query parse_sql_bool
        _query = f"""
        SELECT {cohort_get_keys_str}
        FROM cohort c
        LEFT JOIN cohort_sequencing_group csg ON c.id = csg.cohort_id
        LEFT JOIN sequencing_group sg ON sg.id = csg.sequencing_group_id
        LEFT JOIN sample s ON s.id = sg.sample_id
        WHERE {wheres}
        """

        rows = await self.connection.fetch_all(_query, values)

        unique_cohorts = {}
        cohort_statuses: DefaultDict[str, CohortStatus] = defaultdict(
            lambda: CohortStatus.ACTIVE
        )

        for row in rows:
            row_dict = dict(row)
            cohort_id = row_dict['c_id']
            if cohort_id not in unique_cohorts:
                unique_cohorts[cohort_id] = row_dict
            if (
                cohort_statuses[cohort_id] == CohortStatus.ACTIVE
                and row_dict['s_active'] == True
                and row_dict['sg_archived'] == False
                and row_dict['c_status'].lower() == CohortStatus.ACTIVE.value
            ):
                cohort_statuses[cohort_id] = CohortStatus.ACTIVE
            else:
                cohort_statuses[cohort_id] = CohortStatus.INACTIVE
                # TODO:piyumi early termination

        if filter_.status:
            cohorts = [
                CohortInternal.from_db(dict(row), status)
                for cohort_id, row in unique_cohorts.items()
                if (status := cohort_statuses[cohort_id]) == filter_.status
            ]
        else:
            cohorts = [
                CohortInternal.from_db(dict(row), cohort_statuses[cohort_id])
                for cohort_id, row in unique_cohorts.items()
            ]

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
            INSERT INTO cohort (name, template_id, author, description, project, timestamp, status, audit_log_id)
            VALUES (:name, :template_id, :author, :description, :project, :timestamp, :status, :audit_log_id)
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
                    'status': CohortStatus.ACTIVE.value.upper(),
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
        # TODO:piyumi. Current usage only consume template id

        cohort_get_keys_str = ','.join(self.cohort_get_keys)

        _query = f"""
        SELECT {cohort_get_keys_str}
        FROM cohort c
        LEFT JOIN cohort_sequencing_group csg ON c.id = csg.cohort_id
        LEFT JOIN sequencing_group sg ON sg.id = csg.sequencing_group_id
        LEFT JOIN sample s ON s.id = sg.sample_id
        WHERE c.id = :cohort_id
        """

        rows = await self.connection.fetch_all(_query, {'cohort_id': cohort_id})

        if not rows:
            raise ValueError(f'Cohort with ID {cohort_id} not found')

        cohort_status = CohortStatus.ACTIVE

        for row in rows:
            row_dict = dict(row)
            if (
                cohort_status == CohortStatus.ACTIVE
                and row_dict['s_active'] == True
                and row_dict['s_archived'] == False
                and row_dict['c_status'].lower() == CohortStatus.ACTIVE.value
            ):
                cohort_status = CohortStatus.ACTIVE
            else:
                cohort_status = CohortStatus.INACTIVE
                break

        return CohortInternal.from_db(dict(rows[0]), cohort_status)

    async def update_cohort_given_id(
        self, name: str, description: str, status: CohortStatus, cohort_id: int
    ):
        """
        Update the cohort given its ID
        """
        _query = """
        UPDATE cohort
        SET name = :name, description = :description, status = :status
        WHERE id = :cohort_id
        """

        #TODO:piyumi when values are not provided what happens
        await self.connection.execute(
            _query,
            {
                'name': name,
                'description': description,
                'status': status.value.upper(),
                'cohort_id': cohort_id,
            },
        )

