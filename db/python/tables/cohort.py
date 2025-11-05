# pylint: disable=too-many-instance-attributes
import dataclasses
import datetime
from collections import defaultdict
from typing import DefaultDict, Any

from db.python.filters import GenericFilter, GenericFilterModel
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, to_db_json
from models.base import parse_sql_bool
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

        filter_status = filter_.status
        filter_.status = None  # reset filter and use to filter on the rows fetched

        wheres, values = filter_.to_sql(
            field_overrides={
                'id': 'c.id',
                'name': 'c.name',
                'template_id': 'c.template_id',
                'author': 'c.author',
                'project': 'c.project',
            }
        )
        if not wheres:
            raise ValueError(f'Invalid filter: {filter_}')

        cohort_get_keys_str = ','.join(self.cohort_get_keys)

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
                and parse_sql_bool(row_dict['s_active'])
                and (not parse_sql_bool(row_dict['sg_archived']))
                and row_dict['c_status'].lower() == CohortStatus.ACTIVE.value
            ):
                cohort_statuses[cohort_id] = CohortStatus.ACTIVE
            else:
                cohort_statuses[cohort_id] = CohortStatus.INACTIVE
                # TODO:piyumi early termination

        if filter_status is not None:
            cohorts = []
            for cohort_id, row in unique_cohorts.items():
                status = cohort_statuses[cohort_id]
                include = True

                if filter_status.eq is not None and status != filter_status.eq:
                    include = False
                if filter_status.neq is not None and status == filter_status.neq:
                    include = False
                if filter_status.in_ is not None and status not in filter_status.in_:
                    include = False
                if filter_status.nin is not None and status in filter_status.nin:
                    include = False

                # The remaining filter criteria in GenericFilter are not considered and will be by-passed
                if include:
                    cohorts.append(CohortInternal.from_db(dict(row), status))
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
            raise ValueError(f'Cohort not found')

        cohort_status = CohortStatus.ACTIVE

        for row in rows:
            row_dict = dict(row)
            if (
                cohort_status == CohortStatus.ACTIVE
                and parse_sql_bool(row_dict['s_active'])
                and (not parse_sql_bool(row_dict['sg_archived']))
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

        # The following fields are allowed to update
        cohort_fields = {
            'name': name,
            'description': description,
            'status': status.value.upper() if status else None,
        }

        query_params: dict[str, Any] = {**{k: v for k, v in cohort_fields.items() if v is not None}}

        if not query_params:
            raise ValueError(f'No field to update')

        query = f"""
            UPDATE cohort
            SET {', '.join(f"{k} = :{k}" for k, v in query_params.items() if v is not None)}
            WHERE id = :cohort_id
        """
        query_params['cohort_id'] = cohort_id
        await self.connection.execute(query, values=query_params)
