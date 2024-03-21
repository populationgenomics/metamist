# pylint: disable=too-many-instance-attributes
import dataclasses
import datetime

from db.python.tables.base import DbBase
from db.python.tables.project import ProjectId
from db.python.utils import GenericFilter, GenericFilterModel, to_db_json
from models.models.cohort import Cohort, CohortTemplateModel


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

    template_keys = [
        'id',
        'name',
        'description',
        'criteria',
        'project'
    ]

    async def query(self, filter_: CohortFilter):
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
        cohorts = [Cohort.from_db(dict(row)) for row in rows]
        return cohorts

    async def get_cohort_sequencing_group_ids(self, cohort_id: int) -> list[int]:
        """
        Return all sequencing group IDs for the given cohort.
        """

        _query = """
        SELECT sequencing_group_id FROM cohort_sequencing_group WHERE cohort_id = :cohort_id
        """
        rows = await self.connection.fetch_all(_query, {'cohort_id': cohort_id})
        return [row['sequencing_group_id'] for row in rows]

    async def query_cohort_templates(self, filter_: CohortTemplateFilter):  # TODO: Move this to its own class?
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
        cohort_templates = [CohortTemplateModel.from_db(dict(row)) for row in rows]
        return cohort_templates

    async def get_cohort_template(self, template_id: int):
        """
        Get a cohort template by ID
        """
        _query = """
        SELECT id as id, criteria as criteria FROM cohort_template WHERE id = :template_id
        """
        template = await self.connection.fetch_one(_query, {'template_id': template_id})

        return {'id': template['id'], 'criteria': template['criteria']}

    async def create_cohort_template(
            self,
            name: str,
            description: str,
            criteria: dict,
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
                    'criteria': to_db_json(criteria),
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
        author: str,
        description: str,
        template_id: int,
    ) -> int:
        """
        Create a new cohort
        """

        # Use an atomic transaction for a mult-part insert query to prevent the database being
        # left in an incomplete state if the query fails part way through.
        async with self.connection.transaction():
            audit_log_id = await self.audit_log_id()

            _query = """
            INSERT INTO cohort (name, template_id, author, description, project, timestamp, audit_log_id)
            VALUES (:name, :template_id, :author, :description, :project, :timestamp, :audit_log_id) RETURNING id
            """

            cohort_id = await self.connection.fetch_val(
                _query,
                {
                    'template_id': template_id,
                    'author': author,
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

            for sg in sequencing_group_ids:
                await self.connection.execute(
                    _query,
                    {
                        'cohort_id': cohort_id,
                        'sequencing_group_id': sg,
                        'audit_log_id': audit_log_id,
                    },
                )

            return cohort_id
