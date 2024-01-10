# pylint: disable=too-many-instance-attributes
import dataclasses
import datetime

from db.python.connect import DbBase
from db.python.tables.project import ProjectId
from db.python.utils import GenericFilter, GenericFilterModel
from models.models.cohort import Cohort
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw


@dataclasses.dataclass(kw_only=True)
class CohortFilter(GenericFilterModel):
    """
    Filters for Cohort
    """

    id: GenericFilter[int] | None = None
    name: GenericFilter[str] | None = None
    author: GenericFilter[str] | None = None
    derived_from: GenericFilter[int] | None = None
    timestamp: GenericFilter[datetime.datetime] | None = None
    project: GenericFilter[ProjectId] | None = None


class CohortTable(DbBase):
    """
    Capture Cohort table operations and queries
    """

    table_name = 'cohort'
    common_get_keys = [
        'id',
        'name',
        'derived_from',
        'description',
        'author',
        'project',
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

    async def create_cohort(
        self,
        project: int,
        cohort_name: str,
        sequencing_group_ids: list[str],
        author: str,
        description: str,
        derived_from: int | None = None,
    ) -> int:
        """
        Create a new cohort
        """

        # Use an atomic transaction for a mult-part insert query to prevent the database being
        # left in an incomplete state if the query fails part way through.
        async with self.connection.transaction():
            _query = """
            INSERT INTO cohort (name, derived_from, author, description, project)
            VALUES (:name, :derived_from, :author, :description, :project) RETURNING id
            """

            cohort_id = await self.connection.fetch_val(
                _query,
                {
                    'derived_from': derived_from,
                    'author': author,
                    'description': description,
                    'project': project,
                    'name': cohort_name,
                },
            )

            _query = """
            INSERT INTO cohort_sequencing_group (cohort_id, sequencing_group_id)
            VALUES (:cohort_id, :sequencing_group_id)
            """

            for sg in sequencing_group_ids:
                await self.connection.execute(
                    _query,
                    {
                        'cohort_id': cohort_id,
                        'sequencing_group_id': sequencing_group_id_transform_to_raw(sg),
                    },
                )

            return cohort_id
