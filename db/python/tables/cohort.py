# pylint: disable=too-many-instance-attributes
import dataclasses

from db.python.connect import DbBase
from db.python.tables.project import ProjectId
from db.python.utils import GenericFilter, GenericFilterModel
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format, sequencing_group_id_transform_to_raw)


@dataclasses.dataclass(kw_only=True)
class CohortFilter(GenericFilterModel):
    """
    Filters for Cohort
    """
    id: GenericFilter[int] | None = None
    project: GenericFilter[ProjectId] | None = None

class CohortTable(DbBase):
    """
    Capture Cohort table operations and queries
    """

    table_name = 'cohort'
    common_get_keys = [
        'id',
        'derived_from',
        'description',
        'author',
        'project',
    ]

    async def query(self, filter_: CohortFilter):
        """ Query Cohorts"""
        wheres, values = filter_.to_sql(field_overrides={})
        if not wheres:
            raise ValueError(f'Invalid filter: {filter_}')
        common_get_keys_str = ','.join(self.common_get_keys)
        _query = f"""
        SELECT {common_get_keys_str}
        FROM cohort
        WHERE {wheres}
        """

        print(_query)
        print(values)

        rows = await self.connection.fetch_all(_query, values)
        return [dict(row) for row in rows]


    async def get_sgs_for_cohort(
            self,
            projects: list[str],
    ) -> list[str]:
        """
        Get all sequencing groups for a cohort
        """
    
        _query = 'SELECT * FROM sequencing_group INNER JOIN sample ON sample.id = sequencing_group.sample_id WHERE sample.project in :project'

        rows = await self.connection.fetch_all(_query, {'project': projects})

        return [sequencing_group_id_format(row['id']) for row in rows]

    async def create_cohort(
        self,
        project: str,
        cohort_name: str,
        sequencing_group_ids: list[str],
        author: str,
        derived_from: str = None,
        description: str = 'This field should accept null',
    ) -> int:
        """
        Create a new cohort
        """

        # Create cohort 

        #TODO: Update scheme to handle cohort name
        print(cohort_name)

        _query = 'INSERT INTO cohort (derived_from, author, description, project) VALUES (:derived_from, :author, :description, :project) RETURNING id'
        cohort_id = await self.connection.fetch_val(_query, {'derived_from': derived_from , 'author': author, 'description': description, 'project': project})
        print(cohort_id)
        #populate sequencing groups 
        _query = 'INSERT INTO cohort_sequencing_group (cohort_id, sequencing_group_id) VALUES (:cohort_id, :sequencing_group_id)'
        for sg in sequencing_group_ids:
            await self.connection.execute(_query, {'cohort_id': cohort_id, 'sequencing_group_id': sequencing_group_id_transform_to_raw(sg)})

        return cohort_id
    