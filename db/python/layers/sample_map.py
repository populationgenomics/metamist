from itertools import groupby
from typing import List, Dict
from db.python.connect import SMConnections
from db.python.tables.sample_map import SampleMapTable
from db.python.tables.sample import SampleTable


class SampleMapLayer:
    def __init__(self, author: str):
        self.author = author

    async def get_internal_id_map(self, internal_ids: List[int]) -> Dict[int, str]:

        st = SampleMapTable(author=self.author)

        id_to_external_id = {}

        internal_id_to_project = await st.get_project_map(internal_ids)
        projects = groupby(
            internal_id_to_project.keys(), lambda k: internal_id_to_project[k]
        )
        for project, project_internal_ids_iter in projects:
            project_internal_ids = list(project_internal_ids_iter)
            sample_table = SampleTable(
                SMConnections.get_connection_for_project(project, self.author)
            )
            m = await sample_table.get_sample_id_map_by_internal_ids(
                project_internal_ids
            )
            id_to_external_id.update(m)

        return id_to_external_id
