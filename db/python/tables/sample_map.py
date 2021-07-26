from typing import List

from db.python.connect import SMConnections, NotFoundError
from models.models.sample import sample_id_format


class SampleMapTable:
    """
    Capture Sample table operations and queries
    """

    def __init__(self, author):

        self.connection = SMConnections.get_admin_db()
        self.author = author

        if self.author is None:
            raise Exception('Must provide author to {self.__class__.__name__}')

    async def generate_sample_id(self, project: str) -> int:
        """
        Generate global sample ID, RAW (not transformed)
        Please make sure you call 'sample_id_format(new_id)'
        before returning to client.

        """
        _query = 'INSERT INTO sample_map (project) VALUES (:project) RETURNING id'
        async with self.connection.transaction():
            identifier = await self.connection.fetch_val(_query, {'project': project})

            return identifier

    async def get_project_map(self, raw_sample_ids: List[int]):
        """Get {internal_id: project} map from raw_sample_ids"""
        _query = 'SELECT id, project FROM sample_map WHERE id in :ids'
        inner_id_and_project = await self.connection.fetch_all(
            _query, {'ids': raw_sample_ids}
        )
        sample_id_project_map = dict(inner_id_and_project)
        if len(sample_id_project_map) != len(raw_sample_ids):
            provided_internal_ids = set(raw_sample_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(sample_id_project_map) != len(provided_internal_ids):
                # we have samples missing from the map, so we'll 404 the whole thing
                missing_sample_ids = provided_internal_ids - set(
                    sample_id_project_map.keys()
                )

                raise NotFoundError(
                    f"Couldn't find samples with IDS: {', '.join(sample_id_format(list(missing_sample_ids)))}"
                )

        return sample_id_project_map


if __name__ == '__main__':
    import asyncio

    async def generate_new_sample_id():
        """Test function"""
        author = 'michael.franklin@populationgenomics.org.au'
        await SMConnections.connect()
        st = SampleMapTable(author=author)

        sid = await st.generate_sample_id('dev')
        print(sid)

    asyncio.run(generate_new_sample_id())
