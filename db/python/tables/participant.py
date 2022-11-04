from typing import List, Dict, Tuple, Any, Optional

from db.python.connect import DbBase, NotFoundError
from db.python.utils import ProjectId, to_db_json
from models.models.participant import Participant


class ParticipantTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'participant'

    async def get_project_ids_for_participant_ids(self, participant_ids: List[int]):
        """Get project IDs for participant_ids (mostly for checking auth)"""
        _query = 'SELECT project FROM participant WHERE id in :participant_ids GROUP BY project'
        rows = await self.connection.fetch_all(
            _query, {'participant_ids': participant_ids}
        )
        return set(r['project'] for r in rows)

    async def get_participants_by_ids(self, ids: list[int]) -> tuple[set[ProjectId], list[Participant]]:
        _query = 'SELECT project, id, external_id, reported_sex, reported_gender, karyotype, meta FROM participant WHERE id in :ids'
        rows = await self.connection.fetch_all(
            _query, {'ids': ids}
        )

        ds = [dict(r) for r in rows]
        projects = set(d.pop('project') for d in ds)

        return projects, [Participant(**d) for d in ds]


    async def get_participants(
        self, project: int, internal_participant_ids: List[int] = None
    ):
        """
        Get participants for a project
        """
        values: Dict[str, Any] = {'project': project}
        if internal_participant_ids:
            _query = 'SELECT * FROM participant WHERE project = :project AND id in :ids'
            values['ids'] = internal_participant_ids
        else:
            _query = 'SELECT * FROM participant WHERE project = :project'
        return await self.connection.fetch_all(_query, values)

    async def create_participant(
        self,
        external_id: str,
        reported_sex: int = None,
        reported_gender: str = None,
        karyotype: str = None,
        meta: Dict = None,
        author: str = None,
        project: ProjectId = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        _query = f"""
INSERT INTO participant (external_id, reported_sex, reported_gender, karyotype, meta, author, project)
VALUES (:external_id, :reported_sex, :reported_gender, :karyotype, :meta, :author, :project)
RETURNING id
        """

        return await self.connection.fetch_val(
            _query,
            {
                'external_id': external_id,
                'reported_sex': reported_sex,
                'reported_gender': reported_gender,
                'karyotype': karyotype,
                'meta': to_db_json(meta or {}),
                'author': author or self.author,
                'project': project or self.project,
            },
        )

    async def update_participants(
        self,
        participant_ids: List[int],
        reported_sexes: List[int] = None,
        reported_genders: List[str] = None,
        karyotypes: List[str] = None,
        metas: List[Dict] = None,
        author=None,
    ):
        """
        Update many participants, expects that all lists contain the same number of values.
        You can't update selective fields on selective samples, if you provide metas, this
        function will update EVERY participant with the provided meta values.
        """
        _author = author or self.author
        updaters = ['author = :author']
        values: Dict[str, List[Any]] = {
            'pid': participant_ids,
            'author': [_author] * len(participant_ids),
        }
        if reported_sexes:
            updaters.append('reported_sex = :reported_sex')
            values['reported_sex'] = reported_sexes
        if reported_genders:
            updaters.append('reported_gender = :reported_gender')
            values['reported_gender'] = reported_genders
        if karyotypes:
            updaters.append('karyotype = :karyotype')
            values['karyotype'] = karyotypes
        if metas:
            updaters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta')
            values['meta'] = metas

        keys = list(values.keys())
        list_values = [
            {k: l[idx] for k, l in values.items()}
            for idx in range(len(values[keys[0]]))
        ]

        updaters_str = ', '.join(updaters)
        _query = f'UPDATE participant SET {updaters_str} WHERE id = :pid'
        await self.connection.execute_many(_query, list_values)

    async def update_participant(
        self,
        participant_id: int,
        external_id: str = None,
        reported_sex: int = None,
        reported_gender: str = None,
        karyotype: str = None,
        meta: Dict = None,
        author=None,
    ):
        """
        Update participant
        """
        updaters = ['author = :author']
        fields = {'pid': participant_id, 'author': author or self.author}

        if external_id:
            updaters.append('external_id = :external_id')
            fields['external_id'] = external_id

        if reported_sex:
            updaters.append('reported_sex = :reported_sex')
            fields['reported_sex'] = reported_sex

        if reported_gender:
            updaters.append('reported_gender = :reported_gender')
            fields['reported_gender'] = reported_gender

        if karyotype:
            updaters.append('karyotype = :karyotype')
            fields['karyotype'] = karyotype

        if meta:
            updaters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')
            fields['meta'] = to_db_json(meta)

        _query = f'UPDATE participant SET {", ".join(updaters)} WHERE id = :pid'
        await self.connection.execute(_query, fields)

        return True

    async def get_id_map_by_external_ids(
        self,
        external_participant_ids: List[str],
        project: Optional[ProjectId],
    ) -> Dict[str, int]:
        """Get map of {external_id: internal_participant_id}"""
        assert project

        if len(external_participant_ids) == 0:
            return {}

        _query = 'SELECT external_id, id FROM participant WHERE external_id in :external_ids AND project = :project'
        results = await self.connection.fetch_all(
            _query,
            {
                'external_ids': external_participant_ids,
                'project': project,
            },
        )
        id_map = {r['external_id']: r['id'] for r in results}

        return id_map

    async def get_id_map_by_internal_ids(
        self, internal_participant_ids: List[int], allow_missing=False
    ) -> Dict[int, str]:
        """Get map of {internal_id: external_participant_id}"""
        if len(internal_participant_ids) == 0:
            return {}

        _query = 'SELECT id, external_id FROM participant WHERE id in :ids'
        results = await self.connection.fetch_all(
            _query, {'ids': internal_participant_ids}
        )
        id_map: Dict[int, str] = {r['id']: r['external_id'] for r in results}

        if not allow_missing and len(id_map) != len(internal_participant_ids):
            provided_internal_ids = set(internal_participant_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_internal_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_participant_ids = provided_internal_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find participants with internal IDS: {', '.join(str(s) for s in missing_participant_ids)}"
                )

        return id_map

    async def update_many_participant_external_ids(
        self, internal_to_external_id: Dict[int, str]
    ):
        """Update many participant external_ids through the {internal: external} map"""
        _query = 'UPDATE participant SET external_id = :external_id WHERE id = :participant_id'
        mapped_values = [
            {'participant_id': k, 'external_id': v}
            for k, v in internal_to_external_id.items()
        ]
        await self.connection.execute_many(_query, mapped_values)
        return True

    async def get_external_ids_by_participant(
        self, participant_ids: list[int]
    ) -> dict[int, list[str]]:
        """
        Get list of external IDs for a participant,
        This method signature is partially future-proofed for multiple-external IDs
        """
        if not participant_ids:
            return {}

        _query = 'SELECT id, external_id FROM participant WHERE id in :pids'
        rows = await self.connection.fetch_all(_query, {'pids': participant_ids})
        return {r['id']: [r['external_id']] for r in rows}

    async def get_external_participant_id_to_internal_sample_id_map(
        self, project: ProjectId
    ) -> List[Tuple[str, int]]:
        """
        Get a map of {external_participant_id} -> {internal_sample_id}
        useful to matching joint-called samples in the matrix table to the participant

        Return a list not dictionary, because dict could lose
        participants with multiple samples.
        """
        _query = """
SELECT p.external_id, s.id
FROM participant p
INNER JOIN sample s ON p.id = s.participant_id
WHERE p.project = :project
"""
        values = await self.connection.fetch_all(_query, {'project': project})
        return [(r[0], r[1]) for r in values]

    async def search(
        self, query, project_ids: list[ProjectId], limit: int = 5
    ) -> list[tuple[ProjectId, int, str]]:
        """
        Search by some term, return [ProjectId, ParticipantId, ExternalId]
        """
        _query = """
        SELECT project, id, external_id
        FROM participant
        WHERE project in :project_ids AND external_id LIKE :search_pattern
        LIMIT :limit
        """
        rows = await self.connection.fetch_all(
            _query,
            {
                'project_ids': project_ids,
                'search_pattern': self.escape_like_term(query) + '%',
                'limit': limit,
            },
        )
        return [(r['project'], r['id'], r['external_id']) for r in rows]
