# pylint: disable=too-many-instance-attributes
from collections import defaultdict
from typing import Any

from db.python.db_filters import GenericFilter
from db.python.db_filters.participant import ParticipantFilter
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, escape_like_term, to_db_json
from models.models.participant import ParticipantInternal
from models.models.project import ProjectId


class ParticipantTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    keys_str = ', '.join(
        [
            'id',
            'external_id',
            'reported_sex',
            'reported_gender',
            'karyotype',
            'meta',
            'project',
            'audit_log_id',
        ]
    )

    table_name = 'participant'

    async def get_project_ids_for_participant_ids(self, participant_ids: list[int]):
        """Get project IDs for participant_ids (mostly for checking auth)"""
        _query = """
        SELECT project
        FROM participant
        WHERE id in :participant_ids
        GROUP BY project"""
        rows = await self.connection.fetch_all(
            _query, {'participant_ids': participant_ids}
        )
        return set(r['project'] for r in rows)

    @staticmethod
    async def _construct_participant_query(
        filter_: ParticipantFilter,
        keys: list[str],
        skip: int | None = None,
        limit: int | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Construct a participant query"""
        needs_family = False
        needs_sample = False
        needs_sequencing_group = False
        needs_assay = False

        wheres, values = filter_.to_sql(
            {
                'project': 'pp.project',
                'id': 'pp.id',
                'external_id': 'pp.external_id',
                'meta': 'pp.meta',
            },
            exclude=['family', 'sample', 'sequencing_group', 'assay'],
        )

        if filter_.family:
            needs_family = True
            fwheres, fvalues = filter_.family.to_sql(
                {
                    'id': 'f.id',
                    'external_id': 'f.external_id',
                    'meta': 'f.meta',
                }
            )
            values.update(fvalues)
            if fwheres:
                wheres += ' AND ' + fwheres

        if filter_.sample:
            needs_sample = True
            swheres, svalues = filter_.sample.to_sql(
                {
                    'id': 's.id',
                    'type': 's.type',
                    'external_id': 's.external_id',
                    'meta': 's.meta',
                }
            )
            values.update(svalues)
            if swheres:
                wheres += ' AND ' + swheres

        if filter_.sequencing_group:
            needs_sample = True
            needs_sequencing_group = True
            swheres, svalues = filter_.sequencing_group.to_sql(
                {
                    'id': 'sg.id',
                    'meta': 'sg.meta',
                    'type': 'sg.type',
                    'technology': 'sg.technology',
                    'platform': 'sg.platform',
                }
            )
            values.update(svalues)
            if swheres:
                wheres += ' AND ' + swheres

        if filter_.assay:
            # 2024-06-13 mfranklin
            #   This is explicitly NOT searching assays through the active sequencing
            #   group
            needs_sample = True
            needs_assay = True

            awheres, avalues = filter_.assay.to_sql(
                {
                    'id': 'a.id',
                    'meta': 'a.meta',
                    'type': 'a.type',
                }
            )
            values.update(avalues)
            if awheres:
                wheres += ' AND ' + awheres

        query_lines = [
            'SELECT DISTINCT pp.id',
            'FROM participant pp',
        ]

        if needs_sample:
            query_lines.append('INNER JOIN sample s ON s.participant_id = pp.id')
        if needs_sequencing_group:
            query_lines.append('INNER JOIN sequencing_group sg ON sg.sample_id = s.id')
        if needs_assay:
            # see above for a quick note in assays from participant query
            query_lines.append('INNER JOIN assay a ON a.sample_id = s.id')
        if needs_family:
            query_lines.append(
                'INNER JOIN family_participant fp ON fp.participant_id = pp.id\n'
                'INNER JOIN family f ON f.id = fp.family_id'
            )

        if wheres:
            query_lines.append('WHERE \n' + wheres)

        if limit or skip:
            query_lines.append('ORDER BY pp.id')

        if limit:
            query_lines.append('LIMIT :limit')
            values['limit'] = limit

        if skip:
            query_lines.append('OFFSET :offset')
            values['offset'] = skip

        query = '\n'.join('  ' + line.strip() for line in query_lines)
        outer_query = f"""
            SELECT {', '.join(keys)}
            FROM participant p
            INNER JOIN (
            {query}
            ) as inner_query ON inner_query.id = p.id
        """

        return outer_query, values

    async def query(
        self,
        filter_: ParticipantFilter,
        limit: int | None = None,
        skip: int | None = None,
    ) -> tuple[set[ProjectId], list[ParticipantInternal]]:
        """Query for participants

        Args:
            filter_ (ParticipantFilter): _description_

        Returns:
            list[ParticipantInternal]: _description_
        """
        keys = [
            'p.id',
            'p.external_id',
            'p.reported_sex',
            'p.reported_gender',
            'p.karyotype',
            'p.meta',
            'p.project',
        ]
        query, values = await self._construct_participant_query(
            filter_, keys=keys, skip=skip, limit=limit
        )

        rows = await self.connection.fetch_all(query, values)
        projects = set(r['project'] for r in rows)
        return projects, [ParticipantInternal.from_db(dict(r)) for r in rows]

    async def query_count(self, filter_: ParticipantFilter) -> int:
        """Query for participants count"""
        query, values = await self._construct_participant_query(
            filter_, keys=['COUNT(*) as cnt']
        )
        row = await self.connection.fetch_one(query, values)
        if not row:
            return 0
        return row['cnt']

    async def get_participants_by_ids(
        self, ids: list[int]
    ) -> tuple[set[ProjectId], list[ParticipantInternal]]:
        """Get participants by IDs"""
        return await self.query(ParticipantFilter(id=GenericFilter(in_=ids)))

    async def get_participants(
        self, project: int, internal_participant_ids: list[int] | None = None
    ) -> list[ParticipantInternal]:
        """
        Get participants for a project
        """
        _, particicpants = await self.query(
            ParticipantFilter(
                project=GenericFilter(in_=[project]),
                id=(
                    GenericFilter(in_=internal_participant_ids)
                    if internal_participant_ids
                    else None
                ),
            )
        )
        return particicpants

    async def create_participant(
        self,
        external_id: str,
        reported_sex: int | None,
        reported_gender: str | None,
        karyotype: str | None,
        meta: dict | None,
        project: ProjectId = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        if not (project or self.project):
            raise ValueError('Must provide project to create participant')

        _query = """
INSERT INTO participant
    (external_id, reported_sex, reported_gender, karyotype, meta, audit_log_id, project)
VALUES
    (:external_id, :reported_sex, :reported_gender, :karyotype, :meta, :audit_log_id, :project)
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
                'audit_log_id': await self.audit_log_id(),
                'project': project or self.project,
            },
        )

    async def update_participants(
        self,
        participant_ids: list[int],
        reported_sexes: list[int] | None,
        reported_genders: list[str] | None,
        karyotypes: list[str] | None,
        metas: list[dict] | None,
    ):
        """
        Update many participants, expects that all lists contain the same number of values.
        You can't update selective fields on selective samples, if you provide metas, this
        function will update EVERY participant with the provided meta values.
        """
        updaters = ['audit_log_id = :audit_log_id']
        audit_log_id = await self.audit_log_id()
        values: dict[str, list[Any]] = {
            'pid': participant_ids,
            'audit_log_id': [audit_log_id] * len(participant_ids),
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
        external_id: str | None,
        reported_sex: int | None,
        reported_gender: str | None,
        karyotype: str | None,
        meta: dict | None,
    ):
        """
        Update participant
        """
        updaters = ['audit_log_id = :audit_log_id']
        fields = {'pid': participant_id, 'audit_log_id': await self.audit_log_id()}

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
        external_participant_ids: list[str],
        project: ProjectId | None,
    ) -> dict[str, int]:
        """Get map of {external_id: internal_participant_id}"""
        _project = project or self.project
        if not _project:
            raise ValueError(
                'Must provide project to get participant id map by external'
            )

        if len(external_participant_ids) == 0:
            return {}

        _query = """
        SELECT external_id, id
        FROM participant
        WHERE external_id in :external_ids AND project = :project"""
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
        self, internal_participant_ids: list[int], allow_missing=False
    ) -> dict[int, str]:
        """Get map of {internal_id: external_participant_id}"""
        if len(internal_participant_ids) == 0:
            return {}

        _query = 'SELECT id, external_id FROM participant WHERE id in :ids'
        results = await self.connection.fetch_all(
            _query, {'ids': internal_participant_ids}
        )
        id_map: dict[int, str] = {r['id']: r['external_id'] for r in results}

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

    async def get_participants_by_families(
        self, family_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[ParticipantInternal]]]:
        """Get list of participants keyed by families, duplicates results"""
        _query = """
            SELECT project, fp.family_id, p.id, p.external_id, p.reported_sex, p.reported_gender, p.karyotype, p.meta
            FROM participant p
            INNER JOIN family_participant fp ON fp.participant_id = p.id
            WHERE fp.family_id IN :fids
        """
        rows = await self.connection.fetch_all(_query, {'fids': family_ids})
        retmap = defaultdict(list)
        projects: set[ProjectId] = set()
        for row in rows:
            drow = dict(row)
            projects.add(row['project'])
            fid = drow.pop('family_id')
            retmap[fid].append(ParticipantInternal.from_db(drow))

        return projects, retmap

    async def update_many_participant_external_ids(
        self, internal_to_external_id: dict[int, str]
    ):
        """Update many participant external_ids through the {internal: external} map"""
        _query = """
        UPDATE participant
        SET external_id = :external_id, audit_log_id = :audit_log_id
        WHERE id = :participant_id"""
        audit_log_id = await self.audit_log_id()
        mapped_values = [
            {'participant_id': k, 'external_id': v, 'audit_log_id': audit_log_id}
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

    async def get_external_participant_id_to_internal_sequencing_group_id_map(
        self, project: ProjectId, sequencing_type: str | None = None
    ) -> list[tuple[str, int]]:
        """
        Get a map of {external_participant_id} -> {internal_sequencing_group_id}
        useful to match joint-called sequencing groups in the matrix table to the participant

        Return a list not dictionary, because dict could lose
        participants with multiple sequencing groups.
        """
        wheres = ['p.project = :project']
        values: dict[str, Any] = {'project': project}
        if sequencing_type:
            wheres.append('sg.type = :sequencing_type')
            values['sequencing_type'] = sequencing_type

        _query = f"""
SELECT p.external_id, sg.id
FROM participant p
INNER JOIN sample s ON p.id = s.participant_id
INNER JOIN sequencing_group sg ON sg.sample_id = s.id
WHERE {' AND '.join(wheres)}
"""
        rows = await self.connection.fetch_all(_query, values)
        return [(r[0], int(r[1])) for r in rows]

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
                'search_pattern': escape_like_term(query) + '%',
                'limit': limit,
            },
        )
        return [(r['project'], r['id'], r['external_id']) for r in rows]
