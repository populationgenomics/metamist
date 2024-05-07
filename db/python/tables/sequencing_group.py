# pylint: disable=too-many-instance-attributes
import dataclasses
from collections import defaultdict
from datetime import date
from typing import Any

from db.python.tables.base import DbBase
from db.python.utils import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
    NoOpAenter,
    NotFoundError,
    to_db_json,
)
from models.models.project import ProjectId
from models.models.sequencing_group import (
    SequencingGroupInternal,
    SequencingGroupInternalId,
)


@dataclasses.dataclass(kw_only=True)
class SequencingGroupFilter(GenericFilterModel):
    """Sequencing Group Filter"""

    project: GenericFilter[ProjectId] | None = None
    sample_id: GenericFilter[int] | None = None
    external_id: GenericFilter[str] | None = None
    id: GenericFilter[int] | None = None
    type: GenericFilter[str] | None = None
    technology: GenericFilter[str] | None = None
    platform: GenericFilter[str] | None = None
    active_only: GenericFilter[bool] | None = GenericFilter(eq=True)
    meta: GenericMetaFilter | None = None

    # These fields are manually handled in the query to speed things up, because multiple table
    # joins and dynamic computation are required.
    created_on: GenericFilter[date] | None = None
    assay_meta: GenericMetaFilter | None = None
    has_cram: bool | None = None
    has_gvcf: bool | None = None

    def __hash__(self):  # pylint: disable=useless-super-delegation
        return super().__hash__()


class SequencingGroupTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sequencing_group'
    common_get_keys = [
        'sg.id',
        's.project',
        'sg.sample_id',
        'sg.type',
        'sg.technology',
        'sg.platform',
        'sg.meta',
        'sg.archived',
    ]
    common_get_keys_str = ', '.join(common_get_keys)

    async def query(
        self, filter_: SequencingGroupFilter
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """Query samples"""
        if filter_.is_false():
            return set(), []

        sql_overrides = {
            'project': 's.project',
            'sample_id': 'sg.sample_id',
            'id': 'sg.id',
            'meta': 'sg.meta',
            'type': 'sg.type',
            'technology': 'sg.technology',
            'platform': 'sg.platform',
            'active_only': 'NOT sg.archived',
            'external_id': 'sgexid.external_id',
            'created_on': 'DATE(row_start)',
            'assay_meta': 'meta',
        }

        # Progressively build up the query and query values based on the filters provided to
        # avoid uneccessary joins and improve performance.
        _query: list[str] = []
        query_values: dict[str, Any] = {}
        # These fields are manually handled in the query
        exclude_fields: list[str] = []

        # Base query
        _query.append(
            f"""
            SELECT
                {self.common_get_keys_str}
            FROM sequencing_group AS sg
            LEFT JOIN sample s ON s.id = sg.sample_id
            LEFT JOIN sequencing_group_external_id sgexid ON sg.id = sgexid.sequencing_group_id"""
        )

        if filter_.assay_meta is not None:
            exclude_fields.append('assay_meta')
            wheres, values = filter_.to_sql(sql_overrides, only=['assay_meta'])
            query_values.update(values)
            _query.append(
                f"""
            INNER JOIN (
                SELECT DISTINCT
                    sequencing_group_id
                FROM
                    sequencing_group_assay
                    INNER JOIN (
                        SELECT
                            id
                        FROM
                            assay
                        WHERE
                            {wheres}
                    ) AS assay_subquery ON sequencing_group_assay.assay_id = assay_subquery.id
                ) AS sga_subquery ON sg.id = sga_subquery.sequencing_group_id
            """
            )

        if filter_.created_on is not None:
            exclude_fields.append('created_on')
            wheres, values = filter_.to_sql(sql_overrides, only=['created_on'])
            query_values.update(values)
            _query.append(
                f"""
            INNER JOIN (
                SELECT
                    id,
                    TIMESTAMP(min(row_start)) AS created_on
                FROM
                    sequencing_group FOR SYSTEM_TIME ALL
                WHERE
                    {wheres}
                GROUP BY
                    id
            ) AS sg_timequery ON sg.id = sg_timequery.id
            """
            )

        if filter_.has_cram is not None or filter_.has_gvcf is not None:
            exclude_fields.extend(['has_cram', 'has_gvcf'])
            wheres, values = filter_.to_sql(
                sql_overrides, only=['has_cram', 'has_gvcf']
            )
            query_values.update(values)
            _query.append(
                f"""
            INNER JOIN (
                SELECT
                    sequencing_group_id,
                    FIND_IN_SET('cram', GROUP_CONCAT(LOWER(anlysis_query.type))) > 0 AS has_cram,
                    FIND_IN_SET('gvcf', GROUP_CONCAT(LOWER(anlysis_query.type))) > 0 AS has_gvcf
                FROM
                    analysis_sequencing_group
                    INNER JOIN (
                        SELECT
                            id, type
                        FROM
                            analysis
                    ) AS anlysis_query ON analysis_sequencing_group.analysis_id = anlysis_query.id
                GROUP BY
                    sequencing_group_id
                HAVING
                    {wheres}
            ) AS sg_filequery ON sg.id = sg_filequery.sequencing_group_id
            """
            )

        # Add the rest of the filters
        wheres, values = filter_.to_sql(sql_overrides, exclude=exclude_fields)
        _query.append(
            f"""
            WHERE {wheres}"""
        )
        query_values.update(values)

        rows = await self.connection.fetch_all('\n'.join(_query), query_values)
        sgs = [SequencingGroupInternal.from_db(**dict(r)) for r in rows]
        projects = set(sg.project for sg in sgs)
        return projects, sgs

    async def get_projects_by_sequencing_group_ids(
        self, sequencing_group_ids: list[int]
    ) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT s.project FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            WHERE sg.id in :sequencing_group_ids
            GROUP BY s.project
        """
        if len(sequencing_group_ids) == 0:
            raise ValueError('Received no sequence group IDs to get project ids for')

        rows = await self.connection.fetch_all(
            _query, {'sequencing_group_ids': sequencing_group_ids}
        )
        projects = set(r['project'] for r in rows)
        if not projects:
            raise ValueError(
                'No projects were found for given sequence groups, this is likely an error'
            )

        return projects

    async def get_sequencing_groups_by_ids(
        self, ids: list[int]
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """
        Get sequence groups by internal identifiers
        """

        _query = f"""
            SELECT {SequencingGroupTable.common_get_keys_str}
            FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            WHERE sg.id IN :sgids
        """

        rows = await self.connection.fetch_all(_query, {'sgids': ids})
        if not rows:
            raise NotFoundError(
                f'Couldn\'t find sequencing groups with internal id {ids})'
            )

        sg_rows = [SequencingGroupInternal.from_db(**dict(r)) for r in rows]
        projects = set(r.project for r in sg_rows)

        return projects, sg_rows

    async def get_assay_ids_by_sequencing_group_ids(
        self, ids: list[int]
    ) -> dict[int, list[int]]:
        """
        Get sequence IDs in a sequencing_group
        """
        _query = """
            SELECT sga.sequencing_group_id, sga.assay_id
            FROM sequencing_group_assay sga
            WHERE sga.sequencing_group_id IN :sgids
        """
        rows = await self.connection.fetch_all(_query, {'sgids': ids})
        sequencing_groups: dict[int, list[int]] = defaultdict(list)
        for row in rows:
            sequencing_groups[row['sequencing_group_id']].append(row['assay_id'])

        return dict(sequencing_groups)

    async def get_all_sequencing_group_ids_by_sample_ids_by_type(
        self,
    ) -> dict[int, dict[str, list[int]]]:
        """
        Get all sequencing group IDs by sample IDs by type
        """
        _query = """
        SELECT s.id as sid, sg.id as sgid, sg.type as sgtype
        FROM sample s
        INNER JOIN sequencing_group sg ON s.id = sg.sample_id
        WHERE project = :project
        """
        rows = await self.connection.fetch_all(_query, {'project': self.project})
        sequencing_group_ids_by_sample_ids_by_type: dict[int, dict[str, list[int]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        for row in rows:
            sample_id = row['sid']
            sg_id = row['sgid']
            sg_type = row['sgtype']
            sequencing_group_ids_by_sample_ids_by_type[sample_id][sg_type].append(sg_id)

        return sequencing_group_ids_by_sample_ids_by_type

    async def get_participant_ids_and_sequencing_group_ids_for_sequencing_type(
        self, sequencing_type: str
    ) -> tuple[set[ProjectId], dict[int, list[int]]]:
        """
        Get participant IDs for a specific sequence type.
        Particularly useful for seqr like cases
        """
        _query = """
        SELECT s.project as project, sg.id as sid, s.participant_id as pid
        FROM sequencing_group sg
        INNER JOIN sample s ON sg.sample_id = s.id
        WHERE sg.type = :seqtype AND project = :project
        """

        rows = list(
            await self.connection.fetch_all(
                _query, {'seqtype': sequencing_type, 'project': self.project}
            )
        )

        projects = set(r['project'] for r in rows)
        participant_id_to_sids: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            participant_id_to_sids[r['pid']].append(r['sid'])

        return projects, participant_id_to_sids

    async def get_sequencing_groups_create_date(
        self, sequencing_group_ids: list[int]
    ) -> dict[int, date]:
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        if len(sequencing_group_ids) == 0:
            return {}
        _query = """
        SELECT id, min(row_start)
        FROM sequencing_group FOR SYSTEM_TIME ALL
        WHERE id in :sgids
        GROUP BY id"""
        rows = await self.connection.fetch_all(_query, {'sgids': sequencing_group_ids})
        return {r[0]: r[1].date() for r in rows}

    async def get_samples_create_date_from_sgs(
        self, sequencing_group_ids: list[int]
    ) -> dict[SequencingGroupInternalId, date]:
        """
        Get a map of {internal_sg_id: sample_date_created} for list of sg_ids
        """
        if len(sequencing_group_ids) == 0:
            return {}

        _query = """
        SELECT sg.id, min(s.row_start)
        FROM sequencing_group sg
        INNER JOIN sample s ON s.id = sg.sample_id
        WHERE sg.id in :sgids
        GROUP BY sg.id
        """
        rows = await self.connection.fetch_all(_query, {'sgids': sequencing_group_ids})
        return {r[0]: r[1].date() for r in rows}

    async def get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[SequencingGroupInternal]]]:
        """Get map of samples by analysis_ids"""
        _query = f"""
        SELECT {SequencingGroupTable.common_get_keys_str}, asg.analysis_id
        FROM analysis_sequencing_group asg
        INNER JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
        INNER JOIN sample s ON s.id = sg.sample_id
        WHERE asg.analysis_id IN :aids
        """
        rows = await self.connection.fetch_all(_query, {'aids': analysis_ids})

        mapped_analysis_to_sequencing_group_id: dict[int, list[int]] = defaultdict(list)
        sg_map: dict[int, SequencingGroupInternal] = {}
        projects: set[int] = set()
        for rrow in rows:
            drow = dict(rrow)
            sid = drow['id']
            analysis_id = drow.pop('analysis_id')
            mapped_analysis_to_sequencing_group_id[analysis_id].append(sid)
            projects.add(drow['project'])

            if sid not in sg_map:
                sg_map[sid] = SequencingGroupInternal.from_db(**drow)

        analysis_map: dict[int, list[SequencingGroupInternal]] = {
            analysis_id: [sg_map.get(sgid) for sgid in sgids]
            for analysis_id, sgids in mapped_analysis_to_sequencing_group_id.items()
        }

        return projects, analysis_map

    async def create_sequencing_group(
        self,
        sample_id: int,
        type_: str,
        technology: str,
        platform: str,
        assay_ids: list[int],
        meta: dict = None,
        open_transaction=True,
    ) -> int:
        """Create sequence group"""
        assert sample_id is not None
        assert type_ is not None
        assert technology is not None
        assert platform is not None

        get_existing_query = """
        SELECT id
        FROM sequencing_group
        WHERE
            sample_id = :sample_id
            AND type = :type
            AND technology = :technology
            AND platform = :platform
            AND NOT archived
        """
        existing_sg_ids = await self.connection.fetch_all(
            get_existing_query,
            {
                'sample_id': sample_id,
                'type': type_.lower(),
                'technology': technology.lower(),
                'platform': platform.lower(),
            },
        )

        _query = """
        INSERT INTO sequencing_group
            (sample_id, type, technology, platform, meta, audit_log_id, archived)
        VALUES
            (:sample_id, :type, :technology, :platform, :meta, :audit_log_id, false)
        RETURNING id;
        """

        _seqg_linker_query = """
        INSERT INTO sequencing_group_assay
            (sequencing_group_id, assay_id, audit_log_id)
        VALUES
            (:seqgroup, :assayid, :audit_log_id)
        """

        values = {
            'sample_id': sample_id,
            'type': type_.lower(),
            'technology': technology.lower(),
            'platform': platform.lower(),
            'meta': to_db_json(meta or {}),
        }
        # check if any values are None and raise an exception if so
        bad_keys = [k for k, v in values.items() if v is None]
        if bad_keys:
            raise ValueError(f'Must provide values for {", ".join(bad_keys)}')

        with_function = self.connection.transaction if open_transaction else NoOpAenter

        async with with_function():
            if existing_sg_ids:
                await self.archive_sequencing_groups([s['id'] for s in existing_sg_ids])

            id_of_seq_group = await self.connection.fetch_val(
                _query,
                {**values, 'audit_log_id': await self.audit_log_id()},
            )
            assay_id_insert_values = [
                {
                    'seqgroup': id_of_seq_group,
                    'assayid': s,
                    'audit_log_id': await self.audit_log_id(),
                }
                for s in assay_ids
            ]
            await self.connection.execute_many(
                _seqg_linker_query, assay_id_insert_values
            )

            return id_of_seq_group

    async def update_sequencing_group(
        self, sequencing_group_id: int, meta: dict, platform: str
    ):
        """
        Update meta / platform on sequencing_group
        """
        updaters = ['audit_log_id = :audit_log_id']
        values: dict[str, Any] = {
            'seqgid': sequencing_group_id,
            'audit_log_id': await self.audit_log_id(),
        }

        if meta:
            values['meta'] = to_db_json(meta)
            updaters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')

        if platform:
            updaters.append('platform = :platform')
            values['platform'] = platform

        _query = f"""
        UPDATE sequencing_group
        SET {', '.join(updaters)}
        WHERE id = :seqgid
        """

        await self.connection.execute(_query, values)

    async def archive_sequencing_groups(self, sequencing_group_id: list[int]):
        """
        Archive sequence group by setting archive flag to TRUE
        """
        _query = """
        UPDATE sequencing_group
        SET archived = 1, audit_log_id = :audit_log_id
        WHERE id = :sequencing_group_id;
        """
        # do this so we can reuse the sequencing_group_ids
        _external_id_query = """
        UPDATE sequencing_group_external_id
        SET nullIfInactive = NULL, audit_log_id = :audit_log_id
        WHERE sequencing_group_id = :sequencing_group_id;
        """
        await self.connection.execute(
            _query,
            {
                'sequencing_group_id': sequencing_group_id,
                'audit_log_id': await self.audit_log_id(),
            },
        )
        await self.connection.execute(
            _external_id_query,
            {
                'sequencing_group_id': sequencing_group_id,
                'audit_log_id': await self.audit_log_id(),
            },
        )

    async def get_type_numbers_for_project(self, project) -> dict[str, int]:
        """
        Get number of sequencing groups for each type for a project
        Useful for the web layer
        """
        _query = """
SELECT sg.type, COUNT(*) as n
FROM sequencing_group sg
INNER JOIN sample s ON s.id = sg.sample_id
WHERE s.project = :project AND NOT sg.archived
GROUP BY sg.type
        """
        rows = await self.connection.fetch_all(_query, {'project': project})
        return {r['type']: r['n'] for r in rows}
