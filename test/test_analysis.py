import time
from datetime import UTC, datetime

import pytest

from api.routes.analysis import AnalysisUpdateModel, update_analysis
from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
    parse_sql_bool,
)


class TestAnalysis:
    """Test sample class"""

    @pytest.fixture(autouse=True)
    async def setUp(self, connection_with_project: Connection) -> None:
        self.connection = connection_with_project

        assert connection_with_project.project_id is not None
        self.project_id = connection_with_project.project_id

        self.sgl = SequencingGroupLayer(connection_with_project)
        self.asl = AssayLayer(connection_with_project)
        self.al = AnalysisLayer(connection_with_project)
        self.pl = ParticipantLayer(connection_with_project)
        self.sl = SampleLayer(connection_with_project)

        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                meta={'meta': 'meta ;)'},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'genome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'exome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                ],
            )
        )
        self.sample_id = sample.id
        assert sample.sequencing_groups is not None
        assert len(sample.sequencing_groups) == 2
        assert sample.sequencing_groups[0].id is not None
        assert sample.sequencing_groups[1].id is not None

        self.genome_sequencing_group_id: int = sample.sequencing_groups[0].id
        self.exome_sequencing_group_id: int = sample.sequencing_groups[1].id

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_analysis_by_id(self):
        """
        Test getting an analysis by id
        """
        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        analysis = await self.al.get_analysis_by_id(analysis_id)
        assert analysis.id == analysis_id
        assert analysis.type == 'cram'
        assert analysis.status == AnalysisStatus.COMPLETED

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_empty_query(self):
        """
        Test empty IDs to see the query construction
        """
        analyses = await self.al.query(AnalysisFilter(id=GenericFilter(in_=[])))
        assert len(analyses) == 0

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_add_cram(self):
        """
        Test adding an analysis of type CRAM
        """

        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        acur = await self.connection.pg_connection.execute('SELECT * FROM analysis')
        analyses = await acur.fetchall()

        acur = await self.connection.pg_connection.execute(
            'SELECT * FROM analysis_sequencing_group'
        )
        analysis_sgs = await acur.fetchall()

        assert len(analyses) == 1
        assert analyses[0]['id'] == analysis_id
        assert analysis_sgs[0]['sequencing_group_id'] == 1
        assert analysis_sgs[0]['analysis_id'] == analyses[0]['id']

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_analysis(self):
        """
        Test adding an analysis of type ANALYSIS_RUNNER
        """

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                meta={},
            )
        )

        analyses = await self.al.query(
            AnalysisFilter(
                project=GenericFilter(eq=self.project_id),
                type=GenericFilter(eq='analysis-runner'),
            )
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                cohort_ids=[],
                output=None,
                timestamp_completed=None,
                project=1,
                meta={},
                active=True,
                author=None,
            )
        ]

        assert expected == analyses

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_analysis_by_meta_isnull(self):
        """
        Test getting an analysis by a meta query that uses isnull
        this tests that the comparison works even if there isn't a primitive
        value at the specified path. If we were to use JSON_VALUE in the query this
        wouldn't work.
        """

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                meta={'nested_meta': {'foo': 'bar'}},
            )
        )

        analyses = await self.al.query(
            AnalysisFilter(
                project=GenericFilter(eq=self.project_id),
                meta={'nested_meta': GenericFilter(isnull=False)},
            )
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                cohort_ids=[],
                output=None,
                timestamp_completed=None,
                project=1,
                meta={'nested_meta': {'foo': 'bar'}},
                active=True,
                author=None,
            )
        ]

        assert expected == analyses

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_analysis_by_meta_in_(self):
        """
        Test getting an analysis by a meta query that uses in_ filter. These filters
        were previously not working due to JSON_EXTRACT returning a json value that
        failed comparison to the input list of strings.
        """

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                meta={'foo': 'bar'},
            )
        )

        analyses = await self.al.query(
            AnalysisFilter(
                project=GenericFilter(eq=self.project_id),
                meta={'foo': GenericFilter(in_=['bar', 'baz'])},
            )
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                cohort_ids=[],
                output=None,
                timestamp_completed=None,
                project=1,
                meta={'foo': 'bar'},
                active=True,
                author=None,
            )
        ]

        assert expected == analyses

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_sample_cram_path_map_for_seqr(self):
        """
        Exercise get_sample_cram_path_map_for_seqr()
        """

        part = await self.pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'PEXT1'},
                    meta={},
                    samples=[SampleUpsertInternal(id=self.sample_id)],
                ),
            ],
        )

        assert len(part) == 1
        assert part[0].id is not None

        id_map = await self.al.get_sample_cram_path_map_for_seqr(
            self.project_id, ['blood'], [part[0].id]
        )
        assert isinstance(id_map, list)

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_sgs_by_analysis_id_with_no_eids(self):
        """
        Test get_sgs_by_analysis_id()
        """

        # duplicate here to ensure sgs don't have any external ids
        assay_meta = {
            'sequencing_type': 'genome',
            'sequencing_technology': 'short-read',
            'sequencing_platform': 'illumina',
        }
        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'test_sgs_aid'},
                type='blood',
                meta={},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta=assay_meta,
                            )
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta=assay_meta,
                            )
                        ],
                    ),
                ],
            )
        )

        assert sample.sequencing_groups is not None
        assert len(sample.sequencing_groups) == 2
        assert sample.sequencing_groups[0].id is not None
        assert sample.sequencing_groups[1].id is not None

        genome_id = sample.sequencing_groups[0].id
        exome_id = sample.sequencing_groups[1].id

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[genome_id, exome_id],
                meta={},
            )
        )

        sgs_by_aid = await self.sgl.get_sequencing_groups_by_analysis_ids([a_id])
        assert a_id in sgs_by_aid

        sgs = sorted(sgs_by_aid[a_id], key=lambda sg: sg.id or 0)

        assert genome_id == sgs[0].id
        assert exome_id == sgs[1].id

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_create_analysis_with_timestamp(self):
        """Tests that analyses can be backdated by suppling timestamp_completed"""
        # Test creation with a manually-set timestamp
        test_timestamp = datetime(2013, 2, 22, 0, 0, tzinfo=UTC)
        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[],
                timestamp_completed=test_timestamp,
                meta={},
            ),
        )

        # get the timestamp_completed of the analysis from the db
        init_analysis = await self.al.query(AnalysisFilter(id=GenericFilter(eq=a_id)))

        assert test_timestamp == init_analysis[0].timestamp_completed

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_update_analysis(self):
        """
        Test Analysis update
        """

        # create an analysis
        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[],
                meta={},
            ),
        )

        # get the timestamp_completed of the analysis
        init_analyses = await self.al.query(
            AnalysisFilter(
                project=GenericFilter(eq=self.project_id),
                type=GenericFilter(eq='analysis-runner'),
            )
        )

        # store the timestamp_completed
        init_timestamp_completed = init_analyses[0].timestamp_completed

        # be sure the now is different than before
        time.sleep(2)

        # test that updating with an incorrect output string raises an exception.
        with pytest.raises(ValueError):
            await self.al.update_analysis(
                a_id,
                meta={'sequencing_type': 'genome', 'size': 1024},
                output='test_output',
            )

        # update the analysis with some new data
        await self.al.update_analysis(
            a_id,
            meta={'sequencing_type': 'genome', 'size': 1024},
            output='test://test_output',
        )

        # check the analysis after update
        # be sure timestamp_completed has not been touched
        analyses = await self.al.query(
            AnalysisFilter(
                project=GenericFilter(eq=self.project_id),
                type=GenericFilter(eq='analysis-runner'),
            ),
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[],
                cohort_ids=[],
                output='test://test_output',
                outputs='test://test_output',
                timestamp_completed=init_timestamp_completed,
                project=1,
                meta={'sequencing_type': 'genome', 'size': 1024},
                active=True,
                author=None,
            ),
        ]

        assert expected == analyses

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_route_update_active(self):
        """
        Test that update_analysis(active=False) is effective
        """
        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
            )
        )

        acur = await self.connection.pg_connection.execute('SELECT * FROM analysis')
        analyses = await acur.fetchall()
        assert len(analyses) == 1
        assert analyses[0]['id'] == analysis_id
        assert parse_sql_bool(analyses[0]['active'])

        inactivate = AnalysisUpdateModel(active=False, status=AnalysisStatus.COMPLETED)
        await update_analysis(analysis_id, inactivate, self.connection)

        acur = await self.connection.pg_connection.execute('SELECT * FROM analysis')
        analyses = await acur.fetchall()
        assert len(analyses) == 1
        assert analyses[0]['id'] == analysis_id
        assert not parse_sql_bool(analyses[0]['active'])

        analyses = await self.al.query(
            AnalysisFilter(project=GenericFilter(eq=self.project_id))
        )
        assert len(analyses) == 1
        assert analyses[0].id == analysis_id
        assert not analyses[0].active

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_latest_complete_analysis(self):
        """
        Test getting the most recently completed analysis' id
        """
        analysis_first = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
                timestamp_completed=datetime(2025, 12, 31),
            )
        )

        # This analysis is not the absolutel last to be completed, but it is the last that matches
        # the meta filtering criteria, so it should be selected
        analysis_last_matching_meta = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.exome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': None},
                timestamp_completed=datetime(2026, 1, 1),
            )
        )

        # This is the absolute last to be completed, but its meta does not match the filter
        # so it should not be selected
        analysis_last = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.exome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
                timestamp_completed=datetime(2026, 1, 2),
            )
        )

        assert analysis_last_matching_meta != analysis_first
        assert analysis_last_matching_meta != analysis_last

        latest_complete = await self.al.get_latest_complete_analysis_for_type(
            self.project_id, 'cram', {'sequencing_type': 'genome', 'size': None}
        )

        assert latest_complete.id == analysis_last_matching_meta

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_sg_without_given_type(self):
        """
        Test getting sequencing group IDs whose associated analysis is not of a given type
        """
        analysis_id = await self.al.create_analysis(
            AnalysisInternal(
                type='cram',
                status=AnalysisStatus.COMPLETED,
                sequencing_group_ids=[self.genome_sequencing_group_id],
                meta={'sequencing_type': 'genome', 'size': 1024},
            )
        )

        analysis = await self.al.get_analysis_by_id(analysis_id)
        assert analysis.id == analysis_id

        sg_without_type = (
            await self.al.get_all_sequencing_group_ids_without_analysis_type(
                self.project_id, 'cram'
            )
        )
        assert len(sg_without_type) == 1
        assert sg_without_type[0] == self.exome_sequencing_group_id
