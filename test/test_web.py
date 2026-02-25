import unittest
from typing import Any

import pytest

from api.routes.web import (
    ExportProjectParticipantFields,
    ProjectParticipantGridFilter,
    prepare_participants_for_export,
)
from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers import (
    ParticipantLayer,
    ProjectInsightsLayer,
    SampleLayer,
    SequencingGroupLayer,
    WebLayer,
)
from db.python.tables.participant import ParticipantFilter
from models.enums.web import MetaSearchEntityPrefix
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    Assay,
    AssayInternal,
    AssayUpsertInternal,
    FamilySimple,
    NestedParticipant,
    NestedSample,
    ParticipantUpsertInternal,
    ProjectSummaryInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
    WebProject,
)
from models.models.sequencing_group import NestedSequencingGroup
from models.models.web import (
    ProjectParticipantGridField,
    ProjectParticipantGridFilterType,
    ProjectParticipantGridResponse,
)
from models.utils.sample_id_format import sample_id_format, sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_transform_to_raw,
)


# from test.testbase import TEST_PROJECT_NAME, DbIsolatedTest, run_as_sync


default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}

DEFAULT_FAMILY_FIELDS = [
    ProjectParticipantGridField(
        key='external_ids',
        label='Family ID',
        is_visible=True,
        filter_key='external_id',
    )
]

DEFAULT_PARTICIPANT_FIELDS = [
    ProjectParticipantGridField(
        key='id',
        label='Participant ID',
        is_visible=True,
        filter_key='id',
        filter_types=[
            ProjectParticipantGridFilterType.eq,
            ProjectParticipantGridFilterType.neq,
            ProjectParticipantGridFilterType.gt,
            ProjectParticipantGridFilterType.gte,
            ProjectParticipantGridFilterType.lt,
            ProjectParticipantGridFilterType.lte,
        ],
    ),
    ProjectParticipantGridField(
        key='external_ids',
        label='External Participant ID',
        is_visible=True,
        filter_key='external_id',
    ),
    ProjectParticipantGridField(
        key='reported_sex',
        label='Reported sex',
        is_visible=False,
        filter_key='reported_sex',
    ),
    ProjectParticipantGridField(
        key='reported_gender',
        label='Reported gender',
        is_visible=False,
        filter_key='reported_gender',
        filter_types=None,
    ),
    ProjectParticipantGridField(
        key='karyotype',
        label='Karyotype',
        is_visible=False,
        filter_key='karyotype',
        filter_types=None,
    ),
]

DEFAULT_SAMPLE_FIELDS = [
    ProjectParticipantGridField(
        key='id',
        label='Sample ID',
        is_visible=True,
        filter_key='id',
        filter_types=[
            ProjectParticipantGridFilterType.eq,
            ProjectParticipantGridFilterType.neq,
        ],
    ),
    ProjectParticipantGridField(
        key='external_ids',
        label='External Sample ID',
        is_visible=True,
        filter_key='external_id',
    ),
    ProjectParticipantGridField(
        key='sample_root_id',
        label='Root Sample ID',
        is_visible=False,
        filter_key='sample_root_id',
    ),
    ProjectParticipantGridField(
        key='sample_parent_id',
        label='Parent Sample ID',
        is_visible=False,
        filter_key='sample_root_id',
    ),
    ProjectParticipantGridField(
        key='type', label='Type', is_visible=True, filter_key=None, filter_types=None
    ),
    ProjectParticipantGridField(
        key='created_date',
        label='Created date',
        is_visible=True,
        filter_key=None,
        filter_types=None,
    ),
]

DEFAULT_SEQ_GROUP_FIELDS = [
    ProjectParticipantGridField(
        key='id',
        label='Sequencing Group ID',
        is_visible=True,
        filter_key='id',
        filter_types=[
            ProjectParticipantGridFilterType.eq,
            ProjectParticipantGridFilterType.neq,
        ],
    ),
    ProjectParticipantGridField(
        key='type',
        label='Type',
        is_visible=True,
        filter_key='type',
    ),
    ProjectParticipantGridField(
        key='technology',
        label='Technology',
        is_visible=True,
        filter_key='technology',
    ),
    ProjectParticipantGridField(
        key='platform',
        label='Platform',
        is_visible=True,
        filter_key='platform',
    ),
]

DEFAULT_ASSAY_TYPES = [
    ProjectParticipantGridField(
        key='id',
        label='Assay ID',
        is_visible=True,
        filter_key='id',
        filter_types=[
            ProjectParticipantGridFilterType.eq,
            ProjectParticipantGridFilterType.neq,
        ],
    ),
    ProjectParticipantGridField(
        key='type', label='Type', is_visible=True, filter_key='type', filter_types=None
    ),
    ProjectParticipantGridField(
        key='meta.sequencing_type',
        label='sequencing_type',
        is_visible=False,
        filter_key='meta.sequencing_type',
        filter_types=None,
    ),
    ProjectParticipantGridField(
        key='meta.sequencing_platform',
        label='sequencing_platform',
        is_visible=False,
        filter_key='meta.sequencing_platform',
        filter_types=None,
    ),
    ProjectParticipantGridField(
        key='meta.sequencing_technology',
        label='sequencing_technology',
        is_visible=False,
        filter_key='meta.sequencing_technology',
        filter_types=None,
    ),
    ProjectParticipantGridField(
        key='meta.batch',
        label='batch',
        is_visible=True,
        filter_key='meta.batch',
        filter_types=None,
    ),
]


def data_to_class(data: dict | list) -> dict | list:
    """Convert the data into it's class using the _class field"""
    if isinstance(data, list):
        return [data_to_class(x) for x in data]

    if not isinstance(data, dict):
        return data

    cls = data.pop('_class', None)
    mapped_data = {k: data_to_class(v) for k, v in data.items()}

    if isinstance(cls, type):
        return cls(**mapped_data)

    return mapped_data


def merge(d1: dict, d2: dict):
    """Merges two dictionaries"""
    return dict(d1, **d2)


@pytest.fixture
def test_participant() -> ParticipantUpsertInternal:
    """Do it like this to avoid an upsert writing the test value"""
    return ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Demeter'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id001'},
                meta={},
                type='blood',
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id001.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id001.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    'batch': 'M001',
                                    **default_assay_meta,
                                },
                            ),
                        ],
                    )
                ],
            )
        ],
    )


@pytest.fixture
def test_participant_2() -> ParticipantUpsertInternal:
    """Do it like this to avoid an upsert writing the test value"""
    return ParticipantUpsertInternal(
        external_ids={PRIMARY_EXTERNAL_ORG: 'Meter'},
        meta={},
        samples=[
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'sample_id002'},
                meta={},
                type='blood',
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='Illumina',
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id002.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id002.filename-R1.fastq.gz',
                                            'size': 112,
                                        },
                                        {
                                            'basename': 'sample_id002.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id002.filename-R2.fastq.gz',
                                            'size': 112,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    'batch': 'M001',
                                    'field with spaces': 'field with spaces',
                                    **default_assay_meta,
                                },
                            ),
                        ],
                    ),
                ],
            )
        ],
    )


SINGLE_PARTICIPANT_SUMMARY_RESULT = ProjectSummaryInternal(
    project=WebProject(id=1, name='test-project', meta={}, dataset='test-dataset'),
    total_samples=1,
    total_participants=1,
    total_sequencing_groups=1,
    total_assays=1,
    cram_seqr_stats={
        'genome': {
            'Sequences': '1',
            'Crams': '0',
            'Seqr': '0',
        }
    },
    batch_sequencing_group_stats={'M001': {'genome': '1'}},
    seqr_links={},
    seqr_sync_types=[],
)

SINGLE_PARTICIPANT_QUERY_RESULT = ProjectParticipantGridResponse(
    participants=[],
    total_results=1,
    fields={
        MetaSearchEntityPrefix.FAMILY: [
            ProjectParticipantGridField(
                key='external_ids', label='Family ID', is_visible=True
            )
        ],
        MetaSearchEntityPrefix.PARTICIPANT: [
            ProjectParticipantGridField(
                key='external_ids', label='Participant ID', is_visible=True
            ),
        ],
        MetaSearchEntityPrefix.SAMPLE: [
            ProjectParticipantGridField(key='meta.skey', label='', is_visible=True),
            ProjectParticipantGridField(key='external_ids', label='', is_visible=True),
        ],
        MetaSearchEntityPrefix.SEQUENCING_GROUP: [
            ProjectParticipantGridField(key='type', label='', is_visible=True),
            ProjectParticipantGridField(key='meta.sgkey', label='', is_visible=True),
        ],
        MetaSearchEntityPrefix.ASSAY: [
            ProjectParticipantGridField(key='type', label='', is_visible=True),
            ProjectParticipantGridField(key='meta.akey', label='', is_visible=True),
        ],
    },
)


class TestWeb:
    """Test web class containing web endpoints"""

    maxDiff = None

    @pytest.fixture(autouse=True)
    async def setUp(self, connection_with_project: Connection) -> None:
        self.connection = connection_with_project

        self.web_layer = WebLayer(connection_with_project)
        self.participant_layer = ParticipantLayer(connection_with_project)
        self.proj_insights_layer = ProjectInsightsLayer(connection_with_project)
        self.sample_layer = SampleLayer(connection_with_project)
        self.sg_layer = SequencingGroupLayer(connection_with_project)

    @pytest.mark.asyncio
    async def test_project_summary_empty(self):
        """Test getting the summary for a project"""
        result = await self.web_layer.get_project_summary()

        # Expect an empty project
        expected = ProjectSummaryInternal(
            project=WebProject(
                id=1, name='test-project', meta={}, dataset='test-dataset'
            ),
            total_samples=0,
            total_participants=0,
            total_sequencing_groups=0,
            total_assays=0,
            batch_sequencing_group_stats={},
            cram_seqr_stats={},
            seqr_links={},
            seqr_sync_types=[],
        )

        assert expected == result

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_project_summary_single_entry(
        self, test_participant: ParticipantUpsertInternal
    ):
        """Test project summary with a single participant with all fields"""
        # Now add a participant with a sample and sequence
        await self.participant_layer.upsert_participants(
            participants=[test_participant]
        )

        result = await self.web_layer.get_project_summary()
        assert result == SINGLE_PARTICIPANT_SUMMARY_RESULT

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_project_summary_to_external(
        self, test_participant: ParticipantUpsertInternal
    ):
        """Test project summary to_external function"""
        # Now add a participant with a sample and sequence
        await self.participant_layer.upsert_participants(
            participants=[test_participant]
        )

        summary = await self.web_layer.get_project_summary()
        assert SINGLE_PARTICIPANT_SUMMARY_RESULT.to_external() == summary.to_external()

        internal_participants = await self.web_layer.query_participants(
            ParticipantFilter(), limit=None
        )

        ex_result = ProjectParticipantGridResponse.from_params(
            participants=internal_participants,
            total_results=1,
            filter_fields=ProjectParticipantGridFilter(),
        )

        assert isinstance(internal_participants[0].samples, list)
        assert isinstance(internal_participants[0].samples[0].id, int)
        assert isinstance(ex_result.participants[0].samples[0].id, str)
        assert (
            sample_id_transform_to_raw(ex_result.participants[0].samples[0].id)
            == internal_participants[0].samples[0].id
        )

        assert isinstance(internal_participants[0].samples[0].sequencing_groups, list)
        assert isinstance(ex_result.participants[0].samples[0].sequencing_groups, list)

        assert isinstance(
            internal_participants[0].samples[0].sequencing_groups[0].id, int
        )
        assert isinstance(
            ex_result.participants[0].samples[0].sequencing_groups[0].id, str
        )
        assert (
            sequencing_group_id_transform_to_raw(
                ex_result.participants[0].samples[0].sequencing_groups[0].id
            )
            == internal_participants[0].samples[0].sequencing_groups[0].id
        )

        assert isinstance(
            internal_participants[0].samples[0].sequencing_groups[0].assays, list
        )
        assert isinstance(
            ex_result.participants[0].samples[0].sequencing_groups[0].assays, list
        )
        assert isinstance(
            internal_participants[0].samples[0].sequencing_groups[0].assays[0],
            AssayInternal,
        )
        assert isinstance(
            ex_result.participants[0].samples[0].sequencing_groups[0].assays[0], Assay
        )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    @pytest.mark.skip(reason='Querying JSON keys is not implemented at the moment')
    # TODO Revisit this when querying JSON keys is implemented
    async def test_project_summary_with_filter_with_results(
        self, test_participant: ParticipantUpsertInternal
    ):
        """Project grid but with test filter, that shows results"""
        await self.participant_layer.upsert_participants(
            participants=[test_participant]
        )

        pfilter = ProjectParticipantGridFilter(
            assay=ProjectParticipantGridFilter.ParticipantGridAssayFilter(
                meta={'batch': GenericFilter[Any](startswith='M001')}
            )
        )

        nested_participants = await self.web_layer.query_participants(
            pfilter.to_internal(project=self.connection.project_id), limit=None
        )
        result = ProjectParticipantGridResponse.from_params(
            participants=nested_participants,
            total_results=1,
            filter_fields=pfilter,
        )
        assert len(nested_participants) == 1
        result.participants = []
        assert result == SINGLE_PARTICIPANT_QUERY_RESULT

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    @pytest.mark.skip(reason='Querying JSON keys is not implemented at the moment')
    # TODO Revisit this when querying JSON keys is implemented
    async def test_project_summary_with_filter_no_results(
        self, test_participant: ParticipantUpsertInternal
    ):
        """Project grid but with test filter, that doesn't have results"""
        await self.participant_layer.upsert_participants(
            participants=[test_participant]
        )
        pfilter = ProjectParticipantGridFilter(
            assay=ProjectParticipantGridFilter.ParticipantGridAssayFilter(
                meta={'batch': GenericFilter[Any](startswith='M002')}
            )
        )

        nested_participants = await self.web_layer.query_participants(
            pfilter.to_internal(project=self.connection.project_id), limit=None
        )
        assert len(nested_participants) == 0

        result = ProjectParticipantGridResponse.from_params(
            participants=nested_participants,
            total_results=0,
            filter_fields=pfilter,
        )
        result.participants = []
        assert result == SINGLE_PARTICIPANT_QUERY_RESULT

        empty_result = ProjectParticipantGridResponse(
            total_results=0, participants=[], fields={}
        )

        assert empty_result == result

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_project_summary_multiple_participants(
        self,
        test_participant: ParticipantUpsertInternal,
        test_participant_2: ParticipantUpsertInternal,
    ):
        """Try with multiple participants as some extra security"""
        await self.participant_layer.upsert_participants(
            participants=[test_participant, test_participant_2]
        )

        expected_summary = ProjectSummaryInternal(
            project=WebProject(
                id=1, name='test-project', meta={}, dataset='test-dataset'
            ),
            total_samples=2,
            total_participants=2,
            total_sequencing_groups=2,
            total_assays=2,
            cram_seqr_stats={
                'genome': {
                    'Sequences': '2',
                    'Crams': '0',
                    'Seqr': '0',
                }
            },
            batch_sequencing_group_stats={'M001': {'genome': '2'}},
            seqr_links={},
            seqr_sync_types=[],
        )

        expected_fields = {
            MetaSearchEntityPrefix.FAMILY: DEFAULT_FAMILY_FIELDS,
            MetaSearchEntityPrefix.PARTICIPANT: DEFAULT_PARTICIPANT_FIELDS,
            MetaSearchEntityPrefix.SAMPLE: DEFAULT_SAMPLE_FIELDS,
            MetaSearchEntityPrefix.SEQUENCING_GROUP: DEFAULT_SEQ_GROUP_FIELDS,
            MetaSearchEntityPrefix.ASSAY: [
                *DEFAULT_ASSAY_TYPES,
                ProjectParticipantGridField(
                    key='meta.reads_type',
                    label='reads_type',
                    is_visible=True,
                    filter_key='meta.reads_type',
                    filter_types=None,
                ),
                ProjectParticipantGridField(
                    key='meta.reads',
                    label='reads',
                    is_visible=False,
                    filter_key='meta.reads',
                    filter_types=None,
                ),
                ProjectParticipantGridField(
                    key='meta.field with spaces',
                    label='field with spaces',
                    is_visible=True,
                    filter_key='meta.field with spaces',
                    filter_types=None,
                ),
            ],
        }

        summary = await self.web_layer.get_project_summary()

        assert expected_summary == summary

        nested_participants = await self.web_layer.query_participants(
            ParticipantFilter(), limit=None
        )
        assert len(nested_participants) == 2
        result = ProjectParticipantGridResponse.from_params(
            participants=nested_participants,
            total_results=2,
            filter_fields=ProjectParticipantGridFilter(),
        )
        # make it easier to test
        result.participants = []

        self.maxDiff = None
        # comparison
        for k, expected_field in expected_fields.items():
            sorted_expected_fields = sorted(
                expected_field, key=lambda x: x.key
            )  # sort by key
            sorted_result_fields = sorted(
                result.fields[k], key=lambda x: x.key
            )  # sort by key
            assert sorted_expected_fields == sorted_result_fields, (
                f'Fields for category {k} did not match'
            )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_project_summary_multiple_participants_and_filter(
        self,
        test_participant: ParticipantUpsertInternal,
        test_participant_2: ParticipantUpsertInternal,
    ):
        """Try with multiple participants as some extra security"""
        await self.participant_layer.upsert_participants(
            participants=[test_participant, test_participant_2]
        )

        expected_fields = {
            MetaSearchEntityPrefix.FAMILY: DEFAULT_FAMILY_FIELDS,
            MetaSearchEntityPrefix.PARTICIPANT: DEFAULT_PARTICIPANT_FIELDS,
            MetaSearchEntityPrefix.SAMPLE: DEFAULT_SAMPLE_FIELDS,
            MetaSearchEntityPrefix.SEQUENCING_GROUP: DEFAULT_SEQ_GROUP_FIELDS,
            MetaSearchEntityPrefix.ASSAY: [
                *DEFAULT_ASSAY_TYPES,
                ProjectParticipantGridField(
                    key='meta.reads_type',
                    label='reads_type',
                    is_visible=True,
                    filter_key='meta.reads_type',
                    filter_types=None,
                ),
                ProjectParticipantGridField(
                    key='meta.reads',
                    label='reads',
                    is_visible=False,
                    filter_key='meta.reads',
                    filter_types=None,
                ),
                ProjectParticipantGridField(
                    key='meta.field with spaces',
                    label='field with spaces',
                    is_visible=True,
                    filter_key='meta.field with spaces',
                    filter_types=None,
                ),
            ],
        }

        pfilter = ProjectParticipantGridFilter(
            sample=ProjectParticipantGridFilter.ParticipantGridSampleFilter(
                external_id=GenericFilter[str](contains='_id002')
            )
        )

        nested_participants = await self.web_layer.query_participants(
            pfilter.to_internal(project=self.connection.project_id), limit=None
        )

        assert len(nested_participants) == 1
        result = ProjectParticipantGridResponse.from_params(
            nested_participants,
            filter_fields=pfilter,
            total_results=2,
        )
        result.participants = []

        for k, expected_field in expected_fields.items():
            sorted_expected_fields = sorted(
                expected_field, key=lambda x: x.key
            )  # sort by key
            sorted_result_fields = sorted(
                result.fields[k], key=lambda x: x.key
            )  # sort by key
            assert sorted_expected_fields == sorted_result_fields, (
                f'Fields for category {k} did not match'
            )

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    @pytest.mark.skip(reason='Querying JSON keys is not implemented at the moment')
    # TODO Revisit this when querying JSON keys is implemented
    async def test_field_with_space(
        self,
        test_participant: ParticipantUpsertInternal,
        test_participant_2: ParticipantUpsertInternal,
    ):
        """Test filtering on a meta field with spaces"""
        await self.participant_layer.upsert_participants(
            participants=[test_participant, test_participant_2]
        )

        pfilter = ProjectParticipantGridFilter(
            assay=ProjectParticipantGridFilter.ParticipantGridAssayFilter(
                meta={'field with spaces': GenericFilter[Any](contains='field wi')}
            )
        )
        nested_participants = await self.web_layer.query_participants(
            pfilter.to_internal(project=self.connection.project_id), limit=None
        )

        assert len(nested_participants) == 1

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['writer'])
    async def test_project_summary_inactive_sequencing_group(
        self, test_participant: ParticipantUpsertInternal
    ):
        """
        Insert a sequencing-group, archive it, then check that the summary
        doesn't return that sequencing group
        """
        participants = await self.participant_layer.upsert_participants(
            participants=[test_participant]
        )
        assert isinstance(participants, list)
        assert isinstance(participants[0].samples, list)
        assert isinstance(participants[0].samples[0].sequencing_groups, list)

        sg = participants[0].samples[0].sequencing_groups[0]
        assert isinstance(sg.assays, list)
        assay_ids = [a.id for a in sg.assays if a.id]

        assert sg.id
        new_sg_id = await self.sg_layer.recreate_sequencing_group_with_new_assays(
            sequencing_group_id=sg.id,
            assays=assay_ids,
            meta={'new-meta': 'value'},
        )

        participants = await self.web_layer.query_participants(
            ParticipantFilter(), limit=None
        )
        summary_sgs = participants[0].samples[0].sequencing_groups
        assert len(summary_sgs) == 1
        assert new_sg_id == summary_sgs[0].id


class WebNonDBTests(unittest.TestCase):
    """Handy place to put tests that don't require a database"""

    def test_nested_participant_to_rows(self):
        """Test nested participant to flat rows with a projection"""
        participant = NestedParticipant(
            id=1,
            external_ids={PRIMARY_EXTERNAL_ORG: 'pex1'},
            meta={'pkey': 'value'},
            families=[FamilySimple(id=-2, external_ids={'family_org': 'fex1'})],
            samples=[
                NestedSample(
                    id='xpgA',
                    external_ids={PRIMARY_EXTERNAL_ORG: 'sex1', 'external_org': 'ex02'},
                    meta={'skey': 'svalue'},
                    type='blood',
                    created_date='2021-01-01',
                    non_sequencing_assays=[],
                    sample_root_id=None,
                    sample_parent_id=None,
                    sequencing_groups=[
                        NestedSequencingGroup(
                            id='cpgA',
                            type='genome',
                            external_ids={'sgex1': 'sgex1'},
                            technology='short-read',
                            platform='illumina',
                            meta={'sgkey': 'sgvalue'},
                            assays=[
                                Assay(
                                    id=-1,
                                    type='sequencing',
                                    external_ids={'ex1': 'ex1'},
                                    sample_id='xpgA',
                                    meta={'akey': 'avalue'},
                                ),
                                Assay(
                                    id=-2,
                                    type='sequencing',
                                    external_ids={'ex1': 'ex2'},
                                    sample_id='xpgA',
                                    meta={'akey': 'avalue2'},
                                ),
                            ],
                        )
                    ],
                )
            ],
        )
        fields = ExportProjectParticipantFields(
            fields={
                MetaSearchEntityPrefix.FAMILY: [
                    ProjectParticipantGridField(
                        key='external_ids', label='', is_visible=True
                    )
                ],
                MetaSearchEntityPrefix.PARTICIPANT: [
                    ProjectParticipantGridField(
                        key='external_ids', label='', is_visible=True
                    ),
                    ProjectParticipantGridField(
                        key='meta.pkey', label='', is_visible=True
                    ),
                ],
                MetaSearchEntityPrefix.SAMPLE: [
                    ProjectParticipantGridField(
                        key='meta.skey', label='', is_visible=True
                    ),
                    ProjectParticipantGridField(
                        key='external_ids', label='', is_visible=True
                    ),
                ],
                MetaSearchEntityPrefix.SEQUENCING_GROUP: [
                    ProjectParticipantGridField(key='type', label='', is_visible=True),
                    ProjectParticipantGridField(
                        key='meta.sgkey', label='', is_visible=True
                    ),
                ],
                MetaSearchEntityPrefix.ASSAY: [
                    ProjectParticipantGridField(key='type', label='', is_visible=True),
                    ProjectParticipantGridField(
                        key='meta.akey', label='', is_visible=True
                    ),
                ],
            }
        )

        i = prepare_participants_for_export([participant], fields)
        headers = next(i)
        rows = list(i)
        assert headers == (
            'family.external_ids',
            'participant.external_ids',
            'participant.meta.pkey',
            'sample.meta.skey',
            'sample.external_ids',
            'sequencing_group.type',
            'sequencing_group.meta.sgkey',
            'assay.type',
            'assay.meta.akey',
        )

        non_sg_keys = (
            'family_org: fex1',
            'pex1',
            'value',
            'svalue',
            'sex1, external_org: ex02',
            'genome',
            'sgvalue',
        )
        expected = [
            (*non_sg_keys, 'sequencing', 'avalue'),
            (*non_sg_keys, 'sequencing', 'avalue2'),
        ]

        assert len(rows) == 2

        assert expected == rows

    def test_project_participant_grid_filter(self):
        """
        Test every filter, and make sure it's being converted
        to the internal model correctly
        """
        f_id = 1
        p_id = 2
        s_id = 3
        s_id_ext = sample_id_format(s_id)
        sg_id = 4
        sg_id_ext = sequencing_group_id_format(sg_id)
        big_filter = ProjectParticipantGridFilter(
            family=ProjectParticipantGridFilter.ParticipantGridFamilyFilter(
                id=GenericFilter[int](contains=f_id)
            ),
            participant=ProjectParticipantGridFilter.ParticipantGridParticipantFilter(
                id=GenericFilter[int](contains=p_id),
                meta={'pmeta': GenericFilter[Any](contains='pm')},
                external_id=GenericFilter[str](contains='e'),
            ),
            sample=ProjectParticipantGridFilter.ParticipantGridSampleFilter(
                id=GenericFilter[str](contains=s_id_ext),
                type=GenericFilter[str](contains='t'),
                external_id=GenericFilter[str](contains='e'),
                meta={'smeta': GenericFilter[Any](contains='sm')},
            ),
            sequencing_group=ProjectParticipantGridFilter.ParticipantGridSequencingGroupFilter(
                id=GenericFilter[str](contains=sg_id_ext),
                type=GenericFilter[str](contains='t'),
                external_id=GenericFilter[str](contains='e'),
                meta={'sgmeta': GenericFilter[Any](contains='sg')},
                technology=GenericFilter[str](contains='t'),
                platform=GenericFilter[str](contains='p'),
            ),
            assay=ProjectParticipantGridFilter.ParticipantGridAssayFilter(
                id=GenericFilter[int](contains=5),
                type=GenericFilter[str](contains='t'),
                external_id=GenericFilter[str](contains='e'),
                meta={'ameta': GenericFilter[Any](contains='a')},
            ),
        )

        internal_filter = big_filter.to_internal(project=1)

        # participant internal
        assert internal_filter.id.contains == p_id
        assert internal_filter.meta['pmeta'].contains == 'pm'
        assert internal_filter.external_id.contains == 'e'

        # family internal
        assert internal_filter.family.id.contains == f_id

        # sample internal
        assert internal_filter.sample.id.contains == s_id
        assert internal_filter.sample.type.contains == 't'
        assert internal_filter.sample.external_id.contains == 'e'
        assert internal_filter.sample.meta['smeta'].contains == 'sm'

        # sequencing group internal
        assert internal_filter.sequencing_group.id.contains == sg_id
        assert internal_filter.sequencing_group.type.contains == 't'
        assert internal_filter.sequencing_group.external_id.contains == 'e'
        assert internal_filter.sequencing_group.meta['sgmeta'].contains == 'sg'
        assert internal_filter.sequencing_group.technology.contains == 't'
        assert internal_filter.sequencing_group.platform.contains == 'p'

        # assay internal
        assert internal_filter.assay.id.contains == 5
        assert internal_filter.assay.type.contains == 't'
        assert internal_filter.assay.external_id.contains == 'e'
        assert internal_filter.assay.meta['ameta'].contains == 'a'
