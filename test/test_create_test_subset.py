from test.testbase import DbIsolatedTest, run_as_sync
import unittest
import unittest.mock

from scripts.create_test_subset import TestSubsetGenerator
from db.python.layers.participant import ParticipantLayer
from db.python.tables.project import ProjectPermissionsTable
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.models.assay import AssayUpsertInternal

default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}

all_participants = [
    ParticipantUpsertInternal(
        external_id='Demeter',
        meta={},
        samples=[
            SampleUpsertInternal(
                external_id='sample_id001',
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
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
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    ),
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id001.exome.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.exome.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id001.exome.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id001.exome.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    ),
                ],
                type='blood',
            )
        ],
    ),
    ParticipantUpsertInternal(
        external_id='Apollo',
        meta={},
        samples=[
            SampleUpsertInternal(
                external_id='sample_id002',
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id002.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id002.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id002.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id002.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    ),
                ],
                type='blood',
            ),
            SampleUpsertInternal(
                external_id='sample_id004',
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id004.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id004.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id004.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id004.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    )
                ],
                type='blood',
            ),
        ],
    ),
    ParticipantUpsertInternal(
        external_id='Athena',
        meta={},
        samples=[
            SampleUpsertInternal(
                external_id='sample_id003',
                meta={},
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        assays=[
                            AssayUpsertInternal(
                                meta={
                                    'reads': [
                                        {
                                            'basename': 'sample_id003.filename-R1.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id003.filename-R1.fastq.gz',
                                            'size': 111,
                                        },
                                        {
                                            'basename': 'sample_id003.filename-R2.fastq.gz',
                                            'checksum': None,
                                            'class': 'File',
                                            'location': '/path/to/sample_id003.filename-R2.fastq.gz',
                                            'size': 111,
                                        },
                                    ],
                                    'reads_type': 'fastq',
                                    **default_assay_meta,
                                },
                                type='sequencing',
                            )
                        ],
                    )
                ],
                type='blood',
            )
        ],
    ),
]


class TestCreateTestSubset(DbIsolatedTest):
    """Test class for creating a test subset"""
    project = 'test'

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.pttable = ProjectPermissionsTable(self.connection.connection, True)
        self.project = TestCreateTestSubset.project

        pl = ParticipantLayer(self.connection)
        await pl.upsert_participants(all_participants, open_transaction=False)

    @run_as_sync
    @unittest.mock.patch('scripts.create_test_subset.query')
    async def test_create_test_subset(self, mock_query: unittest.mock.Mock):
        """Test creating a test subset"""
        tsg = TestSubsetGenerator(
            project='test',
            samples_n=2,
            families_n=2,
            additional_families=set(),
            additional_samples=set(),
            skip_ped=True,
        )

        mock_all_sample_ids_query_result = {
            'project': {
                'dataset': 'test',
                'samples': [
                    {
                        'id': 'XPG1',
                        'externalId': 'sample_id001',
                        'sequencingGroups': [
                            {
                                'id': 'XPG123',
                            }
                        ],
                    },
                    {
                        'id': 'XPG2',
                        'externalId': 'sample_id002',
                        'sequencingGroups': [
                            {
                                'id': 'XPG456',
                            }
                        ],
                    },
                    {
                        'id': 'XPG3',
                        'externalId': 'sample_id003',
                        'sequencingGroups': [
                            {
                                'id': 'XPG789',
                            }
                        ],
                    },
                    {
                        'id': 'XPG4',
                        'externalId': 'sample_id004',
                        'sequencingGroups': [
                            {
                                'id': 'XPG101112',
                            }
                        ],
                    },
                ],
            }
        }

        mock_query.side_effect = [
            mock_all_sample_ids_query_result,
        ]

        all_sids = tsg.find_all_sample_ids_in_project(tsg.project)
        self.assertSetEqual(all_sids, {'XPG1', 'XPG2', 'XPG3', 'XPG4'})

        tsg.randomly_select_from_remaining_samples(all_sids, tsg.additional_samples, tsg.samples_n)
        self.assertSetEqual(tsg.additional_samples, {'XPG1', 'XPG4'})

    @run_as_sync
    @unittest.mock.patch('scripts.create_test_subset.query')
    async def test_get_original_project_subset_data_samples(self, mock_query: unittest.mock.Mock):
        """Test getting the original project subset data samples and participants"""
        tsg = TestSubsetGenerator(
            project='test',
            samples_n=2,
            families_n=2,
            additional_families=set(),
            additional_samples={'XPG1', 'XPG4'},
            skip_ped=True,
        )

        mock_original_project_subset_samples_query_result = {
            'project': {
                'samples': [
                    {
                        'id': 'XPG1',
                        'externalId': 'sample_id001',
                        'participant': {
                            'externalId': 'Demeter',
                            'id': 1,
                            'meta': {},
                            'reportedGender': None,
                            'reportedSex': None,
                            'karyotype': None,
                        },
                        'sequencingGroups': [
                            {
                                'id': 'XPG123',
                                'meta': {},
                                'platform': 'illumina',
                                'technology': 'short-read',
                                'type': 'genome',
                                'assays': [
                                    {
                                        'id': 'XPG123',
                                        'meta': {
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
                                            **default_assay_meta,
                                        },
                                        'type': 'sequencing',
                                    }
                                ],
                                'analyses': [],
                            },
                        ],
                    },
                    {
                        'id': 'XPG4',
                        'externalId': 'sample_id004',
                        'participant': {
                            'externalId': 'Apollo',
                            'id': 4,
                            'meta': {},
                            'reportedGender': None,
                            'reportedSex': None,
                            'karyotype': None,
                        },
                        'sequencingGroups': [
                            {
                                'id': 'XPG101112',
                                'meta': {},
                                'platform': 'illumina',
                                'technology': 'short-read',
                                'type': 'genome',
                                'assays': [
                                    {
                                        'id': 'XPG101112',
                                        'meta': {
                                            'reads': [
                                                {
                                                    'basename': 'sample_id004.filename-R1.fastq.gz',
                                                    'checksum': None,
                                                    'class': 'File',
                                                    'location': '/path/to/sample_id004.filename-R1.fastq.gz',
                                                    'size': 111,
                                                },
                                                {
                                                    'basename': 'sample_id004.filename-R2.fastq.gz',
                                                    'checksum': None,
                                                    'class': 'File',
                                                    'location': '/path/to/sample_id004.filename-R2.fastq.gz',
                                                    'size': 111,
                                                },
                                            ],
                                            'reads_type': 'fastq',
                                            **default_assay_meta,
                                        },
                                        'type': 'sequencing',
                                    }
                                ],
                                'analyses': [],
                            }
                        ],
                    },
                ],
            }
        }
        mock_query.side_effect = [
            mock_original_project_subset_samples_query_result,
        ]

        samples = tsg.get_original_project_subset_data_samples(tsg.project, tsg.additional_samples)
        self.assertListEqual(samples, mock_original_project_subset_samples_query_result['project']['samples'])

        expected_participants = [
            {
                'externalId': 'Demeter',
                'id': 1,
                'karyotype' : None,
                'meta': {},
                'reportedGender': None,
                'reportedSex': None,
            },
            {
                'externalId': 'Apollo',
                'id': 4,
                'karyotype' : None,
                'meta': {},
                'reportedGender': None,
                'reportedSex': None,
            }
        ]
        participant_data = tsg.get_participant_data(samples)
        self.assertListEqual(participant_data, expected_participants)

        participant_internal_ids = tsg.get_participant_ids_from_participant_data(participant_data)
        self.assertListEqual(participant_internal_ids, [1, 4])

    @run_as_sync
    async def test_transfer_participants(self):
        """Test transferring participants to the test project"""
        tsg = TestSubsetGenerator(
            project='test',
            samples_n=2,
            families_n=2,
            additional_families=set(),
            additional_samples={'XPG1', 'XPG4'},
            skip_ped=True,
        )
        target_project = tsg.get_target_project_name(tsg.project)

        test_pid = await self.pttable.create_project(target_project, target_project, 'testuser')

        participant_data = [
            {
                'externalId': 'Demeter',
                'id': 1,
                'karyotype' : None,
                'meta': {},
                'reportedGender': None,
                'reportedSex': None,
            },
            {
                'externalId': 'Apollo',
                'id': 4,
                'karyotype' : None,
                'meta': {},
                'reportedGender': None,
                'reportedSex': None,
            }
        ]
        tsg.transfer_participants(
            target_project=target_project,
            participant_data=participant_data,
        )

        pl = ParticipantLayer(self.connection)
        print(await pl.get_participants(project=test_pid))
