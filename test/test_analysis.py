# pylint: disable=invalid-overridden-method
import logging
import os
from test.testbase import DbIsolatedTest, run_as_sync

import requests
from testcontainers.core.container import DockerContainer

from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter
from db.python.utils import GenericFilter
from models.enums import AnalysisStatus
from models.models import (
    AnalysisInternal,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)


class TestAnalysis(DbIsolatedTest):
    """Test sample class"""

    gcs: DockerContainer

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # Convert the relative path to an absolute path
        absolute_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        gcs = DockerContainer('fsouza/fake-gcs-server').with_bind_ports(4443, 4443).with_volume_mapping(absolute_path, '/data',).with_command('-scheme http')
        gcs.start()
        cls.gcs = gcs

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.sl = SampleLayer(self.connection)
        self.sgl = SequencingGroupLayer(self.connection)
        self.asl = AssayLayer(self.connection)
        self.al = AnalysisLayer(self.connection)

        sample = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test01',
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
        self.genome_sequencing_group_id = sample.sequencing_groups[0].id
        self.exome_sequencing_group_id = sample.sequencing_groups[self.project_id].id

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.gcs:
            cls.gcs.stop()
        super().tearDownClass()

    @run_as_sync
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
        self.assertEqual(analysis_id, analysis.id)
        self.assertEqual('cram', analysis.type)
        self.assertEqual(AnalysisStatus.COMPLETED, analysis.status)

    @run_as_sync
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

        analyses = await self.connection.connection.fetch_all('SELECT * FROM analysis')
        analysis_sgs = await self.connection.connection.fetch_all(
            'SELECT * FROM analysis_sequencing_group'
        )

        self.assertEqual(1, len(analyses))
        self.assertEqual(analysis_id, analyses[0]['id'])
        self.assertEqual(1, analysis_sgs[0]['sequencing_group_id'])
        self.assertEqual(analyses[0]['id'], analysis_sgs[0]['analysis_id'])

    @run_as_sync
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
                output=None,
                outputs=None,
                timestamp_completed=None,
                project=1,
                meta={},
                active=True,
                author=None,
            ).copy(update={'output': [], 'outputs': []})
        ]

        self.assertEqual(analyses, expected)

    @run_as_sync
    async def test_analysis_output_str(self):
        """
        Test adding an analysis of type ANALYSIS_RUNNER
        with output file as a string via `output` field
        """

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                meta={},
                output='RANDOM_OUTPUT_STRING'
            )
        )

        analyses = await self.al.query(
            AnalysisFilter(
                id=GenericFilter(eq=a_id),
            )
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                output='RANDOM_OUTPUT_STRING',
                outputs='RANDOM_OUTPUT_STRING',
                timestamp_completed=None,
                project=1,
                meta={},
                active=True,
                author=None,
            )
        ]

        self.assertEqual(analyses, expected)

    @run_as_sync
    async def test_analysis_outputs_str(self):
        """
        Test adding an analysis of type ANALYSIS_RUNNER
        with output file as a string via `outputs` field
        """

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                meta={},
                outputs='RANDOM_OUTPUT_STRING'
            )
        )

        analyses = await self.al.query(
            AnalysisFilter(
                id=GenericFilter(eq=a_id),
            )
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                output='RANDOM_OUTPUT_STRING',
                outputs='RANDOM_OUTPUT_STRING',
                timestamp_completed=None,
                project=1,
                meta={},
                active=True,
                author=None,
            )
        ]

        self.assertEqual(analyses, expected)

    @run_as_sync
    async def test_analysis_output_files_json(self):
        """
        Test adding an analysis of type ANALYSIS_RUNNER
        with output files as a dict/json via `outputs` field
        """

        logger = logging.getLogger()
        logger.info(requests.get('http://0.0.0.0:4443/storage/v1/b/fakegcs/o', timeout=10).text)

        a_id = await self.al.create_analysis(
            AnalysisInternal(
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                meta={},
                outputs={
                    'qc_results': {
                        'basename': 'gs://fakegcs/file1.txt'
                        },
                    'cram': {
                        'basename': 'gs://fakegcs/file1.cram'
                    },
                    'qc': {
                        'cram': {
                            'basename': 'gs://fakegcs/file2.cram',
                            'secondaryFiles':
                                {
                                    'cram_ext': {
                                        'basename': 'gs://fakegcs/file2.cram.ext'
                                    },
                                    'cram_meta': {
                                        'basename': 'gs://fakegcs/file2.cram.meta'
                                    }
                                }
                        },
                        'aggregate': {
                            'cram': {
                                'basename': 'gs://fakegcs/file3.cram',
                                'secondaryFiles': {
                                        'cram_ext': {
                                            'basename': 'gs://fakegcs/file3.cram.ext'
                                        },
                                        'cram_meta': {
                                            'basename': 'gs://fakegcs/file3.cram.meta'
                                        }
                                    }
                            },
                            'qc': {
                                'basename': 'gs://fakegcs/file1.qc',
                                'secondaryFiles': {
                                    'qc_ext': {
                                        'basename': 'gs://fakegcs/file1.qc.ext'
                                    },
                                    'qc_meta': {
                                        'basename': 'gs://fakegcs/file1.qc.meta'
                                    }
                                }
                            },
                        }
                    }
                }
            )
        )

        analyses = await self.al.query(
            AnalysisFilter(
                id=GenericFilter(eq=a_id),
            )
        )
        expected = [
            AnalysisInternal(
                id=a_id,
                type='analysis-runner',
                status=AnalysisStatus.UNKNOWN,
                sequencing_group_ids=[],
                output={
                    'qc_results': {
                        'id': 1,
                        'path': 'gs://fakegcs/file1.txt',
                        'basename': 'file1.txt',
                        'dirname': 'gs://fakegcs',
                        'nameroot': 'file1',
                        'nameext': '.txt',
                        'file_checksum': 'DG+fhg==',
                        'size': 19,
                        'meta': None,
                        'valid': True,
                        'secondary_files': []
                    },
                    'cram': {
                        'id': 2,
                        'path': 'gs://fakegcs/file1.cram',
                        'basename': 'file1.cram',
                        'dirname': 'gs://fakegcs',
                        'nameroot': 'file1',
                        'nameext': '.cram',
                        'file_checksum': 'sl7SXw==',
                        'size': 20,
                        'meta': None,
                        'valid': True,
                        'secondary_files': []
                    },
                    'qc': {
                        'cram': {
                            'id': 3,
                            'path': 'gs://fakegcs/file2.cram',
                            'basename': 'file2.cram',
                            'dirname': 'gs://fakegcs',
                            'nameroot': 'file2',
                            'nameext': '.cram',
                            'file_checksum': 'sl7SXw==',
                            'size': 20,
                            'meta': None,
                            'valid': True,
                            'secondary_files': [
                                {
                                    'id': 4,
                                    'path': 'gs://fakegcs/file2.cram.ext',
                                    'basename': 'file2.cram.ext',
                                    'dirname': 'gs://fakegcs',
                                    'nameroot': 'file2.cram',
                                    'nameext': '.ext',
                                    'file_checksum': 'gb1EbA==',
                                    'size': 21,
                                    'meta': None,
                                    'valid': True,
                                    'secondary_files': None
                                },
                                {
                                    'id': 5,
                                    'path': 'gs://fakegcs/file2.cram.meta',
                                    'basename': 'file2.cram.meta',
                                    'dirname': 'gs://fakegcs',
                                    'nameroot': 'file2.cram',
                                    'nameext': '.meta',
                                    'file_checksum': 'af/YSw==',
                                    'size': 17,
                                    'meta': None,
                                    'valid': True,
                                    'secondary_files': None
                                }
                            ]
                        },
                        'aggregate': {
                            'cram': {
                                'id': 6,
                                'path': 'gs://fakegcs/file3.cram',
                                'basename': 'file3.cram',
                                'dirname': 'gs://fakegcs',
                                'nameroot': 'file3',
                                'nameext': '.cram',
                                'file_checksum': 'sl7SXw==',
                                'size': 20,
                                'meta': None,
                                'valid': True,
                                'secondary_files': [
                                    {
                                        'id': 7,
                                        'path': 'gs://fakegcs/file3.cram.ext',
                                        'basename': 'file3.cram.ext',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file3.cram',
                                        'nameext': '.ext',
                                        'file_checksum': 'HU8n6w==',
                                        'size': 16,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    },
                                    {
                                        'id': 8,
                                        'path': 'gs://fakegcs/file3.cram.meta',
                                        'basename': 'file3.cram.meta',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file3.cram',
                                        'nameext': '.meta',
                                        'file_checksum': 'af/YSw==',
                                        'size': 17,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    }
                                ]
                            },
                            'qc': {
                                'id': 9,
                                'path': 'gs://fakegcs/file1.qc',
                                'basename': 'file1.qc',
                                'dirname': 'gs://fakegcs',
                                'nameroot': 'file1',
                                'nameext': '.qc',
                                'file_checksum': 'uZe/hQ==',
                                'size': 15,
                                'meta': None,
                                'valid': True,
                                'secondary_files': [
                                    {
                                        'id': 10,
                                        'path': 'gs://fakegcs/file1.qc.ext',
                                        'basename': 'file1.qc.ext',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file1.qc',
                                        'nameext': '.ext',
                                        'file_checksum': '/18MDg==',
                                        'size': 14,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    },
                                    {
                                        'id': 11,
                                        'path': 'gs://fakegcs/file1.qc.meta',
                                        'basename': 'file1.qc.meta',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file1.qc',
                                        'nameext': '.meta',
                                        'file_checksum': 'v9x0Zg==',
                                        'size': 15,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    }
                                ]
                            }
                        }
                    }
                },
                outputs={
                    'qc_results': {
                        'id': 1,
                        'path': 'gs://fakegcs/file1.txt',
                        'basename': 'file1.txt',
                        'dirname': 'gs://fakegcs',
                        'nameroot': 'file1',
                        'nameext': '.txt',
                        'file_checksum': 'DG+fhg==',
                        'size': 19,
                        'meta': None,
                        'valid': True,
                        'secondary_files': []
                    },
                    'cram': {
                        'id': 2,
                        'path': 'gs://fakegcs/file1.cram',
                        'basename': 'file1.cram',
                        'dirname': 'gs://fakegcs',
                        'nameroot': 'file1',
                        'nameext': '.cram',
                        'file_checksum': 'sl7SXw==',
                        'size': 20,
                        'meta': None,
                        'valid': True,
                        'secondary_files': []
                    },
                    'qc': {
                        'cram': {
                            'id': 3,
                            'path': 'gs://fakegcs/file2.cram',
                            'basename': 'file2.cram',
                            'dirname': 'gs://fakegcs',
                            'nameroot': 'file2',
                            'nameext': '.cram',
                            'file_checksum': 'sl7SXw==',
                            'size': 20,
                            'meta': None,
                            'valid': True,
                            'secondary_files': [
                                {
                                    'id': 4,
                                    'path': 'gs://fakegcs/file2.cram.ext',
                                    'basename': 'file2.cram.ext',
                                    'dirname': 'gs://fakegcs',
                                    'nameroot': 'file2.cram',
                                    'nameext': '.ext',
                                    'file_checksum': 'gb1EbA==',
                                    'size': 21,
                                    'meta': None,
                                    'valid': True,
                                    'secondary_files': None
                                },
                                {
                                    'id': 5,
                                    'path': 'gs://fakegcs/file2.cram.meta',
                                    'basename': 'file2.cram.meta',
                                    'dirname': 'gs://fakegcs',
                                    'nameroot': 'file2.cram',
                                    'nameext': '.meta',
                                    'file_checksum': 'af/YSw==',
                                    'size': 17,
                                    'meta': None,
                                    'valid': True,
                                    'secondary_files': None
                                }
                            ]
                        },
                        'aggregate': {
                            'cram': {
                                'id': 6,
                                'path': 'gs://fakegcs/file3.cram',
                                'basename': 'file3.cram',
                                'dirname': 'gs://fakegcs',
                                'nameroot': 'file3',
                                'nameext': '.cram',
                                'file_checksum': 'sl7SXw==',
                                'size': 20,
                                'meta': None,
                                'valid': True,
                                'secondary_files': [
                                    {
                                        'id': 7,
                                        'path': 'gs://fakegcs/file3.cram.ext',
                                        'basename': 'file3.cram.ext',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file3.cram',
                                        'nameext': '.ext',
                                        'file_checksum': 'HU8n6w==',
                                        'size': 16,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    },
                                    {
                                        'id': 8,
                                        'path': 'gs://fakegcs/file3.cram.meta',
                                        'basename': 'file3.cram.meta',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file3.cram',
                                        'nameext': '.meta',
                                        'file_checksum': 'af/YSw==',
                                        'size': 17,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    }
                                ]
                            },
                            'qc': {
                                'id': 9,
                                'path': 'gs://fakegcs/file1.qc',
                                'basename': 'file1.qc',
                                'dirname': 'gs://fakegcs',
                                'nameroot': 'file1',
                                'nameext': '.qc',
                                'file_checksum': 'uZe/hQ==',
                                'size': 15,
                                'meta': None,
                                'valid': True,
                                'secondary_files': [
                                    {
                                        'id': 10,
                                        'path': 'gs://fakegcs/file1.qc.ext',
                                        'basename': 'file1.qc.ext',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file1.qc',
                                        'nameext': '.ext',
                                        'file_checksum': '/18MDg==',
                                        'size': 14,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    },
                                    {
                                        'id': 11,
                                        'path': 'gs://fakegcs/file1.qc.meta',
                                        'basename': 'file1.qc.meta',
                                        'dirname': 'gs://fakegcs',
                                        'nameroot': 'file1.qc',
                                        'nameext': '.meta',
                                        'file_checksum': 'v9x0Zg==',
                                        'size': 15,
                                        'meta': None,
                                        'valid': True,
                                        'secondary_files': None
                                    }
                                ]
                            }
                        }
                    }
                },
                timestamp_completed=None,
                project=1,
                meta={},
                active=True,
                author=None,
            )
        ]

        self.assertEqual(analyses, expected)
