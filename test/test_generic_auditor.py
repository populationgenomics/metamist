from collections import namedtuple
import unittest
from unittest.mock import MagicMock, patch
from metamist.audit.generic_auditor import GenericAuditor

# pylint: disable=dangerous-default-value
# noqa: B006


class TestGenericAuditor(unittest.TestCase):
    """Test the audit helper functions"""

    @patch('metamist.audit.generic_auditor.query')
    def test_get_participant_data_for_dataset(self, mock_query):
        """Only participants with a non-empty samples field should be returned"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        mock_query.return_value = {
            'project': {
                'dataset': 'test',
                'participants': [
                    {
                        'id': 1,
                        'externalId': 'P01',
                        'samples': [
                            {
                                'id': 'XPG123',
                                'sequencingGroups': [
                                    {'id': 'CPG123', 'assays': [{'id': 1}]}
                                ],
                            }
                        ],
                    },
                    {
                        'id': 2,
                        'externalId': 'P02',
                        'samples': [
                            {
                                'id': 'XPG456',
                                'sequencingGroups': [
                                    {'id': 'CPG456', 'assays': [{'id': 2}, {'id': 3}]}
                                ],
                            }
                        ],
                    },
                    {'id': 3, 'externalId': 'P01', 'samples': []},
                ],
            }
        }

        expected_participants = [
            {
                'id': 1,
                'externalId': 'P01',
                'samples': [
                    {
                        'id': 'XPG123',
                        'sequencingGroups': [{'id': 'CPG123', 'assays': [{'id': 1}]}],
                    }
                ],
            },
            {
                'id': 2,
                'externalId': 'P02',
                'samples': [
                    {
                        'id': 'XPG456',
                        'sequencingGroups': [
                            {'id': 'CPG456', 'assays': [{'id': 2}, {'id': 3}]}
                        ],
                    }
                ],
            },
        ]

        participant_data = auditor.get_participant_data_for_dataset()

        self.assertListEqual(expected_participants, participant_data)

    def test_get_assay_map_from_participants_genome(self):
        """Only genome sequences should be mapped to the sample ID"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        participants = [
            {
                'externalId': 'EX01',
                'id': 1,
                'samples': [
                    {
                        'id': 'XPG123',
                        'externalId': 'EX01',
                        'sequencingGroups': [
                            {
                                'id': 'CPG123',
                                'type': 'genome',
                                'assays': [
                                    {
                                        'id': 1,
                                        'meta': {
                                            'reads': [
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/read.fq',
                                                    'size': 11,
                                                }
                                            ]
                                        },
                                    },
                                    {
                                        'id': 2,
                                        'meta': {
                                            'reads': [
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/R1.fq',
                                                    'size': 12,
                                                },
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/R2.fq',
                                                    'size': 13,
                                                },
                                            ]
                                        },
                                    },
                                    {
                                        'id': 3,
                                        'meta': {
                                            'reads': [{'location': 'test', 'size': 0}]
                                        },
                                    },
                                ],
                            },
                            {
                                'id': 'CPG456',
                                'type': 'exome',
                                'assays': [
                                    {
                                        'id': 4,
                                        'meta': {
                                            'reads': [
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/exome/read.fq',
                                                    'size': 14,
                                                }
                                            ]
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ]

        (
            sg_sample_id_map,
            assay_sg_id_map,
            assay_paths_sizes,
        ) = auditor.get_assay_map_from_participants(participants)

        expected_sg_sample_mapping = {'CPG123': 'XPG123'}
        expected_assay_sg_mapping = {1: 'CPG123', 2: 'CPG123', 3: 'CPG123'}
        expected_read_sizes = {
            1: [
                ('gs://cpg-dataset-main-upload/read.fq', 11),
            ],
            2: [
                ('gs://cpg-dataset-main-upload/R1.fq', 12),
                ('gs://cpg-dataset-main-upload/R2.fq', 13),
            ],
            3: [
                ('test', 0),
            ],
        }

        self.assertDictEqual(sg_sample_id_map, expected_sg_sample_mapping)
        self.assertDictEqual(assay_sg_id_map, expected_assay_sg_mapping)
        self.assertDictEqual(assay_paths_sizes, expected_read_sizes)

    def test_get_assay_map_from_participants_all(self):
        """Both genome and exome sequences should be mapped to the sample IDs"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome', 'exome'], file_types=('fastq',)
        )
        participants = [
            {
                'externalId': 'EX01',
                'id': 1,
                'samples': [
                    {
                        'id': 'XPG123',
                        'externalId': 'EX01',
                        'sequencingGroups': [
                            {
                                'id': 'CPG123',
                                'type': 'genome',
                                'assays': [
                                    {
                                        'id': 1,
                                        'meta': {
                                            'reads': [
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/read.fq',
                                                    'size': 11,
                                                }
                                            ]
                                        },
                                    },
                                    {
                                        'id': 2,
                                        'meta': {
                                            'reads': [
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/R1.fq',
                                                    'size': 12,
                                                },
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/R2.fq',
                                                    'size': 13,
                                                },
                                            ]
                                        },
                                    },
                                    {
                                        'id': 3,
                                        'meta': {
                                            'reads': [{'location': 'test', 'size': 0}]
                                        },
                                    },
                                ],
                            },
                            {
                                'id': 'CPG456',
                                'type': 'exome',
                                'assays': [
                                    {
                                        'id': 4,
                                        'meta': {
                                            'reads': [
                                                {
                                                    'location': 'gs://cpg-dataset-main-upload/exome/read.fq',
                                                    'size': 14,
                                                }
                                            ]
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ]
        (
            sg_sample_id_map,
            assay_sg_id_map,
            assay_paths_sizes,
        ) = auditor.get_assay_map_from_participants(participants)

        expected_sg_sample_mapping = {'CPG123': 'XPG123', 'CPG456': 'XPG123'}
        expected_assay_sg_mapping = {1: 'CPG123', 2: 'CPG123', 3: 'CPG123', 4: 'CPG456'}
        expected_read_sizes = {
            1: [
                ('gs://cpg-dataset-main-upload/read.fq', 11),
            ],
            2: [
                ('gs://cpg-dataset-main-upload/R1.fq', 12),
                ('gs://cpg-dataset-main-upload/R2.fq', 13),
            ],
            3: [
                ('test', 0),
            ],
            4: [
                ('gs://cpg-dataset-main-upload/exome/read.fq', 14),
            ],
        }

        self.assertDictEqual(sg_sample_id_map, expected_sg_sample_mapping)
        self.assertDictEqual(assay_sg_id_map, expected_assay_sg_mapping)
        self.assertDictEqual(assay_paths_sizes, expected_read_sizes)

    def test_get_sequence_mapping_error_logging(self):
        """If the sequence reads meta field maps to a raw string, logging.error triggers"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        participants = [
            {
                'externalId': 'EX01',
                'id': 1,
                'samples': [
                    {
                        'id': 'XPG123',
                        'externalId': 'EX01',
                        'sequencingGroups': [
                            {
                                'id': 'CPG123',
                                'type': 'genome',
                                'assays': [
                                    {
                                        'id': 1,
                                        'meta': {
                                            'reads': [
                                                'gs://cpg-dataset-main-upload/read.fq',
                                            ]
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ]
        with self.assertLogs(level='ERROR') as log:
            _, _, _ = auditor.get_assay_map_from_participants(participants)
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn(
                "ERROR:root:dev :: Got <class 'str'> read for SG CPG123, expected dict: gs://cpg-dataset-main-upload/read.fq",
                log.output[0],
            )

    def test_get_sequence_mapping_warning_logging(self):
        """If the sequence reads meta field is missing, logging.warning triggers"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        participants = [
            {
                'externalId': 'EX01',
                'id': 1,
                'samples': [
                    {
                        'id': 'XPG123',
                        'externalId': 'EX01',
                        'sequencingGroups': [
                            {
                                'id': 'CPG123',
                                'type': 'genome',
                                'assays': [
                                    {
                                        'id': 1,
                                        'meta': {},
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ]
        with self.assertLogs(level='WARNING') as log:
            _, _, _ = auditor.get_assay_map_from_participants(participants)
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn(
                'WARNING:root:dev :: SG CPG123 assay 1 has no reads field',
                log.output[0],
            )

    @patch('metamist.audit.generic_auditor.query')
    def test_query_genome_analyses_crams(self, mock_query):
        """Test that only the genome analysis crams for a sample map dictionary are returned"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        mock_query.side_effect = [
            {
                'sequencingGroups': [
                    {
                        'id': 'CPG123',
                        'type': 'genome',
                        'analyses': [
                            {
                                'id': 1,
                                'meta': {
                                    'sequencing_type': 'genome',
                                    'sample_ids': [
                                        'CPG123',
                                    ],
                                },
                                'output': 'gs://cpg-dataset-main/cram/CPG123.cram',
                            },
                        ],
                    },
                    {
                        'id': 'CPG456',
                        'type': 'exome',
                        'analyses': [
                            {
                                'id': 2,
                                'meta': {
                                    'sequencing_type': 'exome',
                                    'sample_ids': [
                                        'CPG456',
                                    ],
                                },
                                'output': 'gs://cpg-dataset-main/exome/cram/CPG123.cram',
                            },
                        ],
                    },
                ]
            }
        ]

        test_result = auditor.get_analysis_cram_paths_for_dataset_sgs(
            assay_sg_id_map={1: 'CPG123'}
        )
        expected_result = {'CPG123': {1: 'gs://cpg-dataset-main/cram/CPG123.cram'}}

        self.assertDictEqual(test_result, expected_result)

    @patch('metamist.audit.generic_auditor.query')
    def test_query_genome_and_exome_analyses_crams(self, mock_query):
        """Test that both the genome and exome analysis crams for a sample map dictionary are returned"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome', 'exome'], file_types=('fastq',)
        )
        mock_query.side_effect = [
            {
                'sequencingGroups': [
                    {
                        'id': 'CPG123',
                        'type': 'genome',
                        'analyses': [
                            {
                                'id': 1,
                                'meta': {
                                    'sequencing_type': 'genome',
                                    'sample_ids': [
                                        'CPG123',
                                    ],
                                },
                                'output': 'gs://cpg-dataset-main/cram/CPG123.cram',
                            },
                        ],
                    }
                ]
            },
            {
                'sequencingGroups': [
                    {
                        'id': 'CPG456',
                        'type': 'exome',
                        'analyses': [
                            {
                                'id': 2,
                                'meta': {
                                    'sequencing_type': 'exome',
                                    'sample_ids': [
                                        'CPG456',
                                    ],
                                },
                                'output': 'gs://cpg-dataset-main/exome/cram/CPG123.cram',
                            },
                        ],
                    },
                ]
            },
        ]

        test_result = auditor.get_analysis_cram_paths_for_dataset_sgs(
            assay_sg_id_map={1: 'CPG123', 2: 'CPG456'}
        )

        expected_result = {
            'CPG123': {1: 'gs://cpg-dataset-main/cram/CPG123.cram'},
            'CPG456': {2: 'gs://cpg-dataset-main/exome/cram/CPG123.cram'},
        }

        self.assertDictEqual(test_result, expected_result)

    @patch('metamist.audit.generic_auditor.query')
    def test_query_broken_analyses_crams(self, mock_query):
        """
        All analysis crams must have 'sequencing_type' meta field,
        ValueError raised if not
        """
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        mock_query.return_value = {
            'sequencingGroups': [
                {
                    'id': 'CPG123',
                    'analyses': [
                        {
                            'id': 1,
                            'meta': {
                                'sequence_type': 'genome',
                            },
                            'sample_ids': [
                                'CPG123',
                            ],
                            'output': '',
                        },
                    ],
                },
            ]
        }

        with self.assertRaises(ValueError):
            auditor.get_analysis_cram_paths_for_dataset_sgs(
                assay_sg_id_map={1: 'CPG123'}
            )

    @patch('metamist.audit.generic_auditor.query')
    def test_query_analyses_crams_warning(self, mock_query):
        """Warn if the sample_ids field is absent and the sample meta field is used instead"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        mock_query.return_value = {
            'sequencingGroups': [
                {
                    'id': 'CPG123',
                    'analyses': [
                        {
                            'id': 1,
                            'meta': {
                                'sequencing_type': 'genome',
                                'sampling_id': 'CPG123',
                            },
                            'output': 'gs://cpg-dataset-main/cram/CPG123.cram',
                        },
                    ],
                }
            ]
        }

        with self.assertLogs(level='WARNING') as log:
            _ = auditor.get_analysis_cram_paths_for_dataset_sgs(
                assay_sg_id_map={1: 'CPG123'}
            )
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn(
                'WARNING:root:Analysis 1 missing sample or sequencing group field.',
                log.output[0],
            )

    @patch('metamist.audit.generic_auditor.query')
    def test_analyses_for_sgs_without_crams(self, mock_query):
        """Log any analyses found for samples without completed CRAMs"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        sgs_without_crams = [  # noqa: B006
            'CPG123',
        ]

        mock_query.return_value = {
            'sequencingGroups': [
                {
                    'id': 'CPG123',
                    'analyses': [
                        {
                            'id': 1,
                            'meta': {'sequencing_type': 'genome', 'sample': 'CPG123'},
                            'output': 'gs://cpg-dataset-main/gvcf/CPG123.g.vcf.gz',
                            'type': 'gvcf',
                            'timestampCompleted': '2023-05-11T16:33:00',
                        }
                    ],
                }
            ]
        }

        with self.assertLogs(level='WARNING') as log:
            _ = auditor.analyses_for_sgs_without_crams(sgs_without_crams)
            self.assertEqual(len(log.output), 8)  # 8 analysis types checked
            self.assertEqual(len(log.records), 8)
            self.assertIn(
                "WARNING:root:dev :: SG CPG123 missing CRAM but has analysis {'analysis_id': 1, 'analysis_type': 'gvcf', 'analysis_output': 'gs://cpg-dataset-main/gvcf/CPG123.g.vcf.gz', 'timestamp_completed': '2023-05-11T16:33:00'}",
                log.output[0],
            )

    def test_get_complete_and_incomplete_sgs(self):
        """Report on samples that have completed CRAMs and those that dont"""
        assay_sg_id_map = {  # noqa: B006
            1: 'CPG123',
            2: 'CPG456',
            3: 'CPG789',
        }
        sg_cram_paths = {  # noqa: B006
            'CPG123': {1: 'gs://cpg-dataset-main/cram/CPG123.cram'},
            'CPG456': {2: 'gs://cpg-dataset-main/exome/cram/CPG456.cram'},
        }
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome', 'exome'], file_types=('fastq',)
        )
        auditor.get_gcs_bucket_subdirs_to_search = MagicMock()
        auditor.find_files_in_gcs_buckets_subdirs = MagicMock()
        auditor.analyses_for_sgs_without_crams = MagicMock()

        auditor.get_gcs_bucket_subdirs_to_search.return_value = {
            'cpg-dataset-main': ['cram', 'exome/cram']
        }
        auditor.find_files_in_gcs_buckets_subdirs.return_value = [
            'gs://cpg-dataset-main/cram/CPG123.cram',
            'gs://cpg-dataset-main/exome/cram/CPG456.cram',
        ]

        result = auditor.get_complete_and_incomplete_sgs(
            assay_sg_id_map=assay_sg_id_map,
            sg_cram_paths=sg_cram_paths,
        )

        expected_result = {
            'complete': {'CPG123': [1], 'CPG456': [2]},
            'incomplete': ['CPG789'],
        }

        self.assertDictEqual(result, expected_result)

    async def test_check_for_uningested_or_moved_assays(self):
        """Test 2 ingested reads, one ingested and moved read, and one uningested read"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        assay_reads_sizes = {  # noqa: B006
            1: [('read1.fq', 10), ('read2.fq', 11), ('dir1/read3.fq', 12)]
        }
        completed_sgs = {'CPG123': [1]}
        sg_sample_id_map = {'CPG123': 'EXT123'}
        assay_sg_id_map = {1: 'CPG123'}
        sample_internal_external_id_map = {'CPG123': 'EXT123'}
        auditor.find_sequence_files_in_gcs_bucket = MagicMock()
        auditor.find_sequence_files_in_gcs_bucket.return_value = [
            'read1.fq',
            'read2.fq',
            'dir2/read3.fq',
            'read4.fq',
        ]

        auditor.file_size = MagicMock()
        auditor.file_size.return_value = 12

        (
            uningested_sequence_paths,
            sequences_moved_paths,
            _,
        ) = await auditor.check_for_uningested_or_moved_assays(
            bucket_name='gs://cpg-test-upload',
            assay_filepaths_filesizes=assay_reads_sizes,
            completed_sgs=completed_sgs,
            sg_sample_id_map=sg_sample_id_map,
            assay_sg_id_map=assay_sg_id_map,
            sample_internal_external_id_map=sample_internal_external_id_map,
        )

        AssayReportEntry = namedtuple(
            'AssayReportEntry',
            'sg_id assay_id assay_file_path analysis_ids filesize',
        )
        self.assertEqual(uningested_sequence_paths, {'read4.fq'})
        self.assertEqual(
            sequences_moved_paths,
            [
                AssayReportEntry(
                    sg_id='CPG123',
                    assay_id=1,
                    assay_file_path='dir2/read3.fq',
                    analysis_ids=[1],
                    filesize=12,
                ),
            ],
        )

    def test_get_gcs_bucket_subdirs_to_search(self):
        """
        Takes a list of paths and extracts the bucket name and subdirectory, returning all unique pairs
        of buckets/subdirectories
        """
        paths = [
            'gs://cpg-dataset-main-upload/transfer/data.cram',
            'gs://cpg-dataset-main-upload/transfer/transfer2/data2.cram',
            'gs://cpg-dataset-main-upload/transfer/transfer2/transfer3/data3.cram',
            'gs://cpg-dataset-main-upload/transfer4/data4.cram',
            'gs://cpg-dataset-main-upload/data5.cram',
        ]
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        buckets_subdirs = auditor.get_gcs_bucket_subdirs_to_search(paths)

        expected_result = {
            'cpg-dataset-main-upload': [
                'transfer/',
                'transfer/transfer2/',
                'transfer/transfer2/transfer3/',
                'transfer4/',
            ]
        }

        self.assertDictEqual(buckets_subdirs, expected_result)
