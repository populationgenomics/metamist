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
                        'samples': [{'id': 'CPG123', 'sequences': [{'id': 1}]}],
                    },
                    {
                        'id': 2,
                        'samples': [
                            {'id': 'CPG456', 'sequences': [{'id': 2}, {'id': 3}]}
                        ],
                    },
                    {'id': 3, 'samples': []},
                ],
            }
        }

        expected_participants = [
            {
                'id': 1,
                'samples': [{'id': 'CPG123', 'sequences': [{'id': 1}]}],
            },
            {
                'id': 2,
                'samples': [{'id': 'CPG456', 'sequences': [{'id': 2}, {'id': 3}]}],
            },
        ]

        participant_data = auditor.get_participant_data_for_dataset()

        self.assertListEqual(expected_participants, participant_data)

    def test_get_genome_sequence_mapping(self):
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
                        'id': 'CPG123',
                        'externalId': 'EX01',
                        'sequences': [
                            {
                                'id': 1,
                                'type': 'genome',
                                'meta': {
                                    'reads': {
                                        'location': 'gs://cpg-dataset-main-upload/read.fq',
                                        'size': 12,
                                    }
                                },
                            },
                            {
                                'id': 2,
                                'type': 'genome',
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
                                'type': 'exome',
                                'meta': {'reads': {'location': 'test', 'size': 0}},
                            },
                        ],
                    },
                ],
            },
        ]
        seq_sample_id_map, seq_reads_sizes = auditor.get_sequence_map_from_participants(
            participants
        )

        expected_mapping = {1: 'CPG123', 2: 'CPG123'}
        expected_read_sizes = {
            1: [
                ('gs://cpg-dataset-main-upload/read.fq', 12),
            ],
            2: [
                ('gs://cpg-dataset-main-upload/R1.fq', 12),
                ('gs://cpg-dataset-main-upload/R2.fq', 13),
            ],
        }

        self.assertDictEqual(seq_sample_id_map, expected_mapping)
        self.assertDictEqual(seq_reads_sizes, expected_read_sizes)

    # def test_get_all_sequence_mapping_list_reads(self):
    #     """Both genome and exome sequences should be mapped to the sample IDs"""
    #     auditor = GenericAuditor(
    #         dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
    #     )
    #     participants = [
    #         {
    #             'externalId': 'EX01',
    #             'id': 1,
    #             'samples': [
    #                 {
    #                     'id': 'CPG123',
    #                     'externalId': 'EX01',
    #                     'sequences': [
    #                         {
    #                             'id': 1,
    #                             'type': 'genome',
    #                             'meta': {
    #                                 'reads': [
    #                                     {
    #                                         'location': 'gs://cpg-dataset-main-upload/R1.fq',
    #                                         'size': 12,
    #                                     },
    #                                     {
    #                                         'location': 'gs://cpg-dataset-main-upload/R2.fq',
    #                                         'size': 13,
    #                                     },
    #                                 ]
    #                             },
    #                         },
    #                     ],
    #                 },
    #             ],
    #         },
    #         {
    #             'externalId': 'EX02',
    #             'id': 2,
    #             'samples': [
    #                 {
    #                     'id': 'CPG456',
    #                     'externalId': 'EX02',
    #                     'sequences': [
    #                         {
    #                             'id': 2,
    #                             'type': 'exome',
    #                             'meta': {
    #                                 'reads': [
    #                                     {
    #                                         'location': 'gs://cpg-dataset-main-upload/exomes/R1.fq',
    #                                         'size': 14,
    #                                     },
    #                                     {
    #                                         'location': 'gs://cpg-dataset-main-upload/exomes/R2.fq',
    #                                         'size': 15,
    #                                     },
    #                                 ]
    #                             },
    #                         },
    #                     ],
    #                 },
    #             ],
    #         },
    #     ]
    #     seq_sample_id_map, seq_reads_sizes = auditor.get_sequence_map_from_participants(
    #         participants
    #     )
    #     expected_mapping = {1: 'CPG123', 2: 'CPG456'}
    #     expected_read_sizes = {
    #         1: [
    #             ('gs://cpg-dataset-main-upload/R1.fq', 12),
    #             ('gs://cpg-dataset-main-upload/R2.fq', 13),
    #         ],
    #         2: [
    #             ('gs://cpg-dataset-main-upload/exomes/R1.fq', 14),
    #             ('gs://cpg-dataset-main-upload/exomes/R2.fq', 15),
    #         ],
    #     }
    #
    #     self.assertDictEqual(seq_sample_id_map, expected_mapping)
    #     self.assertDictEqual(seq_reads_sizes, expected_read_sizes)

    def test_get_sequence_mapping_logging(self):
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
                        'id': 'CPG123',
                        'externalId': 'EX01',
                        'sequences': [
                            {
                                'id': 1,
                                'type': 'genome',
                                'meta': {
                                    'reads': 'gs://cpg-dataset-main-upload/read.cram'
                                },
                            },
                        ],
                    },
                ],
            },
        ]
        with self.assertLogs(level='ERROR') as log:
            _, _ = auditor.get_sequence_map_from_participants(participants)
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn(
                "Invalid read type: <class 'str'>: gs://cpg-dataset-main-upload/read.cram",
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
                'sample': {
                    'id': 'CPG123',
                    'analyses': [
                        {
                            'id': 1,
                            'meta': {
                                'sequencing_type': 'genome',
                            },
                            'sample_ids': [
                                'CPG123',
                            ],
                            'output': 'gs://cpg-dataset-main/cram/CPG123.cram',
                        },
                    ],
                },
            },
            {
                'sample': {
                    'id': 'CPG456',
                    'analyses': [
                        {
                            'id': 2,
                            'meta': {
                                'sequencing_type': 'exome',
                            },
                            'sample_ids': [
                                'CPG456',
                            ],
                            'output': 'gs://cpg-dataset-main/exome/cram/CPG123.cram',
                        },
                    ],
                },
            },
        ]

        test_result = auditor.get_analysis_cram_paths_for_dataset_samples(
            sample_internal_to_external_id_map={'CPG123': 'EXT123'}
        )

        expected_result = {'CPG123': {1: 'gs://cpg-dataset-main/cram/CPG123.cram'}}

        self.assertDictEqual(test_result, expected_result)

    # @patch('metamist.audit.generic_auditor.query')
    # def test_query_genome_and_exome_analyses_crams(self, mock_query):
    #     """Test that both the genome and exome analysis crams for a sample map dictionary are returned"""
    #     auditor = GenericAuditor(
    #         dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
    #     )
    #     mock_query.side_effect = [
    #         {
    #             'sample': {
    #                 'id': 'CPG123',
    #                 'analyses': [
    #                     {
    #                         'id': 1,
    #                         'meta': {
    #                             'sequencing_type': 'genome',
    #                         },
    #                         'sample_ids': [
    #                             'CPG123',
    #                         ],
    #                         'output': 'gs://cpg-dataset-main/cram/CPG123.cram',
    #                     },
    #                 ],
    #             },
    #         },
    #         {
    #             'sample': {
    #                 'id': 'CPG456',
    #                 'analyses': [
    #                     {
    #                         'id': 2,
    #                         'meta': {
    #                             'sequencing_type': 'exome',
    #                         },
    #                         'sample_ids': [
    #                             'CPG456',
    #                         ],
    #                         'output': 'gs://cpg-dataset-main/exome/cram/CPG123.cram',
    #                     },
    #                 ],
    #             },
    #         },
    #     ]
    #
    #     test_result = auditor.get_analysis_cram_paths_for_dataset_samples(
    #         sample_internal_to_external_id_map={'CPG123': 'EXT123', 'CPG456': 'EXT456'}
    #     )
    #
    #     expected_result = {
    #         'CPG123': {1: 'gs://cpg-dataset-main/cram/CPG123.cram'},
    #         'CPG456': {2: 'gs://cpg-dataset-main/exome/cram/CPG123.cram'},
    #     }
    #
    #     self.assertDictEqual(test_result, expected_result)

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
            'sample': {
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
        }

        with self.assertRaises(ValueError):
            auditor.get_analysis_cram_paths_for_dataset_samples(
                sample_internal_to_external_id_map={'CPG123': 'EXT123'}
            )

    @patch('metamist.audit.generic_auditor.query')
    def test_query_analyses_crams_warning(self, mock_query):
        """Warn if the sample_ids field is absent and the sample meta field is used instead"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        mock_query.return_value = {
            'sample': {
                'id': 'CPG123',
                'analyses': [
                    {
                        'id': 1,
                        'meta': {
                            'sequencing_type': 'genome',
                            'sample': 'CPG123',
                        },
                        'output': 'gs://cpg-dataset-main/cram/CPG123.cram',
                    },
                ],
            }
        }

        with self.assertLogs(level='WARNING') as log:
            test_result = auditor.get_analysis_cram_paths_for_dataset_samples(
                sample_internal_to_external_id_map={'CPG123': 'EXT123'}
            )
            self.assertEqual(len(log.output), 1)
            self.assertEqual(len(log.records), 1)
            self.assertIn(
                'Analysis: 1 missing "sample_ids" field. Using analysis["meta"].get("sample") instead.',
                log.output[0],
            )

        expected_result = {'CPG123': {1: 'gs://cpg-dataset-main/cram/CPG123.cram'}}

        self.assertDictEqual(test_result, expected_result)

    @patch('metamist.audit.generic_auditor.query')
    def test_analyses_for_samples_without_crams(self, mock_query):
        """Log any analyses found for samples without completed CRAMs"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        samples_without_crams = [  # noqa: B006
            'CPG123',
        ]

        mock_query.return_value = {
            'sample': {
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
        }

        with self.assertLogs(level='WARNING') as log:
            _ = auditor.analyses_for_samples_without_crams(samples_without_crams)
            self.assertEqual(len(log.output), 8)  # 8 analysis types checked
            self.assertEqual(len(log.records), 8)
            self.assertIn(
                "WARNING:root:CPG123 missing CRAM but has analysis {'analysis_id': 1, 'analysis_type': 'gvcf', 'analysis_output': 'gs://cpg-dataset-main/gvcf/CPG123.g.vcf.gz', 'timestamp_completed': '2023-05-11T16:33:00'}",
                log.output[6],
            )

    def test_get_complete_and_incomplete_samples(self):
        """Report on samples that have completed CRAMs and those that dont"""
        sample_map = {  # noqa: B006
            'CPG123': 'EXT123',
            'CPG456': 'EXT456',
            'CPG789': 'EXT789',
        }
        sample_cram_paths = {  # noqa: B006
            'CPG123': {1: 'gs://cpg-dataset-main/cram/CPG123.cram'},
            'CPG456': {2: 'gs://cpg-dataset-main/exome/cram/CPG456.cram'},
        }
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        auditor.get_gcs_bucket_subdirs_to_search = MagicMock()
        auditor.find_files_in_gcs_buckets_subdirs = MagicMock()
        auditor.analyses_for_samples_without_crams = MagicMock()

        auditor.get_gcs_bucket_subdirs_to_search.return_value = {
            'cpg-dataset-main': ['cram', 'exome/cram']
        }
        auditor.find_files_in_gcs_buckets_subdirs.return_value = [
            'gs://cpg-dataset-main/cram/CPG123.cram',
            'gs://cpg-dataset-main/exome/cram/CPG456.cram',
        ]

        result = auditor.get_complete_and_incomplete_samples(
            sample_internal_to_external_id_map=sample_map,
            sample_cram_paths=sample_cram_paths,
        )

        expected_result = {
            'complete': {'CPG123': [1], 'CPG456': [2]},
            'incomplete': ['CPG789'],
        }

        self.assertDictEqual(result, expected_result)

    async def test_check_for_uningested_or_moved_sequences(self):
        """Test 2 ingested reads, one ingested and moved read, and one uningested read"""
        auditor = GenericAuditor(
            dataset='dev', sequencing_type=['genome'], file_types=('fastq',)
        )
        seq_reads_sizes = {  # noqa: B006
            1: [('read1.fq', 10), ('read2.fq', 11), ('dir1/read3.fq', 12)]
        }
        completed_samples = {'CPG123': [1]}
        seq_sample_map = {1: 'CPG123'}
        sample_id_internal_external_map = {'CPG123': 'EXT123'}
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
        ) = await auditor.check_for_uningested_or_moved_sequences(
            bucket_name='gs://cpg-test-upload',
            sequence_filepaths_filesizes=seq_reads_sizes,
            completed_samples=completed_samples,
            seq_id_sample_id_map=seq_sample_map,
            sample_id_internal_external_map=sample_id_internal_external_map,
        )

        SequenceReportEntry = namedtuple(
            'sequence_report_entry',
            'sample_id sequence_id sequence_file_path analysis_ids',
        )
        self.assertEqual(uningested_sequence_paths, {'read4.fq'})
        self.assertEqual(
            sequences_moved_paths,
            [
                SequenceReportEntry(
                    sample_id='CPG123',
                    sequence_id=1,
                    sequence_file_path='dir2/read3.fq',
                    analysis_ids=[1],
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
