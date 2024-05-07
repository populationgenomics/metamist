import unittest
from datetime import datetime
from io import StringIO
from test.testbase import DbIsolatedTest, run_as_sync
from unittest.mock import patch

import api.graphql.schema
from db.python.layers import ParticipantLayer
from metamist.graphql import configure_sync_client, validate
from metamist.parser.generic_metadata_parser import GenericMetadataParser
from metamist.parser.generic_parser import (
    QUERY_MATCH_ASSAYS,
    QUERY_MATCH_PARTICIPANTS,
    QUERY_MATCH_SAMPLES,
    QUERY_MATCH_SEQUENCING_GROUPS,
    ParsedParticipant,
    ParsedSample,
    ParsedSequencingGroup,
)
from models.models import (
    AssayUpsertInternal,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import sequencing_group_id_format


def _get_basic_participant_to_upsert():
    default_assay_meta = {
        'sequencing_type': 'genome',
        'sequencing_technology': 'short-read',
        'sequencing_platform': 'illumina',
    }

    return ParticipantUpsertInternal(
        external_id='Demeter',
        meta={},
        samples=[
            SampleUpsertInternal(
                external_id='sample_id001',
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


class TestValidateParserQueries(unittest.TestCase):
    """
    Validate queries used by the GenericParser
    """

    def test_queries(self):
        """
        We have the luxury of getting the schema directly, so we can validate
        the current development version! Oustide metamist, you'd just leave the
        schema option blank, and it would fetch the schema from the server.
        """

        # only need to apply schema to the first client to create, then it gets cached
        client = configure_sync_client(
            schema=api.graphql.schema.schema.as_str(), auth_token='FAKE'  # type: ignore
        )
        validate(QUERY_MATCH_PARTICIPANTS)
        validate(QUERY_MATCH_SAMPLES, client=client)
        validate(QUERY_MATCH_SEQUENCING_GROUPS, client=client)
        validate(QUERY_MATCH_ASSAYS, client=client)


class TestParseGenericMetadata(DbIsolatedTest):
    """Test the GenericMetadataParser"""

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    @patch('os.path.getsize')
    async def test_key_map(self, mock_stat_size, mock_graphql_query):
        """
        Test the flexible key map + other options
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async
        mock_stat_size.return_value = 111

        rows = [
            'sample id,filenames',
            '<sample-id>,<sample-id>-R1.fastq.gz',
            '<sample-id>,<sample-id>-R2.fastq.gz',
        ]

        parser = GenericMetadataParser(
            search_locations=['.'],
            key_map={
                'fn': ['filenames', 'filename'],
                'sample': ['sample id', 'sample_id'],
            },
            ignore_extra_keys=False,
            reads_column='fn',
            sample_name_column='sample',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
        )
        parser.skip_checking_gcs_objects = True
        parser.filename_map = {
            '<sample-id>-R1.fastq.gz': 'gs://<sample-id>-R1.fastq.gz',
            '<sample-id>-R2.fastq.gz': 'gs://<sample-id>-R2.fastq.gz',
        }
        # samples: list[ParsedSample]
        summary, _ = await parser.parse_manifest(
            StringIO('\n'.join(rows)), delimiter=',', dry_run=True
        )

        self.assertEqual(1, summary['samples']['insert'])
        self.assertEqual(1, summary['assays']['insert'])
        self.assertEqual(0, summary['samples']['update'])
        self.assertEqual(0, summary['assays']['update'])

        parser.ignore_extra_keys = False
        rows = [
            'sample id,filenames,extra',
            '<sample-id>,<sample-id>-R1.fastq.gz,extra',
            '<sample-id>,<sample-id>-R2.fastq.gz,read-all-about-it',
        ]

        try:
            _ = await parser.parse_manifest(
                StringIO('\n'.join(rows)), delimiter=',', dry_run=True
            )
        except ValueError as e:
            self.assertEqual(
                "Key 'extra' not found in provided key map: fn, sample", str(e)
            )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.datetime_added')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    async def test_single_row(
        self, mock_filesize, mock_fileexists, mock_datetime_added, mock_graphql_query
    ):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_assay_ids_for_sample_ids_by_type
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async

        mock_filesize.return_value = 111
        mock_fileexists.return_value = False
        mock_datetime_added.return_value = datetime.fromisoformat('2022-02-02T22:22:22')

        rows = [
            'GVCF\tCRAM\tSampleId\tsample.flowcell_lane\tsample.platform\tsample.centre\tsample.reference_genome\traw_data.FREEMIX\traw_data.PCT_CHIMERAS\traw_data.MEDIAN_INSERT_SIZE\traw_data.MEDIAN_COVERAGE',
            '<sample-id>.g.vcf.gz\t<sample-id>.bam\t<sample-id>\tHK7NFCCXX.1\tILLUMINA\tKCCG\thg38\t0.01\t0.01\t400\t30',
        ]
        parser = GenericMetadataParser(
            search_locations=[],
            sample_name_column='SampleId',
            participant_meta_map={},
            sample_meta_map={'sample.centre': 'centre'},
            assay_meta_map={
                'raw_data.FREEMIX': 'qc.freemix',
                'raw_data.PCT_CHIMERAS': 'qc.pct_chimeras',
                'raw_data.MEDIAN_INSERT_SIZE': 'qc.median_insert_size',
                'raw_data.MEDIAN_COVERAGE': 'qc.median_coverage',
            },
            qc_meta_map={
                'raw_data.FREEMIX': 'freemix',
                'raw_data.PCT_CHIMERAS': 'pct_chimeras',
                'raw_data.MEDIAN_INSERT_SIZE': 'median_insert_size',
                'raw_data.MEDIAN_COVERAGE': 'median_coverage',
            },
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
            reads_column='CRAM',
            gvcf_column='GVCF',
        )

        parser.filename_map = {
            '<sample-id>.g.vcf.gz': '/path/to/<sample-id>.g.vcf.gz',
            '<sample-id>.bam': '/path/to/<sample-id>.bam',
        }

        file_contents = '\n'.join(rows)
        summary, samples = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(1, summary['samples']['insert'])
        self.assertEqual(1, summary['assays']['insert'])
        self.assertEqual(0, summary['samples']['update'])
        self.assertEqual(0, summary['assays']['update'])
        self.assertEqual(1, summary['analyses']['insert'])

        self.assertDictEqual({'centre': 'KCCG'}, samples[0].meta)
        expected_assay_dict = {
            'qc': {
                'median_insert_size': '400',
                'median_coverage': '30',
                'freemix': '0.01',
                'pct_chimeras': '0.01',
            },
            'reads_type': 'bam',
            'reads': {
                'location': '/path/to/<sample-id>.bam',
                'basename': '<sample-id>.bam',
                'class': 'File',
                'checksum': None,
                'size': 111,
                'datetime_added': '2022-02-02T22:22:22',
            },
            'sequencing_platform': 'illumina',
            'sequencing_technology': 'short-read',
            'sequencing_type': 'genome',
        }

        assay_group_dict = {
            'gvcf_types': 'gvcf',
            'gvcfs': [
                {
                    'location': '/path/to/<sample-id>.g.vcf.gz',
                    'basename': '<sample-id>.g.vcf.gz',
                    'class': 'File',
                    'checksum': None,
                    'size': 111,
                    'datetime_added': '2022-02-02T22:22:22',
                }
            ],
        }
        self.assertDictEqual(assay_group_dict, samples[0].sequencing_groups[0].meta)
        self.assertDictEqual(
            expected_assay_dict, samples[0].sequencing_groups[0].assays[0].meta
        )
        analysis = samples[0].sequencing_groups[0].analyses[0]
        self.assertDictEqual(
            {
                'median_insert_size': '400',
                'median_coverage': '30',
                'freemix': '0.01',
                'pct_chimeras': '0.01',
            },
            analysis.meta,
        )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_rows_with_participants(self, mock_graphql_query):
        """
        Test importing a single row with a participant id, forms objects and checks response
        - MOCKS: query_async
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Individual ID\tSample ID\tFilenames\tType',
            'Demeter\tsample_id001\tsample_id001.filename-R1.fastq.gz,sample_id001.filename-R2.fastq.gz\tWGS',
            'Demeter\tsample_id001\tsample_id001.exome.filename-R1.fastq.gz,sample_id001.exome.filename-R2.fastq.gz\tWES',
            'Apollo\tsample_id002\tsample_id002.filename-R1.fastq.gz\tWGS',
            'Apollo\tsample_id002\tsample_id002.filename-R2.fastq.gz\tWGS',
            'Athena\tsample_id003\tsample_id003.filename-R1.fastq.gz',
            'Athena\tsample_id003\tsample_id003.filename-R2.fastq.gz',
            'Apollo\tsample_id004\tsample_id004.filename-R1.fastq.gz',
            'Apollo\tsample_id004\tsample_id004.filename-R2.fastq.gz',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            participant_column='Individual ID',
            sample_name_column='Sample ID',
            reads_column='Filenames',
            seq_type_column='Type',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
        )

        parser.skip_checking_gcs_objects = True
        parser.filename_map = {
            'sample_id001.filename-R1.fastq.gz': '/path/to/sample_id001.filename-R1.fastq.gz',
            'sample_id001.filename-R2.fastq.gz': '/path/to/sample_id001.filename-R2.fastq.gz',
            'sample_id001.exome.filename-R1.fastq.gz': '/path/to/sample_id001.exome.filename-R1.fastq.gz',
            'sample_id001.exome.filename-R2.fastq.gz': '/path/to/sample_id001.exome.filename-R2.fastq.gz',
            'sample_id002.filename-R1.fastq.gz': '/path/to/sample_id002.filename-R1.fastq.gz',
            'sample_id002.filename-R2.fastq.gz': '/path/to/sample_id002.filename-R2.fastq.gz',
            'sample_id003.filename-R1.fastq.gz': '/path/to/sample_id003.filename-R1.fastq.gz',
            'sample_id003.filename-R2.fastq.gz': '/path/to/sample_id003.filename-R2.fastq.gz',
            'sample_id004.filename-R1.fastq.gz': '/path/to/sample_id004.filename-R1.fastq.gz',
            'sample_id004.filename-R2.fastq.gz': '/path/to/sample_id004.filename-R2.fastq.gz',
        }

        # Call generic parser
        file_contents = '\n'.join(rows)
        summary, prows = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        participants: list[ParsedParticipant] = prows

        self.assertEqual(3, summary['participants']['insert'])
        self.assertEqual(0, summary['participants']['update'])
        self.assertEqual(4, summary['samples']['insert'])
        self.assertEqual(0, summary['samples']['update'])
        self.assertEqual(5, summary['assays']['insert'])
        self.assertEqual(0, summary['assays']['update'])
        self.assertEqual(0, summary['analyses']['insert'])

        expected_assay_dict = {
            'reads': [
                {
                    'basename': 'sample_id001.filename-R1.fastq.gz',
                    'checksum': None,
                    'class': 'File',
                    'location': '/path/to/sample_id001.filename-R1.fastq.gz',
                    'size': None,
                    'datetime_added': None,
                },
                {
                    'basename': 'sample_id001.filename-R2.fastq.gz',
                    'checksum': None,
                    'class': 'File',
                    'location': '/path/to/sample_id001.filename-R2.fastq.gz',
                    'size': None,
                    'datetime_added': None,
                },
            ],
            'reads_type': 'fastq',
            'sequencing_platform': 'illumina',
            'sequencing_technology': 'short-read',
            'sequencing_type': 'genome',
        }
        assay = participants[0].samples[0].sequencing_groups[0].assays[0]
        self.maxDiff = None
        self.assertDictEqual(expected_assay_dict, assay.meta)

        # Check that both of Demeter's assays are there
        self.assertEqual(participants[0].external_pid, 'Demeter')
        self.assertEqual(len(participants[0].samples), 1)
        self.assertEqual(len(participants[0].samples[0].sequencing_groups), 2)

        return

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_rows_with_valid_participant_meta(self, mock_graphql_query):
        """
        Test importing a several rows with a participant metadata (reported gender, sex and karyotype),
        forms objects and checks response
        - MOCKS: get_sample_id_map_by_external,  get_participant_id_map_by_external_ids,
        get_assay_ids_for_sample_ids_by_type
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Individual ID\tSample ID\tSex\tGender\tKaryotype',
            'Demeter\tsample_id001\tMale\tNon-binary\tXY',
            'Apollo\tsample_id002\tFemale\tFemale\tXX',
            'Athena\tsample_id003\tFEMalE',
            'Dionysus\tsample_id00x\t\tMale\tXX',
            'Pluto\tsample_id00y',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            participant_column='Individual ID',
            sample_name_column='Sample ID',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            reported_sex_column='Sex',
            reported_gender_column='Gender',
            karyotype_column='Karyotype',
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
        )

        # Call generic parser
        file_contents = '\n'.join(rows)
        participants: list[ParsedParticipant]
        _, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )
        p_by_name = {p.external_pid: p for p in participants}
        demeter = p_by_name['Demeter']
        apollo = p_by_name['Apollo']
        athena = p_by_name['Athena']
        dionysus = p_by_name['Dionysus']
        # pluto = p_by_name['Pluto']

        # Assert that the participant meta is there.
        self.assertEqual(demeter.reported_gender, 'Non-binary')
        self.assertEqual(demeter.reported_sex, 1)
        self.assertEqual(demeter.karyotype, 'XY')
        self.assertEqual(apollo.reported_gender, 'Female')
        self.assertEqual(apollo.reported_sex, 2)
        self.assertEqual(apollo.karyotype, 'XX')
        self.assertEqual(athena.reported_sex, 2)
        self.assertIsNone(athena.reported_gender)
        self.assertIsNone(athena.karyotype)
        self.assertEqual(dionysus.reported_gender, 'Male')
        self.assertEqual(dionysus.karyotype, 'XX')
        return

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_rows_with_invalid_participant_meta(self, mock_graphql_query):
        """
        Test importing a single rows with invalid participant metadata,
        forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_participant_id_map_by_external_ids
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Individual ID\tSample ID\tSex\tKaryotype',
            'Athena\tsample_id003\tFemalee\tXX',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            participant_column='Individual ID',
            sample_name_column='Sample ID',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            reported_sex_column='Sex',
            karyotype_column='Karyotype',
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
        )

        # Call generic parser
        file_contents = '\n'.join(rows)
        with self.assertRaises(ValueError):
            await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )
        return

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_contents')
    async def test_cram_with_no_reference(
        self,
        mock_filecontents,
        mock_filesize,
        mock_fileexists,
        mock_graphql_query,
    ):
        """
        Test importing a single row with a cram with no reference
        This should throw an exception
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        mock_filecontents.return_value = 'testmd5'
        mock_filesize.return_value = 111
        mock_fileexists.return_value = True

        rows = [
            'Sample ID\tFilename',
            'sample_id003\tfile.cram',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            sample_name_column='Sample ID',
            reads_column='Filename',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
            skip_checking_gcs_objects=True,
        )

        parser.filename_map = {'file.cram': 'gs://path/file.cram'}

        # Call generic parser
        file_contents = '\n'.join(rows)
        with self.assertRaises(ValueError) as ctx:
            await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )
        self.assertEqual(
            "Reads type for 'sample_id003' is CRAM, but a reference is not defined, please set the default reference assembly path",
            str(ctx.exception),
        )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    async def test_cram_with_default_reference(
        self, mock_filesize, mock_fileexists, mock_graphql_query
    ):
        """
        Test importing a single row with a cram with no reference
        This should throw an exception
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        mock_filesize.return_value = 111
        mock_fileexists.return_value = True

        rows = [
            'Sample ID\tFilename',
            'sample_id003\tfile.cram',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            sample_name_column='Sample ID',
            reads_column='Filename',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
            default_reference_assembly_location='gs://path/file.fasta',
        )
        parser.skip_checking_gcs_objects = True
        parser.filename_map = {
            'file.cram': 'gs://path/file.cram',
            'file.fasta': 'gs://path/file.fasta',
            'file.fasta.fai': 'gs://path/file.fasta.fai',
        }

        # Call generic parser
        file_contents = '\n'.join(rows)
        samples: list[ParsedSample]
        _, samples = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        expected = {
            'location': 'gs://path/file.fasta',
            'basename': 'file.fasta',
            'class': 'File',
            'checksum': None,
            'size': None,
            'datetime_added': None,
            'secondaryFiles': [
                {
                    'location': 'gs://path/file.fasta.fai',
                    'basename': 'file.fasta.fai',
                    'class': 'File',
                    'checksum': None,
                    'size': None,
                    'datetime_added': None,
                }
            ],
        }

        self.assertDictEqual(
            expected,
            samples[0].sequencing_groups[0].assays[0].meta['reference_assembly'],
        )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    async def test_cram_with_row_level_reference(
        self, mock_fileexists, mock_graphql_query
    ):
        """
        Test importing a single row with a cram with no reference
        This should throw an exception
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        mock_fileexists.return_value = True

        rows = [
            'Sample ID\tFilename\tRef',
            'sample_id003\tfile.cram\tref.fa',
            'sample_id003\tfile2.cram\tref.fa',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            sample_name_column='Sample ID',
            reads_column='Filename',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
            reference_assembly_location_column='Ref'
            # default_reference_assembly_location='gs://path/file.fasta',
        )
        parser.skip_checking_gcs_objects = True
        parser.filename_map = {
            'file.cram': 'gs://path/file.cram',
            'file2.cram': 'gs://path/file2.cram',
            'ref.fa': 'gs://path/ref.fa',
        }

        # Call generic parser
        file_contents = '\n'.join(rows)
        samples: list[ParsedSample]
        _, samples = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        expected = {
            'location': 'gs://path/ref.fa',
            'basename': 'ref.fa',
            'class': 'File',
            'checksum': None,
            'size': None,
            'datetime_added': None,
            'secondaryFiles': [
                {
                    'location': 'gs://path/ref.fa.fai',
                    'basename': 'ref.fa.fai',
                    'class': 'File',
                    'checksum': None,
                    'size': None,
                    'datetime_added': None,
                }
            ],
        }

        self.assertDictEqual(
            expected,
            samples[0].sequencing_groups[0].assays[0].meta['reference_assembly'],
        )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    async def test_cram_with_multiple_row_level_references(
        self, mock_fileexists, mock_graphql_query
    ):
        """
        Test importing a single row with a cram with no reference
        This should throw an exception
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        mock_fileexists.return_value = True

        rows = [
            'Sample ID\tFilename\tRef',
            'sample_id003\tfile.cram\tref.fa',
            'sample_id003\tfile2.cram\tref2.fa',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            sample_name_column='Sample ID',
            reads_column='Filename',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
            reference_assembly_location_column='Ref'
            # default_reference_assembly_location='gs://path/file.fasta',
        )
        parser.skip_checking_gcs_objects = True
        parser.filename_map = {
            'file.cram': 'gs://path/file.cram',
            'file2.cram': 'gs://path/file.cram',
            'ref.fa': 'gs://path/ref.fa',
            'ref2.fa': 'gs://path/ref2.fa',
        }

        # Call generic parser
        file_contents = '\n'.join(rows)

        with self.assertRaises(ValueError) as ctx:
            await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )
        self.assertEqual(
            'Multiple reference assemblies were defined for sample_id003: ref.fa, ref2.fa',
            str(ctx.exception),
        )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.datetime_added')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    async def test_matching_sequencing_groups_and_assays(
        self, mock_filesize, mock_fileexists, mock_datetime_added, mock_graphql_query
    ):
        """Test basic import with data that exists in the database"""
        mock_graphql_query.side_effect = self.run_graphql_query_async
        mock_filesize.return_value = 111
        mock_fileexists.return_value = False
        mock_datetime_added.return_value = datetime.fromisoformat('2022-02-02T22:22:22')

        player = ParticipantLayer(self.connection)
        participant = await player.upsert_participant(
            _get_basic_participant_to_upsert()
        )

        filenames = [
            'sample_id001.filename-R1.fastq.gz',
            'sample_id001.filename-R2.fastq.gz',
        ]

        rows = [
            'Participant ID\tSample ID\tFilename',
            f'Demeter\tsample_id001\t{filenames[0]}',
            f'Demeter\tsample_id001\t{filenames[1]}',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            participant_column='Participant ID',
            sample_name_column='Sample ID',
            reads_column='Filename',
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
        )

        parser.filename_map = {f: '/path/to/' + f for f in filenames}

        summary, parsed_files = await parser.parse_manifest(
            StringIO('\n'.join(rows)), delimiter='\t', dry_run=True
        )

        self.assertEqual(1, summary['participants']['update'])
        self.assertEqual(1, summary['samples']['update'])
        self.assertEqual(1, summary['sequencing_groups']['update'])
        self.assertEqual(1, summary['assays']['update'])
        self.assertEqual(0, summary['participants']['insert'])
        self.assertEqual(0, summary['samples']['insert'])
        self.assertEqual(0, summary['sequencing_groups']['insert'])
        self.assertEqual(0, summary['assays']['insert'])

        parsed_p: ParsedParticipant = parsed_files[0]
        self.assertEqual(participant.id, parsed_p.internal_pid)
        self.assertEqual(
            sample_id_format(participant.samples[0].id),
            parsed_p.samples[0].internal_sid,
        )

        sg: SequencingGroupUpsertInternal = participant.samples[0].sequencing_groups[0]
        sg_parsed: ParsedSequencingGroup = (
            parsed_files[0].samples[0].sequencing_groups[0]
        )

        self.assertEqual(
            sequencing_group_id_format(sg.id), sg_parsed.internal_seqgroup_id
        )
        self.assertEqual(len(sg.assays), len(sg_parsed.assays))
        self.assertEqual(sg.assays[0].id, sg_parsed.assays[0].internal_id)


class FastqPairMatcher(unittest.TestCase):
    """Test Fastq pair matching logic explictly"""

    def test_simple(self):
        """Simple fastq pair matching case"""
        entries = [
            'gs://BUCKET/FAKE/<sample-id>.filename-R2.fastq.gz',
            'gs://BUCKET/FAKE/<sample-id>.filename-R1.fastq.gz',
        ]

        grouped = GenericMetadataParser.parse_fastqs_structure(entries)

        self.assertEqual(1, len(grouped))
        self.assertEqual(2, len(grouped[0]))
        self.assertListEqual(sorted(grouped[0]), grouped[0])

    def test_post_r_value_matcher(self):
        """Test entries with post R values, eg: R1_001.fastq"""
        entries = [
            'gs://BUCKET/FAKE/<sample-id>.filename-R2_001.fastq.gz',
            'gs://BUCKET/FAKE/<sample-id>.filename-R1_001.fastq.gz',
            'gs://BUCKET/FAKE/<sample-id>.filename-R1_002.fastq.gz',
            'gs://BUCKET/FAKE/<sample-id>.filename-R2_002.fastq.gz',
        ]

        grouped = GenericMetadataParser.parse_fastqs_structure(entries)

        self.assertEqual(2, len(grouped))
        self.assertEqual(2, len(grouped[0]))

        # check that the 002 got grouped together
        self.assertIn('002', grouped[1][0])
        self.assertIn('002', grouped[1][1])
