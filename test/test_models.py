from unittest import TestCase

from models.models import (
    PRIMARY_EXTERNAL_ORG,
    ParticipantUpsert,
    ParticipantUpsertInternal,
    SampleUpsert,
    SampleUpsertInternal,
)
from models.models.output_file import OutputFileInternal
from models.utils.sample_id_format import sample_id_format


class TestParticipantModels(TestCase):
    """Test participant model conversions"""

    def test_participant_to_internal_basic(self):
        """Test converting a basic participant to internal model"""
        external = ParticipantUpsert(external_ids={PRIMARY_EXTERNAL_ORG: 'hey-hey'})
        internal = external.to_internal()

        self.assertIsInstance(internal, ParticipantUpsertInternal)
        self.assertDictEqual({PRIMARY_EXTERNAL_ORG: 'hey-hey'}, internal.external_ids)

    def test_participant_to_external_basic(self):
        """Test converting a basic participant to external model"""
        internal = ParticipantUpsertInternal(
            id=1, external_ids={PRIMARY_EXTERNAL_ORG: 'hey-hey'}
        )
        external = internal.to_external()

        self.assertIsInstance(external, ParticipantUpsert)
        self.assertEqual(1, external.id)
        self.assertDictEqual({PRIMARY_EXTERNAL_ORG: 'hey-hey'}, external.external_ids)


class TestSampleModels(TestCase):
    """Test sample model conversions"""

    def test_sample_to_internal_basic(self):
        """Test converting a basic sample to internal model"""
        external = SampleUpsert(external_ids={PRIMARY_EXTERNAL_ORG: 'hey-hey'})
        internal = external.to_internal()

        self.assertIsInstance(internal, SampleUpsertInternal)
        self.assertDictEqual({PRIMARY_EXTERNAL_ORG: 'hey-hey'}, internal.external_ids)

    def test_sample_to_external_basic(self):
        """Test converting a basic sample to external model"""
        internal = SampleUpsertInternal(
            id=1, external_ids={PRIMARY_EXTERNAL_ORG: 'hey-hey'}
        )
        external = internal.to_external()

        self.assertIsInstance(external, SampleUpsert)
        self.assertEqual(sample_id_format(1), external.id)
        self.assertDictEqual({PRIMARY_EXTERNAL_ORG: 'hey-hey'}, external.external_ids)


class TestReconstructJson(TestCase):
    """Test rebuilding the nested outputs structure from flat output_file rows"""

    @staticmethod
    def output_file(path: str):
        """Build a file as it comes back from the output_file table"""
        basename = path.rsplit('/', maxsplit=1)[-1]
        return OutputFileInternal(
            id=1,
            path=path,
            basename=basename,
            dirname='gs://bucket',
            nameroot=basename.split('.')[0],
            nameext='.' + basename.split('.')[-1],
            file_checksum=None,
            size=1,
            valid=True,
        )

    def test_secondary_file_survives_either_row_order(self):
        """
        Rows come back from the database in no guaranteed order. A secondary
        file must end up nested under its primary either way — in particular
        the primary row must not overwrite a secondary that landed first.
        """
        primary = (self.output_file('gs://bucket/file.cram'), 'cram')
        secondary = (
            self.output_file('gs://bucket/file.cram.ext'),
            'cram.secondary_files.ext',
        )

        for label, rows in (
            ('primary first', [primary, secondary]),
            ('secondary first', [secondary, primary]),
        ):
            with self.subTest(label):
                outputs = OutputFileInternal.reconstruct_json(rows)
                assert isinstance(outputs, dict)

                self.assertEqual('gs://bucket/file.cram', outputs['cram']['path'])
                self.assertEqual(
                    'gs://bucket/file.cram.ext',
                    outputs['cram']['secondary_files']['ext']['path'],
                )

    def test_file_without_secondary_files_keeps_empty_dict(self):
        """A file with no secondary files still reports an empty dict"""
        outputs = OutputFileInternal.reconstruct_json(
            [(self.output_file('gs://bucket/file.cram'), 'cram')]
        )
        assert isinstance(outputs, dict)

        self.assertEqual({}, outputs['cram']['secondary_files'])
