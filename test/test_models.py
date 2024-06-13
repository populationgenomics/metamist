from unittest import TestCase

from models.models import (
    PRIMARY_EXTERNAL_ORG,
    ParticipantUpsert,
    ParticipantUpsertInternal,
    SampleUpsert,
    SampleUpsertInternal,
)
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
        internal = ParticipantUpsertInternal(id=1, external_ids={PRIMARY_EXTERNAL_ORG: 'hey-hey'})
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
        internal = SampleUpsertInternal(id=1, external_ids={PRIMARY_EXTERNAL_ORG: 'hey-hey'})
        external = internal.to_external()

        self.assertIsInstance(external, SampleUpsert)
        self.assertEqual(sample_id_format(1), external.id)
        self.assertDictEqual({PRIMARY_EXTERNAL_ORG: 'hey-hey'}, external.external_ids)
