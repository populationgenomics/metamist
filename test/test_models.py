from unittest import TestCase

from models.models import (
    ParticipantUpsert,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SampleUpsert,
)
from models.utils.sample_id_format import sample_id_format


class TestParticipantModels(TestCase):
    """Test participant model conversions"""

    def test_participant_to_internal_basic(self):
        """Test converting a basic participant to internal model"""
        external = ParticipantUpsert(external_id='hey-hey')
        internal = external.to_internal()

        self.assertIsInstance(internal, ParticipantUpsertInternal)
        self.assertEqual('hey-hey', internal.external_id)

    def test_participant_to_external_basic(self):
        """Test converting a basic participant to external model"""
        internal = ParticipantUpsertInternal(id=1, external_id='hey-hey')
        external = internal.to_external()

        self.assertIsInstance(external, ParticipantUpsert)
        self.assertEqual(1, external.id)
        self.assertEqual('hey-hey', external.external_id)


class TestSampleModels(TestCase):
    """Test sample model conversions"""

    def test_sample_to_internal_basic(self):
        """Test converting a basic sample to internal model"""
        external = SampleUpsert(external_id='hey-hey')
        internal = external.to_internal()

        self.assertIsInstance(internal, SampleUpsertInternal)
        self.assertEqual('hey-hey', internal.external_id)

    def test_sample_to_external_basic(self):
        """Test converting a basic sample to external model"""
        internal = SampleUpsertInternal(id=1, external_id='hey-hey')
        external = internal.to_external()

        self.assertIsInstance(external, SampleUpsert)
        self.assertEqual(sample_id_format(1), external.id)
        self.assertEqual('hey-hey', external.external_id)
