# pylint: disable=too-many-locals
import asyncio
from collections import defaultdict
from datetime import datetime
from functools import cached_property
from math import ceil
from typing import Any

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.base import BaseLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.sample import SampleFilter
from models.models import OurDNADashboard, OurDNALostSample, ProjectId, Sample
from models.models.participant import ParticipantInternal


class SampleProcessMeta:
    """Helper class to encapsulate sample metadata properties and calculations."""

    def __init__(self, sample: Sample):
        self.sample = sample
        self.meta = sample.meta

    def get_property(self, property_name: str) -> Any:
        """Get a property from the meta field of a sample."""
        return self.meta.get(property_name) or self.meta.get(
            property_name.replace('-', '_')
        )

    @staticmethod
    def try_parse_datetime(d: str) -> datetime | None:
        """
        Attempts to parse a datetime string in the format '%Y-%m-%d %H:%M:%S'.

        Args:
            d (str): The datetime string to parse.

        Returns:
            datetime | None: A datetime object if parsing is successful, otherwise None.
        """
        if not d:
            return None
        try:
            return datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        except TypeError as e:
            # Optionally log the error message
            print(f'Datetime passed is not a str: {e}')
            return None
        except ValueError as e:
            # Optionally log the error message
            print(f'Error parsing datetime: {e}')
            return None

    @cached_property
    def collection_time(self) -> datetime | None:
        """Returns the collection time for a sample."""
        return self.try_parse_datetime(self.get_property('collection-time'))

    @cached_property
    def process_start_time(self) -> datetime | None:
        """Returns the process start time for a sample."""
        return self.try_parse_datetime(self.get_property('process-start-time'))

    @cached_property
    def process_end_time(self) -> datetime | None:
        """Returns the process end time for a sample."""
        return self.try_parse_datetime(self.get_property('process-end-time'))

    @cached_property
    def processing_time(self) -> int | None:
        """Get processing time for a sample."""
        if not (self.process_start_time and self.process_end_time):
            return None
        return int((self.process_end_time - self.process_start_time).total_seconds())

    @cached_property
    def processing_time_by_site(self) -> tuple[str | None, int | None]:
        """Get processing times and site for a sample."""
        return self.get_property('processing-site'), self.processing_time

    @cached_property
    def collection_to_process_end_time(self) -> int | None:
        """Get the time taken from collection to process end."""
        if self.collection_time and self.process_end_time:
            return int((self.process_end_time - self.collection_time).total_seconds())
        return None

    @cached_property
    def collection_to_process_start_time(self) -> int | None:
        """Get the time taken from collection to process start."""
        if self.collection_time and self.process_start_time:
            return int((self.process_start_time - self.collection_time).total_seconds())
        return None

    @cached_property
    def time_since_collection(self) -> int | None:
        """Get the time since the sample was collected."""
        if self.collection_time:
            return int((datetime.now() - self.collection_time).total_seconds())
        return None

    @cached_property
    def get_lost_sample_properties(self) -> OurDNALostSample:
        """Returns the normalised properties to report for a sample that has been lost"""
        return OurDNALostSample(
            sample_id=self.sample.id,
            collection_time=self.get_property('collection-time'),
            process_start_time=self.get_property('process-start-time'),
            process_end_time=self.get_property('process-end-time'),
            received_time=self.get_property('received-time'),
            received_by=self.get_property('received-by'),
            collection_lab=self.get_property('collection-lab'),
            courier=self.get_property('courier'),
            courier_tracking_number=self.get_property('courier-tracking-number'),
            courier_scheduled_pickup_time=self.get_property(
                'courier-scheduled-pickup-time'
            ),
            courier_actual_pickup_time=self.get_property('courier-actual-pickup-time'),
            courier_scheduled_dropoff_time=self.get_property(
                'courier-scheduled-dropoff-time'
            ),
            courier_actual_dropoff_time=self.get_property(
                'courier-actual-dropoff-time'
            ),
            time_since_collection=self.time_since_collection,
        )

    @cached_property
    def is_lost(self) -> bool:
        """Returns True if the sample is considered lost, otherwise False."""
        # if time since collection time is > 72 hours and process_start_time is None, return True else False
        if self.collection_time:
            return (
                datetime.now() - self.collection_time
            ).total_seconds() > 72 * 60 * 60 and self.process_start_time is None
        return False


class OurDnaDashboardLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sample_layer = SampleLayer(connection)
        self.participant_layer = ParticipantLayer(connection)

    async def query(
        self,
        project_id: ProjectId,
    ) -> OurDNADashboard:
        """Get dashboard data"""
        samples: list[Sample] = []
        participants: list[ParticipantInternal] = []

        s, participants = await asyncio.gather(
            self.sample_layer.query(
                filter_=SampleFilter(
                    project=GenericFilter(eq=project_id),
                    # Get the top level samples only
                    sample_root_id=GenericFilter(isnull=True),
                )
            ),
            self.participant_layer.get_participants(project=project_id),
        )

        # Converting to external to show stats per sample (with XPG ID) via the GraphQL API
        samples = [sample.to_external() for sample in s]
        participants_by_id = {p.id: p for p in participants}

        grouped_participant_samples: dict[int, list] = defaultdict(list)

        # Group instances of A by their foreign key
        for sample in samples:
            if sample.participant_id:
                grouped_participant_samples[sample.participant_id].append(sample)

        # Data to be returned
        collection_to_process_end_time: dict[str, int] = (
            self.process_collection_to_process_end_times(samples=samples)
        )
        collection_to_process_end_time_statistics: dict[str, float | None] = (
            self.process_collection_to_process_end_times_statistics(
                collection_to_process_end_time=collection_to_process_end_time
            )
        )
        collection_to_process_end_time_24h: dict[str, int] = (
            self.process_collection_to_process_end_times_24h(samples=samples)
        )
        processing_times_by_site: dict[str, dict[int, int]] = (
            self.proccess_processing_times_by_site(samples=samples)
        )
        total_samples_by_collection_event_name: dict[str, int] = (
            self.process_total_samples_by_collection_event_name(samples=samples)
        )
        samples_lost_after_collection: list[OurDNALostSample] = (
            self.process_samples_lost_after_collection(samples=samples)
        )
        samples_concentration_gt_1ug: dict[str, float] = (
            self.process_samples_concentration_gt_1ug(samples=samples)
        )
        participants_consented_not_collected: list[int] = (
            self.process_participants_consented_not_collected(
                participants_by_id, grouped_participant_samples
            )
        )
        participants_signed_not_consented: list[int] = (
            self.process_participants_signed_not_consented(
                participants_by_id, grouped_participant_samples
            )
        )

        return OurDNADashboard(
            collection_to_process_end_time=collection_to_process_end_time,
            collection_to_process_end_time_statistics=collection_to_process_end_time_statistics,
            collection_to_process_end_time_24h=collection_to_process_end_time_24h,
            processing_times_by_site=processing_times_by_site,
            total_samples_by_collection_event_name=total_samples_by_collection_event_name,
            samples_lost_after_collection=samples_lost_after_collection,
            samples_concentration_gt_1ug=samples_concentration_gt_1ug,
            participants_consented_not_collected=participants_consented_not_collected,
            participants_signed_not_consented=participants_signed_not_consented,
        )

    def process_collection_to_process_end_times(self, samples: list[Sample]) -> dict:
        """Get the time between blood collection and sample processing"""
        collection_to_process_end_time: dict[str, int] = {}

        for sample in samples:
            processed_meta = SampleProcessMeta(sample)
            if processed_meta.collection_to_process_end_time is not None:
                collection_to_process_end_time[sample.id] = (
                    processed_meta.collection_to_process_end_time
                )
        return collection_to_process_end_time

    def process_collection_to_process_end_times_statistics(
        self, collection_to_process_end_time: dict[str, int]
    ) -> dict[str, float | None]:
        """Get the statistics for the time between blood collection and sample processing"""
        collection_to_process_end_time_statistics: dict[str, float | None] = {}

        collection_to_process_end_time_statistics['average'] = (
            sum(collection_to_process_end_time.values())
            / len(collection_to_process_end_time)
            if collection_to_process_end_time
            else None
        )

        collection_to_process_end_time_statistics['min'] = (
            min(collection_to_process_end_time.values())
            if collection_to_process_end_time
            else None
        )

        collection_to_process_end_time_statistics['max'] = (
            max(collection_to_process_end_time.values())
            if collection_to_process_end_time
            else None
        )

        return collection_to_process_end_time_statistics

    def process_collection_to_process_end_times_24h(
        self, samples: list[Sample]
    ) -> dict:
        """Get the time between blood collection and sample processing"""
        collection_to_process_end_time_24h: dict[str, int] = {}

        for sample in samples:
            processed_meta = SampleProcessMeta(sample)
            if (
                processed_meta.collection_to_process_end_time
                and processed_meta.collection_to_process_end_time > 24 * 60 * 60
            ):
                collection_to_process_end_time_24h[sample.id] = (
                    processed_meta.collection_to_process_end_time
                )
        return collection_to_process_end_time_24h

    def proccess_processing_times_by_site(self, samples: list[Sample]) -> dict:
        """Get the processing times by site"""
        processing_times_by_site: dict[str, dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        for sample in samples:
            processed_meta = SampleProcessMeta(sample)
            processing_site, processing_time = processed_meta.processing_time_by_site
            if processing_site and processing_time:
                hour_bucket = ceil(processing_time / 3600)
                processing_times_by_site[processing_site][hour_bucket] += 1

        for site in processing_times_by_site:
            min_bucket = min(processing_times_by_site[site])
            max_bucket = max(processing_times_by_site[site])
            for i in range(min_bucket, max_bucket + 1):
                processing_times_by_site[site].setdefault(i, 0)

        return processing_times_by_site

    def process_total_samples_by_collection_event_name(
        self, samples: list[Sample]
    ) -> dict:
        """Get total number of samples collected from each type of collection-event-name"""
        total_samples_by_collection_event_name: dict[str, int] = defaultdict(int)

        for sample in samples:
            processed_meta = SampleProcessMeta(sample)
            _collection_event_name = processed_meta.get_property(
                'collection-event-name'
            )
            total_samples_by_collection_event_name[
                _collection_event_name or 'Unknown'
            ] += 1
        return total_samples_by_collection_event_name

    def process_samples_lost_after_collection(
        self, samples: list[Sample]
    ) -> list[OurDNALostSample]:
        """Get total number of many samples have been lost, EG: participants have been consented, blood collected, not processed (etc), Alert here (highlight after 72 hours)"""
        samples_lost_after_collection: list[OurDNALostSample] = []

        for sample in samples:
            processed_meta = SampleProcessMeta(sample)
            if processed_meta.is_lost:
                samples_lost_after_collection.append(
                    processed_meta.get_lost_sample_properties
                )

        return samples_lost_after_collection

    def process_samples_concentration_gt_1ug(self, samples: list[Sample]) -> dict:
        """Get the concentration of the sample where the concentration is more than 1 ug of DNA"""
        samples_concentration_gt_1ug: dict[str, float] = {}

        for sample in samples:
            processed_meta = SampleProcessMeta(sample)
            concentration = processed_meta.get_property('concentration')
            if concentration and float(concentration) > 1:
                samples_concentration_gt_1ug[sample.id] = float(concentration)
        return samples_concentration_gt_1ug

    def process_participants_consented_not_collected(
        self,
        participants: dict[int, ParticipantInternal],
        grouped_participants_samples: dict[int, list[Sample]],
    ) -> list[int]:
        """Get the participants who have been consented but have not had a sample collected"""
        filtered_participants: list[int] = []
        for participant_id, samples in grouped_participants_samples.items():
            participant = participants[participant_id]
            if participant.meta.get('consent') and any(
                SampleProcessMeta(sample).collection_time is None for sample in samples
            ):
                filtered_participants.append(participant.id)
        return filtered_participants

    def process_participants_signed_not_consented(
        self,
        participants: dict[int, ParticipantInternal],
        grouped_participants_samples: dict[int, list[Sample]],
    ) -> list[int]:
        """Get the participants who have signed but have not been consented"""
        filtered_participants: list[int] = []
        for participant_id in grouped_participants_samples:
            participant = participants[participant_id]
            if not participant.meta.get('consent'):
                filtered_participants.append(participant.id)
        return filtered_participants
