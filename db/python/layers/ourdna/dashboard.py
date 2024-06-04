# pylint: disable=too-many-locals
import json
from collections import defaultdict
from datetime import datetime
from math import ceil
from typing import Any

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.sample import SampleFilter
from models.models import OurDNADashboard, ProjectId, Sample


class OurDnaDashboardLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sample_layer = SampleLayer(connection)
        self.participant_layer = ParticipantLayer(connection)

    @staticmethod
    def get_meta_property(sample: Sample, property_name: str) -> Any:
        """
        Get a property from the meta field of a sample, accounting for hyphenated property names
        or underscores in the property name
        """
        return sample.meta.get(property_name) or sample.meta.get(
            property_name.replace('-', '_')
        )

    def get_collection_to_process_end_time(self, sample: Sample) -> int | None:
        """
        I want to know how long it took between blood collection and sample processing - SAMPLE TABLE
        @fields: collection-time, process-end-time
        """
        _collection_time = self.get_meta_property(
            sample=sample, property_name='collection-time'
        )
        _process_end_time = self.get_meta_property(
            sample=sample, property_name='process-end-time'
        )
        if _collection_time is None or _process_end_time is None:
            return None

        time_taken = datetime.strptime(
            _process_end_time, '%Y-%m-%d %H:%M:%S'
        ) - datetime.strptime(_collection_time, '%Y-%m-%d %H:%M:%S')

        return int(time_taken.total_seconds())

    def get_processing_times_by_site(
        self, sample: Sample
    ) -> tuple[str | None, int | None]:
        """
        I want to know what the sample processing times were for samples at each designated site (BBV, Garvan, Westmead, etc)
        @fields: process-start-time, process-end-time, processing-site where the time fields are of the format '2022-07-03 13:28:00'
        """
        _process_start_time = self.get_meta_property(
            sample=sample, property_name='process-start-time'
        )
        _process_end_time = self.get_meta_property(
            sample=sample, property_name='process-end-time'
        )
        _processing_site = self.get_meta_property(
            sample=sample, property_name='processing-site'
        )
        if (
            _process_start_time is None
            or _process_end_time is None
            or _processing_site is None
        ):
            return None, None

        processing_time = datetime.strptime(
            _process_end_time, '%Y-%m-%d %H:%M:%S'
        ) - datetime.strptime(_process_start_time, '%Y-%m-%d %H:%M:%S')

        return _processing_site, int(processing_time.total_seconds())

    def get_collection_to_process_start_time(self, sample: Sample) -> int | None:
        """
        I want to know how long it has been since the sample was collected - SAMPLE TABLE
        @fields: collection-time, process-start-time
        """
        _collection_time = self.get_meta_property(
            sample=sample, property_name='collection-time'
        )
        _process_start_time = self.get_meta_property(
            sample=sample, property_name='process-start-time'
        )
        if _collection_time is None or _process_start_time is None:
            return None

        time_taken = datetime.strptime(
            _process_start_time, '%Y-%m-%d %H:%M:%S'
        ) - datetime.strptime(_collection_time, '%Y-%m-%d %H:%M:%S')

        return int(time_taken.total_seconds())

    def fetch_key_from_meta(self, key: str, meta: str | dict[str, Any]) -> bool:
        """
        Fetches a key from the given meta, if it exists, and checks if it is not False or None
        """
        if isinstance(meta, str):
            meta = json.loads(meta)
        if isinstance(meta, dict):
            if key in meta:
                value = meta.get(key)
                if value is not False and value is not None:
                    return True
        return False

    async def query(
        self,
        filter_: SampleFilter,
        project_id: ProjectId = None,
    ) -> OurDNADashboard:
        """Get dashboard data"""
        samples: list[Sample] = []
        participants: list[tuple[int, dict, dict]] = []

        samples = [
            s.to_external() for s in await self.sample_layer.query(filter_=filter_)
        ]

        participants = (
            await self.participant_layer.get_participants_and_samples_meta_by_project(
                project=project_id
            )
        )

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
        samples_lost_after_collection: dict[str, dict[str, Any]] = (
            self.process_samples_lost_after_collection(samples=samples)
        )
        samples_concentration_gt_1ug: dict[str, float] = (
            self.process_samples_concentration_gt_1ug(samples=samples)
        )
        participants_consented_not_collected: list[int] = (
            self.process_participants_consented_not_collected(participants)
        )
        participants_signed_not_consented: list[int] = (
            self.process_participants_signed_not_consented(participants)
        )

        return OurDNADashboard.from_sample(
            d={
                'collection_to_process_end_time': collection_to_process_end_time,
                'collection_to_process_end_time_statistics': collection_to_process_end_time_statistics,
                'collection_to_process_end_time_24h': collection_to_process_end_time_24h,
                'processing_times_by_site': processing_times_by_site,
                'total_samples_by_collection_event_name': total_samples_by_collection_event_name,
                'samples_lost_after_collection': samples_lost_after_collection,
                'samples_concentration_gt_1ug': samples_concentration_gt_1ug,
                'participants_consented_not_collected': participants_consented_not_collected,
                'participants_signed_not_consented': participants_signed_not_consented,
            }
        )

    def process_collection_to_process_end_times(self, samples: list[Sample]) -> dict:
        """Get the time between blood collection and sample processing"""
        collection_to_process_end_time: dict[str, int] = {}

        for sample in samples:
            time_to_process_end = self.get_collection_to_process_end_time(sample)
            if time_to_process_end is not None:
                collection_to_process_end_time[sample.id] = time_to_process_end

        return collection_to_process_end_time

    def process_collection_to_process_end_times_statistics(
        self, collection_to_process_end_time: dict[str, int]
    ) -> dict:
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
            time_to_process_end = self.get_collection_to_process_end_time(sample)
            if time_to_process_end is not None and time_to_process_end > 24 * 60 * 60:
                collection_to_process_end_time_24h[sample.id] = time_to_process_end

        return collection_to_process_end_time_24h

    def proccess_processing_times_by_site(self, samples: list[Sample]) -> dict:
        """Get the processing times by site"""
        processing_times_by_site: dict[str, dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        for sample in samples:
            processing_site, processing_time = self.get_processing_times_by_site(sample)
            if processing_site is not None and processing_time is not None:
                hour_bucket = ceil(processing_time / 3600)
                processing_times_by_site[processing_site][hour_bucket] += 1

        for site in processing_times_by_site:
            min_bucket = min(processing_times_by_site[site].keys())
            max_bucket = max(processing_times_by_site[site].keys())

            for i in range(min_bucket, max_bucket + 1):
                if i not in processing_times_by_site[site]:
                    processing_times_by_site[site][i] = 0

        return processing_times_by_site

    def process_total_samples_by_collection_event_name(
        self, samples: list[Sample]
    ) -> dict:
        """Get total number of samples collected from each type of collection-event-name"""
        total_samples_by_collection_event_name: dict[str, int] = defaultdict(int)

        for sample in samples:
            _collection_event_name = self.get_meta_property(
                sample=sample, property_name='collection-event-name'
            )
            if _collection_event_name is not None:
                total_samples_by_collection_event_name[_collection_event_name] += 1
            else:
                total_samples_by_collection_event_name['Unknown'] += 1

        return total_samples_by_collection_event_name

    def process_samples_lost_after_collection(self, samples: list[Sample]) -> dict:
        """Get total number of many samples have been lost, EG: participants have been consented, blood collected, not processed (etc), Alert here (highlight after 72 hours)"""
        samples_lost_after_collection: dict[str, dict[str, Any]] = {}

        for sample in samples:
            time_to_process_start = self.get_collection_to_process_start_time(sample)

            if (
                time_to_process_start is not None
                and time_to_process_start > 72 * 60 * 60
            ):
                samples_lost_after_collection[sample.id] = {
                    'time_to_process_start': time_to_process_start,
                    'collection_time': self.get_meta_property(
                        sample=sample, property_name='collection-time'
                    ),
                    'process_start_time': self.get_meta_property(
                        sample=sample, property_name='process-start-time'
                    ),
                    'process_end_time': self.get_meta_property(
                        sample=sample, property_name='process-end-time'
                    ),
                    'received_time': self.get_meta_property(
                        sample=sample, property_name='received-time'
                    ),
                    'received_by': self.get_meta_property(
                        sample=sample, property_name='received-by'
                    ),
                    'collection_lab': self.get_meta_property(
                        sample=sample, property_name='collection-lab'
                    ),
                    'courier': self.get_meta_property(
                        sample=sample, property_name='courier'
                    ),
                    'courier_tracking_number': self.get_meta_property(
                        sample=sample, property_name='courier-tracking-number'
                    ),
                    'courier_scheduled_pickup_time': self.get_meta_property(
                        sample=sample, property_name='courier-scheduled-pickup-time'
                    ),
                    'courier_actual_pickup_time': self.get_meta_property(
                        sample=sample, property_name='courier-actual-pickup-time'
                    ),
                    'courier_scheduled_dropoff_time': self.get_meta_property(
                        sample=sample, property_name='courier-scheduled-dropoff-time'
                    ),
                    'courier_actual_dropoff_time': self.get_meta_property(
                        sample=sample, property_name='courier-actual-dropoff-time'
                    ),
                }

        return samples_lost_after_collection

    def process_samples_concentration_gt_1ug(self, samples: list[Sample]) -> dict:
        """Get the concentration of the sample where the concentration is more than 1 ug of DNA"""
        samples_concentration_gt_1ug: dict[str, float] = {}

        for sample in samples:
            if (
                sample.meta.get('concentration') is not None
                and float(sample.meta.get('concentration')) > 1
            ):
                samples_concentration_gt_1ug[sample.id] = float(
                    sample.meta.get('concentration')
                )

        return samples_concentration_gt_1ug

    def process_participants_consented_not_collected(
        self, participants: list[tuple[int, dict, dict]]
    ) -> list[int]:
        """Get the participants who have been consented but have not had a sample collected"""
        return [
            p[0]
            for p in participants
            if self.fetch_key_from_meta('consent', p[1])
            and not self.fetch_key_from_meta('collection-time', p[2])
        ]

    def process_participants_signed_not_consented(
        self, participants: list[tuple[int, dict, dict]]
    ) -> list[int]:
        """Get the participants who have signed but have not been consented"""
        return [
            p[0] for p in participants if not self.fetch_key_from_meta('consent', p[1])
        ]
