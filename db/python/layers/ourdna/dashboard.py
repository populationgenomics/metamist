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
from models.models import ProjectId, Sample, SampleInternal


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
    ) -> dict:
        """Get dashboard data"""
        samples_internal: list[SampleInternal] = []
        samples: list[Sample] = []
        participants: list[tuple[int, dict, dict]] = []

        # Data to be returned
        collection_to_process_end_time: dict[str, int] = {}
        collection_to_process_end_time_statistics: dict[str, float | None] = {}
        collection_to_process_end_time_24h: dict[str, int] = {}
        processing_times_by_site: dict[str, dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )  # {site: {hour_bucket: count}}
        total_samples_by_collection_event_name: dict[str, int] = defaultdict(int)
        samples_lost_after_collection: dict[str, dict[str, Any]] = {}
        samples_concentration_gt_1ug: dict[str, float] = {}
        participants_consented_not_collected: list[int] = []
        participants_signed_not_consented: list[int] = []

        samples_internal = await self.sample_layer.query(filter_=filter_)
        samples = [s.to_external() for s in samples_internal]

        participants = (
            await self.participant_layer.get_participants_and_samples_meta_by_project(
                project=project_id
            )
        )

        # Get the participants who have been consented but have not had a sample collected
        participants_consented_not_collected.extend(
            [
                p[0]
                for p in participants
                if self.fetch_key_from_meta('consent', p[1])
                and not self.fetch_key_from_meta('collection-time', p[2])
            ]
        )

        # Get the participants who have signed but have not been consented
        participants_signed_not_consented.extend(
            [
                p[0]
                for p in participants
                if not self.fetch_key_from_meta('consent', p[1])
            ]
        )

        # go through all samples and get the meta and do something with it
        # if we want stats for samples by participant, move this logic up before participant and make use of
        # the filtered samples to map to each participant
        for sample in samples:
            # Get the time between blood collection and sample processing
            time_to_process_end = self.get_collection_to_process_end_time(sample)
            if time_to_process_end is not None:
                collection_to_process_end_time[sample.id] = time_to_process_end

            # Get the time between blood collection and sample processing, where time taken is more than 24 hours
            if time_to_process_end is not None and time_to_process_end > 24 * 60 * 60:
                collection_to_process_end_time_24h[sample.id] = time_to_process_end

            # Get the sample processing times for each sample at each designated site"""
            processing_site, processing_time = self.get_processing_times_by_site(sample)
            if processing_site is not None:
                # Take the ceiling of the processing time, then update the count for the dict accordingly for the site
                current_bucket = ceil(processing_time / 3600)
                processing_times_by_site[processing_site][current_bucket] += 1

            # Get total number of samples collected from each type of collection-event-name"""
            _collection_event_name = self.get_meta_property(
                sample=sample, property_name='collection-event-name'
            )
            if _collection_event_name is not None:
                total_samples_by_collection_event_name[_collection_event_name] += 1
            else:
                total_samples_by_collection_event_name['Unknown'] += 1

            # Get total number of many samples have been lost, EG: participants have been consented, blood collected, not processed (etc), Alert here (highlight after 72 hours)
            time_to_process_start = self.get_collection_to_process_start_time(sample)

            # If we decide to handle None, we can separate into separate checks and handle explicitly
            # can show time to now if process_start_time is None and have an alert field for this to
            # to show a warning that the sample has not been processed yet
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

            # Get the concentration of the sample where the concentration is more than 1 ug of DNA
            if (
                sample.meta.get('concentration') is not None
                and float(sample.meta.get('concentration')) > 1
            ):
                samples_concentration_gt_1ug[sample.id] = float(
                    sample.meta.get('concentration')
                )

        # Populate missing buckets with 0 count for new_processing_times_by_site
        # Get the lowest value and highest value for each site. Then iterate through the range and populate the missing buckets with 0
        for site in processing_times_by_site:
            min_bucket = min(processing_times_by_site[site].keys())
            max_bucket = max(processing_times_by_site[site].keys())

            for i in range(min_bucket, max_bucket + 1):
                if i not in processing_times_by_site[site]:
                    processing_times_by_site[site][i] = 0

        # Pull collection_to_process_end_time statistics, including average, median, min, max
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

        return {
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
