# pylint: disable=too-many-locals
from collections import defaultdict
from datetime import datetime
from math import ceil
from typing import Any

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.participant import ParticipantTable
from db.python.tables.sample import SampleFilter, SampleTable
from models.models.participant import ParticipantInternal
from models.models.sample import Sample, SampleInternal


class OurDnaDashboardLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        # TODO initialize other layers here as needed for aggregation
        # self.at = AnalysisRunnerTable(connection)

        self.sample_table = SampleTable(connection)
        self.participant_table = ParticipantTable(connection)

    def get_collection_to_process_end_time(self, sample: Sample) -> int | None:
        """
        I want to know how long it took between blood collection and sample processing - SAMPLE TABLE
        @fields: collection-time, process-end-time
        """
        if (
            sample.meta.get('collection_time') is None
            or sample.meta.get('process_end_time') is None
        ):
            return None

        time_taken = datetime.strptime(
            sample.meta.get('process_end_time'), '%Y-%m-%d %H:%M:%S'
        ) - datetime.strptime(sample.meta.get('collection_time'), '%Y-%m-%d %H:%M:%S')

        return int(time_taken.total_seconds())

    def get_processing_times_by_site(
        self, sample: Sample
    ) -> tuple[str | None, int | None]:
        """
        I want to know what the sample processing times were for samples at each designated site (BBV, Garvan, Westmead, etc)
        @fields: process-start-time, process-end-time, processing-site where the time fields are of the format '2022-07-03 13:28:00'
        """
        if (
            sample.meta.get('process_start_time') is None
            or sample.meta.get('process_end_time') is None
            or sample.meta.get('processing_site') is None
        ):
            return None, None

        processing_time = datetime.strptime(
            sample.meta.get('process_end_time'), '%Y-%m-%d %H:%M:%S'
        ) - datetime.strptime(
            sample.meta.get('process_start_time'), '%Y-%m-%d %H:%M:%S'
        )

        return sample.meta.get('processing_site'), int(processing_time.total_seconds())

    def get_collection_to_process_start_time(self, sample: Sample) -> int | None:
        """
        I want to know how long it has been since the sample was collected - SAMPLE TABLE
        @fields: collection-time, process-start-time
        """
        if (
            sample.meta.get('collection_time') is None
            or sample.meta.get('process_start_time') is None
        ):
            return None

        time_taken = datetime.strptime(
            sample.meta.get('process_start_time'), '%Y-%m-%d %H:%M:%S'
        ) - datetime.strptime(sample.meta.get('collection_time'), '%Y-%m-%d %H:%M:%S')

        return int(time_taken.total_seconds())

    async def query(
        self,
        filter_: SampleFilter,
        check_project_ids: bool = True,
        project_id: int = None,
    ) -> dict:
        """Get dashboard data"""
        projects: set[int]
        samples_internal: list[SampleInternal] = []
        samples: list[Sample] = []
        participants: list[ParticipantInternal] = []
        sample_participants: list[ParticipantInternal] = []

        # Data to be returned
        collection_to_process_end_time: dict[str, int] = {}
        collection_to_process_end_time_24h: dict[str, int] = {}
        processing_times_by_site: dict[str, dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )  # {site: {hour_bucket: count}}
        total_samples_by_collection_event_name: dict[str, int] = defaultdict(int)
        samples_lost_after_collection: dict[str, dict[str, Any]] = {}
        samples_concentration_gt_1ug: dict[str, float] = {}
        participants_consented_not_collected: list[int] = []
        participants_signed_not_consented: list[int] = []

        # TODO We should figure out if we need to handle more than a single project_id
        # for project_id in project_ids:
        participants.extend(
            await self.participant_table.get_participants(project=project_id)
        )

        projects, samples_internal = await self.sample_table.query(filter_=filter_)
        samples = [s.to_external() for s in samples_internal]

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        for participant in participants:
            samples_for_participant = [
                sample for sample in samples if sample.participant_id == participant.id
            ]

            # Create a new participant object with the samples for this participant
            sample_participants.append(
                participant.copy(update={'samples': samples_for_participant})
            )

            # Get the samples that have been collected and consented
            collected_samples = [
                sample
                for sample in samples_for_participant
                if participant.meta.get('consent') is not None
                and sample.meta.get('collection_time') is not None
            ]

            if len(collected_samples) == 0:
                participants_consented_not_collected.append(participant.id)

            # Get the participants that have signed but not consented
            if participant.meta.get('consent') is None:
                participants_signed_not_consented.append(participant.id)

        # TODO Add logic here to query and aggregate the data

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
            if sample.meta.get('collection_event_name') is not None:
                total_samples_by_collection_event_name[
                    sample.meta.get('collection_event_name')
                ] += 1
            else:
                total_samples_by_collection_event_name['unknown'] += 1

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
                    'collection_time': sample.meta.get('collection_time'),
                    'process_start_time': sample.meta.get('process_start_time'),
                    'process_end_time': sample.meta.get('process_end_time'),
                    'received_time': sample.meta.get('received_time'),
                    'received_by': sample.meta.get('received_by'),
                    'collection_lab': sample.meta.get('collection_lab'),
                    'courier': sample.meta.get('courier'),
                    'courier_tracking_number': sample.meta.get(
                        'courier_tracking_number'
                    ),
                    'courier_scheduled_pickup_time': sample.meta.get(
                        'courier_scheduled_pickup_time'
                    ),
                    'courier_actual_pickup_time': sample.meta.get(
                        'courier_actual_pickup_time'
                    ),
                    'courier_scheduled_dropoff_time': sample.meta.get(
                        'courier_scheduled_dropoff_time'
                    ),
                    'courier_actual_dropoff_time': sample.meta.get(
                        'courier_actual_dropoff_time'
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

        return {
            'collection_to_process_end_time': collection_to_process_end_time,
            'collection_to_process_end_time_24h': collection_to_process_end_time_24h,
            'processing_times_by_site': processing_times_by_site,
            'total_samples_by_collection_event_name': total_samples_by_collection_event_name,
            'samples_lost_after_collection': samples_lost_after_collection,
            'samples_concentration_gt_1ug': samples_concentration_gt_1ug,
            'participants_consented_not_collected': participants_consented_not_collected,
            'participants_signed_not_consented': participants_signed_not_consented,
        }
