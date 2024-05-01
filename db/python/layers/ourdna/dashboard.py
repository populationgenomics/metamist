from collections import defaultdict
from typing import Any

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.sample import SampleFilter, SampleTable
from models.models.sample import SampleInternal


class OurDnaDashboardLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        # TODO initialize other layers here as needed for aggregation
        # self.at = AnalysisRunnerTable(connection)

        self.sample_table = SampleTable(connection)

    async def get_collection_to_process_end_time(self, sample: SampleInternal) -> int | None:
        """
        I want to know how long it took between blood collection and sample processing - SAMPLE TABLE
        @fields: collection-time, process-end-time
        """
        if sample.meta.get('collection_time') is None or sample.meta.get('process_end_time') is None:
            return None
        return int(sample.meta.get('process_end_time')) - int(sample.meta.get('collection_time'))

    async def get_processing_times_by_site(self, sample: SampleInternal) -> tuple[str, int] | None:
        """
        I want to know what the sample processing times were for samples at each designated site (BBV, Garvan, Westmead, etc) - SAMPLE TABLE (per site, per sample)
        @fields: process-start-time, process-end-time, processing-site
        """
        if sample.meta.get('process_start_time') is None or sample.meta.get('process_end_time') is None or sample.meta.get('processing_site') is None:
            return None

        processing_time = int(sample.meta.get('process_end_time')) - int(sample.meta.get('process_start_time'))

        return sample.meta.get('processing_site'), processing_time

    async def get_collection_to_process_start_time(self, sample: SampleInternal) -> int | None:
        """
        I want to know how long it has been since the sample was collected - SAMPLE TABLE
        @fields: collection-time, process-start-time
        """
        if sample.meta.get('collection_time') is None or sample.meta.get('process_start_time') is None:
            return None
        return int(sample.meta.get('process_start_time')) - int(sample.meta.get('collection_time'))

    async def query(
        self, filter_: SampleFilter, check_project_ids: bool = True
    ) -> dict:
        """Get dashboard data"""
        projects: set[int]
        samples: list[SampleInternal]

        projects, samples = await self.sample_table.query(filter_=filter_)

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        # TODO Add logic here to query and aggregate the data

        # go through all samples and get the meta and do something with it

        collection_to_process_end_time: dict[int, int] = {}
        collection_to_process_end_time_24h: dict[int, int] = {}
        processing_times_by_site: dict[str, list[dict[int, int]]] = defaultdict(list)
        total_samples_by_collection_event_name: dict[str, int] = {}
        samples_lost_after_collection: dict[int, dict[Any, Any]] = {}

        for sample in samples:

            # Get the time between blood collection and sample processing
            time_to_process_end = await self.get_collection_to_process_end_time(sample)
            collection_to_process_end_time[sample.id] = time_to_process_end

            # Get the time between blood collection and sample processing, where time taken is more than 24 hours
            if time_to_process_end is not None and time_to_process_end > 24 * 60 * 60:
                collection_to_process_end_time_24h[sample.id] = time_to_process_end

            # Get the sample processing times for each sample at each designated site"""
            processing_site, processing_time = await self.get_processing_times_by_site(sample)
            processing_times_by_site[processing_site].append({sample.id: processing_time})

            # Get total number of samples collected from each type of collection-event-name"""
            total_samples_by_collection_event_name[sample.meta.get('collection_event_name')] += 1

            # Get total number of many samples have been lost, EG: participants have been consented, blood collected, not processed (etc), Alert here (highlight after 72 hours)
            time_to_process_start = await self.get_collection_to_process_start_time(sample)

            # If we decide to handle None, we can separate into separate checks and handle explicitly
            # can show time to now if process_start_time is None and have an alert field for this to
            # to show a warning that the sample has not been processed yet
            if time_to_process_start is not None and time_to_process_start > 72 * 60 * 60:
                samples_lost_after_collection[sample.id] = {
                    'time_to_process_start': time_to_process_start,
                    'collection_time': sample.meta.get('collection_time'),
                    'process_start_time': sample.meta.get('process_start_time'),
                    'process_end_time': sample.meta.get('process_end_time'),
                    'received_time': sample.meta.get('received_time'),
                    'received_by': sample.meta.get('received_by'),
                    'collection_lab': sample.meta.get('collection_lab'),
                    'courier': sample.meta.get('courier'),
                    'courier_tracking_number': sample.meta.get('courier_tracking_number'),
                    'courier_scheduled_pickup_time': sample.meta.get('courier_scheduled_pickup_time'),
                    'courier_actual_pickup_time': sample.meta.get('courier_actual_pickup_time'),
                    'courier_scheduled_dropoff_time': sample.meta.get('courier_scheduled_dropoff_time'),
                    'courier_actual_dropoff_time': sample.meta.get('courier_actual_dropoff_time'),
                }

        return {}