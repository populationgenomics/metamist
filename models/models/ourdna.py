from collections import defaultdict

from pydantic import BaseModel


class OurDNALostSample(BaseModel):
    """Model for OurDNA Lost Sample"""

    sample_id: str
    time_to_process_start: int
    collection_time: str
    process_start_time: str
    process_end_time: str
    received_time: str
    received_by: str
    collection_lab: str
    courier: str
    courier_tracking_number: str
    courier_scheduled_pickup_time: str
    courier_actual_pickup_time: str
    courier_scheduled_dropoff_time: str
    courier_actual_dropoff_time: str


class OurDNADashboard(BaseModel):
    """Model for OurDNA Dashboard"""

    collection_to_process_end_time: dict[str, int] = {}
    collection_to_process_end_time_statistics: dict[str, float | None] = {}
    collection_to_process_end_time_24h: dict[str, int] = {}
    processing_times_by_site: dict[str, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    total_samples_by_collection_event_name: dict[str, int] = defaultdict(int)
    samples_lost_after_collection: list[OurDNALostSample] = []
    samples_concentration_gt_1ug: dict[str, float] = {}
    participants_consented_not_collected: list[int] = []
    participants_signed_not_consented: list[int] = []
