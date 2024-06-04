from collections import defaultdict
from typing import Any

from pydantic import BaseModel


class OurDNADashboard(BaseModel):
    """Model for OurDNA Dashboard"""

    collection_to_process_end_time: dict[str, int] = {}
    collection_to_process_end_time_statistics: dict[str, float | None] = {}
    collection_to_process_end_time_24h: dict[str, int] = {}
    processing_times_by_site: dict[str, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    total_samples_by_collection_event_name: dict[str, int] = defaultdict(int)
    samples_lost_after_collection: dict[str, dict[str, Any]] = {}
    samples_concentration_gt_1ug: dict[str, float] = {}
    participants_consented_not_collected: list[int] = []
    participants_signed_not_consented: list[int] = []

    @staticmethod
    def from_sample(d: dict) -> 'OurDNADashboard':
        """
        Convert from a sample object
        """
        collection_to_process_end_time = d.pop('collection_to_process_end_time', {})
        collection_to_process_end_time_statistics = d.pop(
            'collection_to_process_end_time_statistics', {}
        )
        collection_to_process_end_time_24h = d.pop(
            'collection_to_process_end_time_24h', {}
        )
        processing_times_by_site = d.pop('processing_times_by_site', {})
        total_samples_by_collection_event_name = d.pop(
            'total_samples_by_collection_event_name', {}
        )
        samples_lost_after_collection = d.pop('samples_lost_after_collection', {})
        samples_concentration_gt_1ug = d.pop('samples_concentration_gt_1ug', {})
        participants_consented_not_collected = d.pop(
            'participants_consented_not_collected', []
        )
        participants_signed_not_consented = d.pop(
            'participants_signed_not_consented', []
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

    def to_dict(self) -> dict:
        """
        Convert to a dictionary
        """
        return {
            'collection_to_process_end_time': self.collection_to_process_end_time,
            'collection_to_process_end_time_statistics': self.collection_to_process_end_time_statistics,
            'collection_to_process_end_time_24h': self.collection_to_process_end_time_24h,
            'processing_times_by_site': self.processing_times_by_site,
            'total_samples_by_collection_event_name': self.total_samples_by_collection_event_name,
            'samples_lost_after_collection': self.samples_lost_after_collection,
            'samples_concentration_gt_1ug': self.samples_concentration_gt_1ug,
            'participants_consented_not_collected': self.participants_consented_not_collected,
            'participants_signed_not_consented': self.participants_signed_not_consented,
        }
