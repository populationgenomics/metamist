import strawberry

from models.models.ourdna import OurDNADashboard, OurDNALostSample


@strawberry.experimental.pydantic.type(model=OurDNALostSample, all_fields=True)  # type: ignore
class GraphQLOurDNALostSample:
    """OurDNA Lost Sample GraphQL model to be used in OurDNA Dashboard"""

    pass  # pylint: disable=unnecessary-pass


@strawberry.experimental.pydantic.type(model=OurDNADashboard)  # type: ignore
class GraphQLOurDNADashboard:
    """OurDNA Dashboard model"""

    collection_to_process_end_time: strawberry.scalars.JSON
    collection_to_process_end_time_statistics: strawberry.scalars.JSON
    collection_to_process_end_time_bucket_statistics: strawberry.scalars.JSON
    collection_to_process_end_time_24h: strawberry.scalars.JSON
    processing_times_by_site: strawberry.scalars.JSON
    processing_times_by_collection_site: strawberry.scalars.JSON
    total_samples_by_collection_event_name: strawberry.scalars.JSON
    samples_lost_after_collection: list[GraphQLOurDNALostSample]
    samples_concentration_gt_1ug: strawberry.scalars.JSON
    participants_consented_not_collected: list[int]
    participants_signed_not_consented: list[int]
