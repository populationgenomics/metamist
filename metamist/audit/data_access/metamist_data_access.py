"""Repository for Metamist data access."""

from typing import List, Optional, Dict, Any
from datetime import datetime
from cpg_utils import to_path

from ..models import (
    SequencingGroup,
    Analysis,
    Assay,
    Sample,
    Participant,
    ReadFile,
    FileMetadata,
    FilePath,
    ExternalIds,
)
from ..adapters import GraphQLClient


class MetamistDataAccess:
    """Layer for accessing Metamist data."""

    def __init__(self, graphql_client: GraphQLClient):
        """
        Initialize the data access layer.

        Args:
            graphql_client: GraphQL client adapter
        """
        self.graphql_client = graphql_client

    async def get_sequencing_groups(
        self,
        dataset: str,
        sequencing_types: List[str],
        sequencing_technologies: List[str],
        sequencing_platforms: List[str],
    ) -> List[SequencingGroup]:
        """
        Fetch sequencing groups from Metamist.

        Args:
            dataset: Dataset name
            sequencing_types: List of sequencing types
            sequencing_technologies: List of sequencing technologies
            sequencing_platforms: List of sequencing platforms

        Returns:
            List of SequencingGroup entities
        """
        raw_sgs = await self.graphql_client.get_sequencing_groups(
            dataset, sequencing_types, sequencing_technologies, sequencing_platforms
        )

        return [self._parse_sequencing_group(sg) for sg in raw_sgs]

    async def get_analyses_for_sequencing_groups(
        self, dataset: str, sg_ids: List[str], analysis_types: List[str]
    ) -> List[Analysis]:
        """
        Fetch analyses for sequencing groups.

        Args:
            dataset: Dataset name
            sg_ids: List of sequencing group IDs
            analysis_types: List of analysis types to filter

        Returns:
            List of Analysis entities
        """
        raw_data = await self.graphql_client.get_analyses_for_sequencing_groups(
            dataset, sg_ids, analysis_types
        )

        analyses = []
        for sg_data in raw_data:
            sg_id = sg_data['id']
            for analysis_data in sg_data.get('analyses', []):
                analysis = self._parse_analysis(analysis_data, sg_id)
                if analysis:
                    analyses.append(analysis)

        return analyses

    async def get_latest_cram_analyses(
        self, dataset: str, sg_ids: List[str]
    ) -> Dict[str, Analysis]:
        """
        Get the latest CRAM analysis for each sequencing group.

        Args:
            dataset: Dataset name
            sg_ids: List of sequencing group IDs

        Returns:
            Dictionary mapping SG ID to its latest CRAM analysis
        """
        analyses = await self.get_analyses_for_sequencing_groups(
            dataset, sg_ids, ['CRAM', 'cram']
        )

        # Group by SG and find latest
        analyses_by_sg: Dict[str, List[Analysis]] = {}
        for analysis in analyses:
            if analysis.sequencing_group_id:
                if analysis.sequencing_group_id not in analyses_by_sg:
                    analyses_by_sg[analysis.sequencing_group_id] = []
                analyses_by_sg[analysis.sequencing_group_id].append(analysis)

        # Get latest analysis for each SG
        latest_analyses = {}
        for sg_id, sg_analyses in analyses_by_sg.items():
            if sg_analyses:
                # Sort by timestamp and get the latest
                sorted_analyses = sorted(
                    sg_analyses,
                    key=lambda a: self._parse_timestamp(a.timestamp_completed or ''),
                    reverse=True,
                )
                latest_analyses[sg_id] = sorted_analyses[0]

        return latest_analyses

    async def get_enum_values(self, enum_type: str) -> List[str]:
        """
        Get valid values for an enum type.

        Args:
            enum_type: The enum type to get values for

        Returns:
            List of valid enum values
        """
        return await self.graphql_client.get_enum_values(enum_type)

    def _parse_sequencing_group(self, data: Dict[str, Any]) -> SequencingGroup:
        """Parse raw sequencing group data into entity."""
        # Parse participant
        participant_data = data['sample']['participant']
        participant = Participant(
            id=participant_data['id'],
            external_ids=ExternalIds(participant_data.get('externalIds', {})),
        )

        # Parse sample
        sample = Sample(
            id=data['sample']['id'],
            external_ids=ExternalIds(data['sample'].get('externalIds', {})),
            participant=participant,
        )

        # Parse assays
        assays = []
        for assay_data in data.get('assays', []):
            assay = self._parse_assay(assay_data)
            if assay:
                assays.append(assay)

        return SequencingGroup(
            id=data['id'],
            type=data['type'],
            technology=data['technology'],
            platform=data['platform'],
            sample=sample,
            assays=assays,
        )

    def _parse_assay(self, data: Dict[str, Any]) -> Optional[Assay]:
        """Parse raw assay data into entity."""
        assay = Assay(id=data['id'])

        # Parse read files from meta
        meta = data.get('meta', {})
        reads = meta.get('reads', [])

        # Handle both list and single read formats
        if isinstance(reads, dict):
            reads = [reads]

        for read in reads:
            read_file = self._parse_read_file(read)
            if read_file:
                assay.add_read_file(read_file)

            # Handle secondary files
            for secondary in read.get('secondaryFiles', []):
                secondary_file = self._parse_read_file(secondary)
                if secondary_file:
                    assay.add_read_file(secondary_file)

        return assay if assay.read_files else None

    def _parse_read_file(self, data: Dict[str, Any]) -> Optional[ReadFile]:
        """Parse raw read file data into entity."""
        location = data.get('location')
        if not location:
            return None

        try:
            file_path = FilePath(to_path(location))
            metadata = FileMetadata(
                filepath=file_path,
                filesize=data.get('size'),
                checksum=data.get('checksum'),
            )
            return ReadFile(metadata=metadata)
        except (ValueError, AttributeError):
            return None

    def _parse_analysis(self, data: Dict[str, Any], sg_id: str) -> Optional[Analysis]:
        """Parse raw analysis data into entity."""
        # Handle different output formats
        output_path = None
        output_size = None
        output_checksum = None

        if 'outputs' in data and isinstance(data['outputs'], dict):
            output_path = data['outputs'].get('path')
            output_size = data['outputs'].get('size')
            output_checksum = data['outputs'].get('file_checksum')
        elif 'output' in data:
            output_path = data['output']

        if not output_path:
            return None

        try:
            file_path = FilePath(to_path(output_path))
            output_file = FileMetadata(
                filepath=file_path, filesize=output_size, checksum=output_checksum
            )

            return Analysis(
                id=data['id'],
                type=data['type'],
                output_file=output_file,
                sequencing_group_id=sg_id,
                timestamp_completed=data.get('timestampCompleted'),
            )
        except (ValueError, AttributeError):
            return None

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime."""
        if not timestamp_str:
            return datetime.min

        try:
            return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return datetime.min
