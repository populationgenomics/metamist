"""Repository for Metamist data access."""

from datetime import datetime
from cpg_utils import to_path

from metamist.audit.models import (
    SequencingGroup,
    Analysis as AuditAnalysis,
    Assay,
    Sample,
    Participant,
    ReadFile,
    FileMetadata,
    FilePath,
    ExternalIds,
    AuditConfig,
)
from metamist.audit.adapters import GraphQLClient

from metamist.apis import AnalysisApi
from metamist.models import (
    Analysis,
    AnalysisStatus,
    AnalysisUpdateModel,
)


class MetamistDataAccess:
    """Layer for accessing Metamist data."""

    def __init__(self, graphql_client: GraphQLClient = None):
        """
        Initialize the data access layer.

        Args:
            graphql_client: GraphQL client adapter
        """
        self.graphql_client = graphql_client or GraphQLClient()

    async def get_sequencing_groups(
        self,
        dataset: str,
        sequencing_types: list[str],
        sequencing_technologies: list[str],
        sequencing_platforms: list[str],
    ) -> list[SequencingGroup]:
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
        self, dataset: str, sg_ids: list[str], analysis_types: list[str]
    ) -> list[AuditAnalysis]:
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
        self, dataset: str, sg_ids: list[str]
    ) -> dict[str, AuditAnalysis]:
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
        analyses_by_sg: dict[str, list[AuditAnalysis]] = {}
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

    async def get_audit_deletion_analysis(
        self, dataset: str, output_path: str
    ) -> dict | None:
        """
        Fetch a specific audit deletion analysis by output path.

        Args:
            dataset: Dataset name
            output_path: Output path of the analysis

        Returns:
            analysis dict or None if not found
        """
        analyses = await self.get_audit_deletion_analyses(dataset)
        for analysis in analyses:
            if analysis['output'] == output_path:
                return analysis
        return None

    async def get_audit_deletion_analyses(self, dataset: str) -> list[dict]:
        """
        Fetch completed audit deletion analyses for a dataset.

        Args:
            dataset: Dataset name

        Returns:
            List of audit deletion analyses
        """
        return await self.graphql_client.get_audit_deletion_analyses(dataset)

    async def create_audit_deletion_analysis(
        self,
        dataset: str,
        audited_report_name: str,
        deletion_report_path: str,
        stats: dict,
    ) -> int:
        """
        Create a new audit deletion analysis.

        Args:
            dataset: Dataset name
            audited_report_name: Name of the audited report
            deletion_report_path: Path to the deletion report
            stats: Statistics for the analysis
        """
        analysis = Analysis(
            type='audit_deletion',
            output=deletion_report_path,
            status=AnalysisStatus('completed'),
            meta={audited_report_name: stats},
        )
        return await AnalysisApi().create_analysis_async(dataset, analysis)

    async def update_audit_deletion_analysis(
        self,
        existing_analysis: dict,
        audited_report_name: str,
        stats: dict,
    ) -> None:
        """
        Upsert a specific audit deletion analysis.

        Args:
            dataset: Dataset name
            existing_analysis: Existing analysis dict
            audited_report_name: Name of the audited report
            stats: Statistics for the analysis
        """
        analysis_update = AnalysisUpdateModel(
            status=AnalysisStatus('completed'),
            output=existing_analysis['output'],
            meta=existing_analysis['meta'] | {audited_report_name: stats},
        )
        await AnalysisApi().update_analysis_async(
            existing_analysis['id'], analysis_update
        )

    async def get_enum_values(self, enum_type: str) -> list[str]:
        """
        Get valid values for an enum type.

        Args:
            enum_type: The enum type to get values for

        Returns:
            List of valid enum values
        """
        return await self.graphql_client.get_enum_values(enum_type)

    async def validate_metamist_enums(
        self,
        config: 'AuditConfig',
    ) -> 'AuditConfig':
        """
        Validate enum values against Metamist API.

        Args:
            metamist: Metamist data access object

        Returns:
            Validated configuration
        """

        async def validate_enum_value(enum_type: str, config_values: tuple[str]) -> str:
            valid_values = await self.graphql_client.get_enum_values(enum_type)
            if 'all' in config_values:
                return valid_values
            if any(value.lower() not in valid_values for value in config_values):
                raise ValueError(
                    f"Invalid {enum_type} values: {', '.join(config_values)}. "
                    f"Valid values are: {', '.join(valid_values)}."
                )
            return tuple(config_values)

        sequencing_types = await validate_enum_value(
            'sequencing_type', config.sequencing_types
        )
        sequencing_techs = await validate_enum_value(
            'sequencing_technology', config.sequencing_technologies
        )
        sequencing_platforms = await validate_enum_value(
            'sequencing_platform', config.sequencing_platforms
        )
        analysis_types = await validate_enum_value(
            'analysis_type', config.analysis_types
        )

        return AuditConfig(
            dataset=config.dataset,
            sequencing_types=sequencing_types,
            sequencing_technologies=sequencing_techs,
            sequencing_platforms=sequencing_platforms,
            analysis_types=analysis_types,
            file_types=config.file_types,
            excluded_prefixes=config.excluded_prefixes,
        )

    def _parse_sequencing_group(self, data: dict) -> SequencingGroup:
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

    def _parse_assay(self, data: dict) -> Assay | None:
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

    def _parse_read_file(self, data: dict) -> ReadFile | None:
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

    def _parse_analysis(self, data: dict, sg_id: str) -> AuditAnalysis | None:
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

            return AuditAnalysis(
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
