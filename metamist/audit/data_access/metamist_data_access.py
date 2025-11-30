"""Repository for Metamist data access."""

from datetime import datetime
from cpg_utils import to_path

from metamist.audit.models import (
    SequencingGroup,
    Analysis,
    Assay,
    Sample,
    Participant,
    FileMetadata,
    ExternalIds,
    AuditConfig,
)
from metamist.audit.adapters import GraphQLClient


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
    ) -> list[Analysis]:
        """
        Fetch analyses for sequencing groups.

        Args:
            dataset: Dataset name
            sg_ids: List of sequencing group IDs
            analysis_types: List of analysis types to filter

        Returns:
            List of Analysis entities
        """
        response = await self.graphql_client.get_analyses_for_sequencing_groups(
            dataset, sg_ids, analysis_types
        )

        analyses = []
        for sg_data in response:
            for analysis in sg_data.get('analyses', []):
                if parsed_analysis := self._parse_analysis(analysis, sg_data['id']):
                    analyses.append(parsed_analysis)

        return analyses

    def get_audit_deletion_analyses(self, dataset: str) -> list[dict]:
        """
        Fetch completed audit deletion analyses for a dataset.

        Args:
            dataset: Dataset name

        Returns:
            List of audit deletion analyses
        """
        return self.graphql_client.get_audit_deletion_analyses(dataset)

    def get_audit_deletion_analysis(self, dataset: str, output: str) -> dict | None:
        """
        Fetch a specific audit deletion analysis by output path.

        Args:
            dataset: Dataset name
            output: Output path of the analysis

        Returns:
            analysis GQL dict or None if not found
        """
        analyses = self.get_audit_deletion_analyses(dataset)
        for analysis in analyses:
            if analysis['output'] == output:
                return analysis
        return None

    def create_audit_deletion_analysis(
        self,
        dataset: str,
        cohort_name: str,
        audited_report_name: str,
        deletion_report_path: str,
        stats: dict,
    ) -> int:
        """
        Create a new audit deletion analysis.

        Args:
            dataset: Dataset name
            cohort_name: Name of the cohort
            audited_report_name: Name of the audited report
            deletion_report_path: Path to the deletion report
            stats: Statistics for the analysis
        """
        cohort_id = self.graphql_client.get_dataset_cohort(dataset, cohort_name)
        return self.graphql_client.create_audit_deletion_analysis(
            dataset,
            [cohort_id],
            deletion_report_path,
            meta={audited_report_name: stats},
        )

    def update_audit_deletion_analysis(
        self,
        existing_analysis: dict,
        audited_report_name: str,
        stats: dict,
    ) -> int:
        """
        Upsert a specific audit deletion analysis.

        Args:
            dataset: Dataset name
            existing_analysis: Existing analysis dict
            audited_report_name: Name of the audited report
            stats: Statistics for the analysis
        """
        current_stats = existing_analysis['meta'].get(audited_report_name, {})
        new_stats = {  # Sum the old and new stats
            'total_size': stats.get('total_size', 0)
            + current_stats.get('total_size', 0),
            'file_count': stats.get('file_count', 0)
            + current_stats.get('file_count', 0),
        }
        return self.graphql_client.update_audit_deletion_analysis(
            existing_analysis['id'],
            existing_analysis['meta'] | {audited_report_name: new_stats},
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
        Validate config args against allowed values from the Metamist enums table.

        Args:
            config: The configuration to validate

        Returns:
            Validated configuration
        """

        async def validate_enum_input(enum_type: str, input_values: tuple[str]) -> str:
            valid_values = await self.get_enum_values(enum_type)
            if 'all' in input_values:
                return valid_values
            if any(value.lower() not in valid_values for value in input_values):
                raise ValueError(
                    f"Invalid {enum_type} values: {', '.join(input_values)}. "
                    f"Valid values are: {', '.join(valid_values)}."
                )
            return tuple(input_values)

        sequencing_types = await validate_enum_input(
            'sequencing_type', config.sequencing_types
        )
        sequencing_techs = await validate_enum_input(
            'sequencing_technology', config.sequencing_technologies
        )
        sequencing_platforms = await validate_enum_input(
            'sequencing_platform', config.sequencing_platforms
        )
        analysis_types = await validate_enum_input(
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
                if secondary_file := self._parse_read_file(secondary):
                    assay.add_read_file(secondary_file)

        return assay if assay.read_files else None

    def _parse_read_file(self, data: dict) -> FileMetadata | None:
        """Parse raw read file data into entity."""
        try:
            location = data['location']
            return FileMetadata(
                filepath=to_path(location),
                filesize=data.get('size'),
                checksum=data.get('checksum'),
            )
        except (ValueError, KeyError):
            return None

    def _parse_analysis(self, data: dict, sg_id: str) -> Analysis | None:
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

        output_file = FileMetadata(
            filepath=to_path(output_path),
            filesize=output_size,
            checksum=output_checksum,
        )
        return Analysis(
            id=data['id'],
            type=data['type'],
            output_file=output_file,
            sequencing_group_id=sg_id,
            timestamp_completed=self._parse_timestamp(data.get('timestampCompleted')),
        )

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime."""
        if not timestamp_str:
            return datetime.min

        try:
            return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return datetime.min
