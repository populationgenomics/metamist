import json

from metamist.apis import AnalysisApi, ProjectApi, SeqrApi
from metamist.graphql import gql, query_async
from metamist.model.export_type import ExportType

from .config import ES_INDEX_STAGES, SeqrDatasetType
from .logging_config import logger


class MetamistFetcher:
    """Class to fetch data from Metamist using the Metamist APIs."""

    def __init__(
        self,
        datasets: list[str] | None = None,
        sequencing_types: list[str] | None = None,
        sequencing_technologies: list[str] | None = None,
    ):
        self.datasets = datasets
        self.sequencing_types = sequencing_types
        self.sequencing_technologies = sequencing_technologies
        self.aapi = AnalysisApi()
        self.seqr_api = SeqrApi()
        self.papi = ProjectApi()

    @staticmethod
    def _sequencing_type_filter(output_path: str, sequencing_type: str) -> bool:
        if output_path.removeprefix('gs://').split('/')[0].endswith('-test'):
            return False
        if sequencing_type == 'genome' and 'exome' in output_path:
            return False
        if sequencing_type == 'exome' and 'exome' not in output_path:
            return False

        return True

    @staticmethod
    def _sequencing_technology_filter(
        output_path: str, sequencing_technology: str
    ) -> bool:
        if sequencing_technology == 'long-read' and 'pacbio' not in output_path:
            return False
        if sequencing_technology == 'short-read' and 'pacbio' in output_path:
            return False

        return True

    async def get_seqr_projects(
        self, sequencing_type, ignore_datasets=set[str] | None
    ) -> list[dict]:
        """Get all seqr projects from Metamist, excluding the ones in ignore_datasets"""
        seqr_projects_response = await self.papi.get_seqr_projects_async()
        seqr_projects = []
        for project in seqr_projects_response:
            if ignore_datasets and project['name'] in ignore_datasets:
                continue

            meta_key = f'seqr-project-{sequencing_type}'
            seqr_guid = project.get('meta', {}).get(meta_key)

            if not seqr_guid:
                continue

            seqr_projects.append(
                {
                    'name': project['name'],
                    'guid': seqr_guid,
                }
            )

        return seqr_projects

    async def get_participants_from_metamist(
        self, dataset: str, sequencing_type: str, sequencing_technology: str
    ) -> list[dict]:
        """Get participants with families from metamist"""
        _query = gql(
            """
            query MyQuery($dataset: String!, $seqType: String!,  $seqTech: String!) {
                project(name: $dataset) {
                    participants {
                        id
                        externalId
                        samples {
                            sequencingGroups(type: {eq: $seqType}, technology: {eq: $seqTech}) {
                                id
                            }
                        }
                        families {
                            externalId
                            id
                        }
                    }
                }
            }
            """
        )

        query_results = await query_async(
            _query,
            {
                'dataset': dataset,
                'seqType': sequencing_type,
                'seqTech': sequencing_technology,
            },
        )
        # check if any participant has no family
        participants = []
        for participant in query_results['project']['participants']:
            if not participant['families']:
                logger.warning(
                    f'{dataset} :: Participant {participant["externalId"]} has no family - skipping'
                )
                continue
            participants.append(participant)

        return participants

    async def get_individual_metadata_rows_from_metamist(
        self, dataset: str, participant_eids: set[str]
    ) -> list[dict]:
        """Get individual metadata rows from Metamist, containing HPO terms, affected status, and consanguinity. Must specify participant external IDs."""
        individual_metadata_resp = (
            await self.seqr_api.get_individual_metadata_for_seqr_async(
                project=dataset, export_type=ExportType('json')
            )
        )
        if individual_metadata_resp is None or isinstance(
            individual_metadata_resp, str
        ):
            raise ValueError(
                f'{dataset} :: There is an issue with getting individual metadata from Metamist, please try again later'
            )

        json_rows: list[dict] = individual_metadata_resp['rows']
        json_rows = [
            row for row in json_rows if row['individual_id'] in participant_eids
        ]

        return json_rows

    async def get_family_ids_from_external_ids(
        self, dataset: str, family_eids: set[str]
    ) -> set[str]:
        """Get internal family IDs from external family IDs"""
        _query = gql(
            """
            query MyQuery($dataset: String!, $familyEids: [String!]) {
                project(name: $dataset) {
                    families(externalId: {in_: $familyEids}) {
                        id
                    }
                }
            }
            """
        )
        query_results = await query_async(
            _query,
            {'dataset': dataset, 'familyEids': list(family_eids)},
        )
        return {f['id'] for f in query_results['project']['families']}

    async def get_participants_in_families(
        self, dataset: str, family_eids: set[str]
    ) -> set[str]:
        """Get all participants in the specified families"""
        _query = gql(
            """
            query MyQuery($dataset: String!, $familyExtIds: [String!]) {
              project(name: $dataset) {
                families(externalId: {in_: $familyExtIds}) {
                  id
                  externalId
                  participants {
                    id
                    externalId
                  }
                }
              }
            }
            """
        )
        participants = set()
        query_results = await query_async(
            _query, {'dataset': dataset, 'familyExtIds': list(family_eids)}
        )
        families = query_results['project']['families']
        for family in families:
            for participant in family['participants']:
                participants.add(participant['externalId'])
        return participants

    async def get_pedigree_rows_from_metamist(
        self,
        dataset: str,
        family_eids: set[str],
    ) -> list[dict] | None:
        """Get the pedigree from Metamist. Must specify family external IDs."""
        PED_QUERY = gql(
            """
            query MyQuery($dataset: String!, $internalFamilyIds: [Int!]) {
                project(name: $dataset) {
                    pedigree(internalFamilyIds: $internalFamilyIds)
                }
            }
            """
        )
        family_internal_ids = await self.get_family_ids_from_external_ids(
            dataset, family_eids
        )

        ped_data = await query_async(
            PED_QUERY,
            {'dataset': dataset, 'internalFamilyIds': list(family_internal_ids)},
        )

        return ped_data['project']['pedigree']

    @staticmethod
    async def get_families_metadata_from_metamist(
        dataset: str,
        family_eids: set[str],
    ) -> list[dict]:
        """
        Fetch family description and coded phenotype from Metamist. Must specify family external IDs.
        """
        _query = gql(
            """
            query MyQuery($dataset: String!, $familyEids: [String!]) {
                project(name: $dataset) {
                    families(externalId: {in_: $familyEids}) {
                        id
                        externalId
                        description
                        codedPhenotype
                    }
                }
            }
            """
        )
        query_results = await query_async(
            _query, {'dataset': dataset, 'familyEids': list(family_eids)}
        )

        return query_results['project']['families']

    async def get_sgs_and_analyses_with_technology_and_seq_type(
        self,
        dataset: str,
        sequencing_type: str,
        sequencing_technology: str,
        analysis_type: str,
    ) -> list[dict]:
        """Get sequencing group IDs with the specified sequencing type and technology"""
        _query = gql(
            """
            query MyQuery($dataset: String!, $seqType: String!, $seqTech: String!, $analysisType: String!) {
              project(name: $dataset) {
                sequencingGroups(type: {eq: $seqType}, technology: {eq: $seqTech}) {
                  id
                  analyses(status: {eq: COMPLETED}, type: {eq: $analysisType}) {
                    id
                    output
                  }
                  sample {
                    id
                    externalId
                    participant {
                      id
                      externalId
                      families {
                        id
                        externalId
                      }
                    }
                  }
                }
              }
            }
            """
        )

        query_results = await query_async(
            _query,
            {
                'dataset': dataset,
                'seqType': sequencing_type,
                'seqTech': sequencing_technology,
                'analysisType': analysis_type,
            },
        )
        return query_results['project']['sequencingGroups']

    async def get_analyses_for_participants_with_technology_and_seq_type(
        self,
        dataset: str,
        participant_eids: set[str],
        sequencing_type: str,
        sequencing_technology: str,
        analysis_type: str,
    ) -> list[dict]:
        """Get analyses of a type from sequencing groups with the specified sequencing type and technology for a set of participants."""
        _query = gql(
            """
            query MyQuery($dataset: String!, $individualExtIds: [String!], $sgType: String!, $sgTech: String!, $analysisType: String!) {
              project(name: $dataset) {
                participants(externalId: {in_: $individualExtIds}) {
                  id
                  externalId
                  samples {
                    id
                    externalId
                    sequencingGroups(type: {eq: $sgType}, technology: {eq: $sgTech})  {
                      id
                      type
                      technology
                      analyses(status: {eq: COMPLETED}, type: {eq: $analysisType}) {
                        id
                        output
                      }
                    }
                  }
                }
              }
            }
            """
        )

        query_results = await query_async(
            _query,
            {
                'dataset': dataset,
                'individualExtIds': list(participant_eids),
                'sgType': sequencing_type,
                'sgTech': sequencing_technology,
                'analysisType': analysis_type,
            },
        )
        return query_results['project']['participants']

    async def get_latest_es_index_analysis(
        self, dataset: str, sequencing_type: str, seqr_dataset_type: SeqrDatasetType
    ) -> dict:
        """Get the latest ES index analysis from a list of ES index analyses"""
        es_index_analyses = await self.get_es_index_analyses_from_metamist(
            dataset, sequencing_type, seqr_dataset_type
        )
        return es_index_analyses[-1] if es_index_analyses else None

    async def get_es_index_analyses_from_metamist(
        self, dataset: str, sequencing_type: str, seqr_dataset_type: SeqrDatasetType
    ) -> list[dict]:
        """Get the ES index analyses of a given seqr dataset type for a metamist dataset"""
        stage_name = ES_INDEX_STAGES[seqr_dataset_type]
        _query = gql(
            """
            query ES_Indices($dataset: String!, $sequencingType: String!, $stage: String!) {
                project(name: $dataset) {
                    analyses(type: {eq: "es-index"}, meta: {sequencing_type: $sequencingType, stage: $stage}, status: {eq: COMPLETED}) {
                        id
                        output
                        timestampCompleted
                        meta
                        sequencingGroups {
                            id
                        }
                    }
                }
            }
            """
        )

        query_results = await query_async(
            _query,
            {
                'dataset': dataset,
                'sequencingType': sequencing_type,
                'stage': stage_name,
            },
        )

        es_index_analyses = query_results['project']['analyses']
        es_index_analyses = [
            {
                'id': analysis['id'],
                'meta': analysis['meta'],
                'output': analysis['output'],
                'timestamp_completed': analysis['timestampCompleted'],
                'sequencing_group_ids': [
                    sg['id'] for sg in analysis['sequencingGroups']
                ],
            }
            for analysis in es_index_analyses
            if analysis['timestampCompleted'] and analysis['sequencingGroups']
        ]

        es_index_analyses = sorted(
            es_index_analyses,
            key=lambda el: el['timestamp_completed'],
        )

        return es_index_analyses

    async def get_reads_map_from_metamist(
        self,
        dataset: str,
        participant_eids: set[str],
        sequencing_group_ids: set[str],
        sequencing_type: str,
        sequencing_technology: str,
    ) -> dict[str, list[dict[str, str]]]:
        """
        Get map of participant EID to cram path.

        Args:
            dataset (str): The dataset to query.
            participant_eids (Set[str]): Set of participant external IDs.
            sequencing_group_ids (Set[str]): Set of sequencing group IDs.
            sequencing_type (str): Type of sequencing (e.g., 'genome', 'exome').
            sequencing_technology (str): Technology used for sequencing (e.g., 'short-read', 'long-read').

        Returns:
            Dict[str, List[Dict[str, str]]]: A dictionary mapping participant IDs to lists of file paths.
        """
        # Fetch and filter reads map
        reads_map = await self.aapi.get_samples_reads_map_async(
            project=dataset, export_type='json'
        )
        reads_map = [r for r in reads_map if r['participant_id'] in participant_eids]

        # Remove duplicates
        unique_reads = {tuple(sorted(d.items())) for d in reads_map}
        unique_reads_map = [dict(t) for t in unique_reads]

        logger.info(
            f'{dataset} :: Filtered reads map to {len(unique_reads_map)} records for {len(participant_eids)} participants.'
        )

        peid_to_reads_map: dict[str, list[dict[str, str]]] = {
            peid: [] for peid in participant_eids
        }
        already_added = set()

        for row in unique_reads_map:
            pid, output, sg_id = (
                row['participant_id'],
                row['output'],
                row['sequencing_group_id'],
            )

            if (
                sg_id in sequencing_group_ids
                and output not in already_added
                and self._sequencing_type_filter(output, sequencing_type)
                and self._sequencing_technology_filter(output, sequencing_technology)
            ):
                peid_to_reads_map[pid].append({'filePath': output})
                already_added.add(output)

        # Count uploadable reads
        number_of_uploadable_reads = sum(
            len(reads) for reads in peid_to_reads_map.values()
        )
        logger.info(
            f'{dataset} :: Found {number_of_uploadable_reads} uploadable reads.'
        )

        return peid_to_reads_map


class FileFetcher:
    """
    Class to fetch data from files instead of Metamist. Useful if you have custom sync requirements and you write the data to temp files.

    Eventually these methods should check the file schema and raise errors if the schema is not as expected.
    """

    def __init__(self, datasets: list[str], sequencing_types: list[str]):
        self.datasets = datasets
        self.sequencing_types = sequencing_types

    @staticmethod
    def get_participants_from_file(file_path: str) -> dict:
        """
        Get participants from a file instead of via GraphQL query
        """
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def get_pedigree_rows_from_file(file_path: str) -> list[dict] | None:
        """
        Get pedigree rows directly from a file
        """
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def get_families_metadata_from_file(file_path: str) -> list[dict]:
        """
        Get families metadata rows directly from a file
        """
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def get_individual_metadata_from_file(file_path: str) -> list[dict]:
        """
        Get individual metadata rows directly from a file
        """
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def get_reads_map_from_file(file_path: str) -> dict[str, list[dict[str, str]]]:
        """
        Get the participant external ID to reads map directly from a file
        """
        with open(file_path, 'r') as f:
            return json.load(f)
