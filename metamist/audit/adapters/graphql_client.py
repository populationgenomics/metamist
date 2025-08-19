"""GraphQL client adapter for Metamist API."""

from typing import Any, Dict, List, Optional
from metamist.graphql import gql, query_async


class GraphQLClient:
    """Adapter for GraphQL operations."""
    
    # Query definitions
    QUERY_DATASET_SGS = gql(
        """
        query DatasetData($dataset: String!, $seqTypes: [String!], $seqTechs: [String!], $seqPlatforms: [String!]) {
            project(name: $dataset) {
                sequencingGroups(type: {in_: $seqTypes}, technology: {in_: $seqTechs}, platform: {in_: $seqPlatforms}) {
                    id
                    type
                    technology
                    platform
                    sample {
                        id
                        externalIds
                        participant {
                            id
                            externalIds
                        }
                    }
                    assays {
                        id
                        meta
                    }
                }
            }
        }
        """
    )
    
    QUERY_SG_ANALYSES = gql(
        """
        query sgAnalyses($dataset: String!, $sgIds: [String!], $analysisTypes: [String!]) {
            project(name: $dataset) {
                sequencingGroups(id: {in_: $sgIds}) {
                    id
                    analyses(status: {eq: COMPLETED}, type: {in_: $analysisTypes}) {
                        id
                        meta
                        output
                        outputs
                        type
                        timestampCompleted
                    }
                }
            }
        }
        """
    )
    
    QUERY_ENUMS = gql(
        """
        query enumsQuery {
            enum {
                analysisType
                sampleType
                sequencingType
                sequencingPlatform
                sequencingTechnology
            }
        }
        """
    )
    
    def __init__(self):
        """Initialize GraphQL client."""
        self._enums_cache: Optional[Dict[str, List[str]]] = None
    
    async def get_sequencing_groups(
        self,
        dataset: str,
        sequencing_types: List[str],
        sequencing_technologies: List[str],
        sequencing_platforms: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch sequencing groups from Metamist.
        
        Args:
            dataset: Dataset name
            sequencing_types: List of sequencing types to filter
            sequencing_technologies: List of sequencing technologies to filter
            sequencing_platforms: List of sequencing platforms to filter
            
        Returns:
            List of sequencing group dictionaries
        """
        result = await query_async(
            self.QUERY_DATASET_SGS,
            {
                'dataset': dataset,
                'seqTypes': sequencing_types,
                'seqTechs': sequencing_technologies,
                'seqPlatforms': sequencing_platforms,
            }
        )
        return result['project']['sequencingGroups']
    
    async def get_analyses_for_sequencing_groups(
        self,
        dataset: str,
        sg_ids: List[str],
        analysis_types: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch analyses for given sequencing groups.
        
        Args:
            dataset: Dataset name
            sg_ids: List of sequencing group IDs
            analysis_types: List of analysis types to filter
            
        Returns:
            List of sequencing groups with their analyses
        """
        result = await query_async(
            self.QUERY_SG_ANALYSES,
            {
                'dataset': dataset,
                'sgIds': sg_ids,
                'analysisTypes': analysis_types,
            }
        )
        return result['project']['sequencingGroups']
    
    async def get_enums(self) -> Dict[str, List[str]]:
        """
        Get all available enum values from Metamist.
        
        Returns:
            Dictionary of enum types and their values
        """
        if self._enums_cache is None:
            result = await query_async(self.QUERY_ENUMS)
            self._enums_cache = result['enum']
        return self._enums_cache
    
    async def get_enum_values(self, enum_type: str) -> List[str]:
        """
        Get values for a specific enum type.
        
        Args:
            enum_type: The enum type to get values for
            
        Returns:
            List of enum values
        """
        enums = await self.get_enums()
        
        # Map common names to GraphQL field names
        enum_map = {
            'analysis_type': 'analysisType',
            'sample_type': 'sampleType',
            'sequencing_type': 'sequencingType',
            'sequencing_platform': 'sequencingPlatform',
            'sequencing_technology': 'sequencingTechnology',
        }
        
        graphql_field = enum_map.get(enum_type, enum_type)
        
        if graphql_field not in enums:
            raise KeyError(f'Enum {enum_type} not found in Metamist GraphQL API')
        
        return enums[graphql_field]
