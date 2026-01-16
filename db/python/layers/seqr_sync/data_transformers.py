from typing import Any

from cloudpathlib import AnyPath

from .config import SeqrDatasetType


class SeqrTransformer:
    """Transform data from Metamist to Seqr format"""

    @staticmethod
    def process_ped_sex(value):
        """Process the sex value into seqr's expected format."""
        if not isinstance(value, int):
            return value
        if value == 0:
            return 'U'
        if value == 1:
            return 'M'
        if value == 2:
            return 'F'
        return 'U'

    @staticmethod
    def process_ped_affected_status(value):
        """Process an affected status value into seqr's expected format."""
        if not isinstance(value, int):
            raise ValueError(f'Unexpected affected value: {value}')
        return {
            -9: 'U',
            0: 'U',
            1: 'N',
            2: 'A',
        }[value]

    @staticmethod
    def process_individual_metadata_hpo_terms(terms: str):
        """ "Process hpo terms into a list of terms."""
        return [t.strip() for t in terms.split(',')]

    @staticmethod
    def process_individual_metadata_affected_status(affected: str):
        """Parse the affected value from the input."""
        affected = str(affected).upper()
        if affected in ('1', 'U', 'UNAFFECTED'):
            return 'N'
        if affected == '2' or affected.startswith('A'):
            return 'A'
        if not affected or affected in ('0', 'UNKNOWN'):
            return 'U'

        return None

    @staticmethod
    def process_individual_metadata_consanguinity(consanguity: str):
        """Parse the consanguity value from the input."""
        if not consanguity:
            return None

        if isinstance(consanguity, bool):
            return consanguity

        if consanguity.lower() in ('t', 'true', 'yes', 'y', '1'):
            return True

        if consanguity.lower() in ('f', 'false', 'no', 'n', '0'):
            return False

        return None

    def format_families_metadata_rows(self, family_rows: list[dict]) -> list[dict]:
        """Format the family rows for Seqr"""
        fam_row_seqr_keys = {
            'familyId': 'externalId',
            'displayName': 'externalId',
            'description': 'description',
            'codedPhenotype': 'codedPhenotype',
        }

        family_metadata = [
            {
                seqr_key: fam.get(mm_key)
                for seqr_key, mm_key in fam_row_seqr_keys.items()
            }
            for fam in family_rows
        ]

        return family_metadata

    def format_pedigree_rows(self, ped_rows: list[dict]) -> list[dict]:
        """Format the pedigree rows for Seqr"""
        ped_row_seqr_keys = {
            'familyId': 'family_id',
            'individualId': 'individual_id',
            'paternalId': 'paternal_id',
            'maternalId': 'maternal_id',
            'notes': 'notes',
            'sex': 'sex',
            'affected': 'affected',
        }
        for row in ped_rows:
            row['sex'] = self.process_ped_sex(row['sex'])
            row['affected'] = self.process_ped_affected_status(row['affected'])

        pedigree_metadata = [
            {
                seqr_key: row.get(mm_key)
                for seqr_key, mm_key in ped_row_seqr_keys.items()
            }
            for row in ped_rows
        ]

        return pedigree_metadata

    def format_individual_metadata_rows(
        self, individual_metadata_rows: list[dict]
    ) -> list[dict]:
        """Format the individual metadata rows for Seqr"""
        key_processor = {
            'hpo_terms_present': self.process_individual_metadata_hpo_terms,
            'hpo_terms_absent': self.process_individual_metadata_hpo_terms,
            'affected': self.process_individual_metadata_affected_status,
            'consanguinity': self.process_individual_metadata_consanguinity,
        }

        seqr_map = {
            'family_id': 'family_id',
            'individual_id': 'individual_id',
            'affected': 'affected',
            'features': 'hpo_terms_present',
            'absent_features': 'hpo_terms_absent',
            'birth_year': 'birth_year',
            'death_year': 'death_year',
            'onset_age': 'age_of_onset',
            'notes': 'individual_notes',
            'consanguinity': 'consanguinity',
            'affected_relatives': 'affected_relatives',
            'expected_inheritance': 'expected_inheritance',
            'maternal_ethnicity': 'maternal_ancestry',
            'paternal_ethnicity': 'paternal_ancestry',
        }

        individual_metadata = []
        for row in individual_metadata_rows:
            individual_metadata.append(
                {
                    seqr_key: key_processor[sm_key](row[sm_key])
                    if sm_key in key_processor
                    else row[sm_key]
                    for seqr_key, sm_key in seqr_map.items()
                    if sm_key in row
                }
            )

        return individual_metadata

    def format_es_index_analysis(
        self,
        es_index_analysis: dict,
        seqr_dataset_type: SeqrDatasetType,
        sgid_to_peid_map_path: AnyPath,
        ignore_extra_samples_in_callset: bool = True,
    ) -> dict[str, Any]:
        """Format the ES index analysis for Seqr"""
        es_index_metadata = {
            'elasticsearchIndex': es_index_analysis['output'],
            'datasetType': seqr_dataset_type.value,
            'mappingFilePath': sgid_to_peid_map_path,
            'ignoreExtraSamplesInCallset': ignore_extra_samples_in_callset,
        }

        return es_index_metadata
