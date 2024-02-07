"""
This script is used to harmonise the assay metadata from the old tob-wgs project with the new metadata schema.
This script checks the assay metadata for each assay in the project and checks if the field is in the FIELD_MAPPING dictionary.
If the field is in the dictionary, the field is added to the new_assay_data dictionary with the new field name as the key.
If the field is not in the dictionary, the a ValueError is raised.
If the field is in the dictionary but the value is None, the field is not added to the new_assay_data dictionary.
If the field is in the dictionary and the value is not None, the field is added to the new_assay_data dictionary with the new field name as the key and the value as the value.
If there are multiple mappings to the same field, the value is compared to the value already in the new_assay_data dictionary. If the values are different, the field is printed to the console.
"""
from typing import Dict

from metamist.apis import AssayApi
from metamist.graphql import gql, query
from metamist.models import AssayUpsert

SG_QUERY = gql(
    """
query MyQuery($active_status: Boolean!) {
  sample(project: {eq: "tob-wgs-test"}) {
    id
    sequencingGroups(activeOnly: {eq: $active_status}) {
      id
      assays {
        id
        meta
      }
    }
  }
}
"""
)

FIELD_MAPPING = {
    'sample_count': 'sample_count',
    'library_id': 'library_id',  # e.g. "LP0001169-NTP_B05"
    'sample.library_id': 'library_id',  # e.g. "LP0001123-NTP_F03"
    'Bioinformatics*': 'bioinformatics_package',
    'raw_data.FREEMIX': 'freemix',  # str type
    'freemix': 'freemix',  # str type
    'raw_data.PCT_CHIMERAS': 'pct_chimeras',  # str type
    'pct_chimeras': 'pct_chimeras',  # str type
    'Reference Genome*': 'reference_genome',  # e.g. "GRCh38"
    'reference_genome': 'reference_genome',  # e.g. "hg38"
    'sample.reference_genome': 'reference_genome',  # e.g. "hg38"
    'Status': 'upload_status',  # e.g. "uploaded"
    'status': 'upload_status',  # e.g. "uploaded"
    'Service*': 'sequencing_type',  # e.g. "30X WGS  (KAPA PCR- Free)"...same information as sequencing_type?
    'sequencing_type': 'sequencing_type',  # is it sequencing_type or sequence_type? e.g. "genome"
    'Specimen Type*': 'specimen_type',  # e.g. "102:Normal-Blood derived"
    'sample.flowcell_lane': 'flowcell_lane',  # e.g. "HC2NNDSX2.1-2-3-4"
    'flowcell_lane': 'flowcell_lane',  # e.g. "HLHVVDSX2.1-2-3-4"
    'median_coverage': 'median_coverage',  # str type
    'raw_data.MEDIAN_COVERAGE': 'median_coverage',  # str type
    'Eppendorf Tube Label': 'Eppendorf_tube_label',
    'raw_data.PERCENT_DUPLICATION': 'percent_duplication',  # str type
    'percent_duplication': 'percent_duplication',
    'Rack': 'rack',
    'centre': 'sequencing_centre',  # e.g. "KCCG"
    'sample.centre': 'sequencing_centre',  # e.g. "KCCG"
    'KCCG FluidX tube ID': 'fluid_x_tube_id',
    'platform': 'sequencing_platform',  # e.g. ILLUMINA
    'sequencing_platform': 'sequencing_platform',  # e.g. illumina
    'sample.platform': 'sequencing_platform',  # e.g. ILLUMINA
    'Extraction Method': 'extraction_method',
    'raw_data.MEDIAN_INSERT_SIZE': 'median_insert_size',  # str type
    'median_insert_size': 'median_insert_size',  # str type
    'Samples for Nanopore sequencing ': 'nanopore_samples_checked',  # 126 instnaces of this field - what does it mean?
    'Sample Buffer*': 'sample_buffer',
    'batch': 'batch_number',  # int type
    'batch_name': 'batch_name',
    'ng available': 'ng_available',  # 15 instances of this field
    'Container Type*': 'container_type',
    'Primary study': 'primary_study',
    'Sample Type*': 'sample_type',  # e.g. "DNA"
    'Box': 'box',
    'Well': 'well',
    'comments': 'comments',
    'sequencing_technology': 'sequencing_technology',  # e.g. "short-read"
}


class AssayHarmoniser:
    """
    A class used to harmonise assay data.

    ...

    Attributes
    ----------
    api_instance : AssayApi
        an instance of the AssayApi class
    sg_query : gql
        a GraphQL query for active and inactive sequencing groups

    Methods
    -------
    perform_upsert(new_assay_id: str, assay_data: Dict):
        Performs an upsert operation on the assay data.
    get_active_assay_data(active_sgs: Dict, sample_id: str) -> Dict:
        Retrieves the active assay data for a given sample ID.
    harmonise_assay_data(assay: Dict) -> Dict:
        Harmonises the assay data based on a predefined field mapping.
    main() -> Dict:
        Executes the main workflow of the class, which includes querying for active and inactive sequencing groups,
        harmonising the assay data, and performing upsert operations.
    """

    def __init__(self):
        self.api_instance = AssayApi()

    def perform_upsert(self, new_assay_id: str, assay_data: Dict):
        """
        Creates an upsert function for the given assay data.

        This method creates an upsert function that, when called, will update or insert (upsert) the given assay data
        using the AssayApi instance. The upsert function is not called within this method; instead, it is returned
        so it can be called later.
        """

        def upsert():
            assay_upsert = AssayUpsert(
                id=new_assay_id,
                type=None,
                external_ids=None,
                sample_id=None,
                meta=assay_data,
            )  # AssayUpsert |
            # Update Assay
            _ = self.api_instance.update_assay(assay_upsert)

        return upsert

    def get_active_assay_data(self, active_sgs: Dict, sample_id: str) -> str | None:
        """
        Retrieves the active assay ID for a given sample ID.

        This method iterates over the active sequencing groups and returns the ID of the first assay
        found for the given sample ID. It assumes that each new sequencing group has only one assay.
        """
        for sample in active_sgs['sample']:
            if sample_id == sample['id']:
                for sg in sample['sequencingGroups']:
                    # assumes new SG has only one assay per sequencing group
                    if len(sg['assays']) > 1:
                        raise ValueError(
                            f'Sequencing group has more than one assay: {sg["id"]}'
                        )
                    assay_id = sg['assays'][0]['id']
                    return assay_id

        return None

    def harmonise_assay_data(self, assay: Dict) -> Dict:
        """
        Harmonises the assay data based on a predefined field mapping.

        This method iterates over the fields in the assay data and maps them to new fields based on the FIELD_MAPPING
        dictionary. If multiple old fields map to the same new field and have different values, it prints a warning message.
        It also checks if there are any fields in the assay data that are not in the FIELD_MAPPING dictionary and raises
        an error if any are found.
        """
        harmonised_data = {}

        # check if multiple fields map to the same harmonised field
        new_assay_data: Dict[str, Dict] = {}
        for old_field, new_field in FIELD_MAPPING.items():
            if old_value := assay['meta'].get(old_field):
                existing_value = new_assay_data.get(new_field)
                if existing_value and old_value != existing_value:
                    print(
                        f'Multiple fields map to the same harmonised field: {new_field}'
                    )
                    print(
                        f'Assay ID: {assay["id"]}, old field: {old_field}, new field: {new_field}, old value: {existing_value}, new value: {old_value}'
                    )
                else:
                    new_assay_data[new_field] = old_value
        # Add fields from the original assay that are not in FIELD_MAPPING. This should not happen
        for field in assay['meta']:
            if field not in FIELD_MAPPING:
                raise ValueError(f'Field not in FIELD_MAPPING: {field}')
        harmonised_data[assay['id']] = new_assay_data
        return harmonised_data

    def main(self):
        """
        Executes the main workflow of the class.

        This method queries for active and inactive sequencing groups, harmonises the assay data, and performs upsert
        operations. It first creates a list of upsert functions without calling them. After all upsert functions have
        been created, it iterates over the list and calls each function to perform the upsert operation.
        """
        # pylint: disable=unsubscriptable-object
        active_response: Dict = query(SG_QUERY, variables={'active_status': True})
        inactive_response: Dict = query(SG_QUERY, variables={'active_status': False})

        api_calls = []
        for sample in inactive_response['sample']:
            sample_id = sample['id']
            for sg in sample['sequencingGroups']:
                for assay in sg['assays']:
                    harmonised_assay = self.harmonise_assay_data(assay)
                    active_assay_id = self.get_active_assay_data(
                        active_response, sample_id
                    )
                    api_calls.append(
                        self.perform_upsert(str(active_assay_id), harmonised_assay)
                    )

        # perform assay update
        for api_call in api_calls:
            api_call()


if __name__ == '__main__':
    harmoniser = AssayHarmoniser()
    harmoniser.main()
