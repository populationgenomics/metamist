"""
This script is used to harmonise the assay metadata from the old tob-wgs project with the new metadata schema.
This script checks the assay metadata for each assay in the project and checks if the field is in the FIELD_MAPPING dictionary.
If the field is in the dictionary, the field is added to the new_assay_data dictionary with the new field name as the key.
If the field is not in the dictionary, the a ValueError is raised.
If the field is in the dictionary but the value is None, the field is not added to the new_assay_data dictionary.
If the field is in the dictionary and the value is not None, the field is added to the new_assay_data dictionary with the new field name as the key and the value as the value.
If there are multiple mappings to the same field, the value is compared to the value already in the new_assay_data dictionary. If the values are different, the field is printed to the console.
"""

from metamist.graphql import gql, query

from metamist.apis import AssayApi
from metamist.models import AssayUpsert

from typing import Dict

api_instance = AssayApi()

_query = gql(
    """
query MyQuery {
  project(name: "tob-wgs") {
    samples {
      assays {
        id
        meta

      }
    }
  }
}"""
)

FIELD_MAPPING = {
    'sample_count': 'sample_count',
    'library_id': 'library_id',  # e.g. "LP0001169-NTP_B05"
    'sample.library_id': 'library_id',  # e.g. "LP0001123-NTP_F03"
    'Bioinformatics*': 'bioinformatics_package',
    'raw_data.FREEMIX': 'freemix',  # type: str
    'freemix': 'freemix',  # type: str
    'raw_data.PCT_CHIMERAS': 'pct_chimeras',  # type: str
    'pct_chimeras': 'pct_chimeras',  # type: str
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
    'median_coverage': 'median_coverage',  # type: str
    'raw_data.MEDIAN_COVERAGE': 'median_coverage',  # type: str
    'Eppendorf Tube Label': 'Eppendorf_tube_label',
    'raw_data.PERCENT_DUPLICATION': 'percent_duplication',  # type: str
    'percent_duplication': 'percent_duplication',
    'Rack': 'rack',
    'centre': 'sequencing_centre',  # e.g. "KCCG"
    'sample.centre': 'sequencing_centre',  # e.g. "KCCG"
    'KCCG FluidX tube ID': 'fluid_x_tube_id',
    'platform': 'sequencing_platform',  # e.g. ILLUMINA
    'sequencing_platform': 'sequencing_platform',  # e.g. illumina
    'sample.platform': 'sequencing_platform',  # e.g. ILLUMINA
    'Extraction Method': 'extraction_method',
    'raw_data.MEDIAN_INSERT_SIZE': 'median_insert_size',  # type: str
    'median_insert_size': 'median_insert_size',  # type: str
    'Samples for Nanopore sequencing ': 'nanopore_samples_checked',  # 126 instnaces of this field - what does it mean?
    'Sample Buffer*': 'sample_buffer',
    'batch': 'batch_number',  # type: int
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


def harmonise_assay_data(response: Dict) -> Dict:
    harmonised_data = {}

    # check if multiple fields map to the same harmonised field
    for sample in response['project']['samples']:
        for assay in sample['assays']:
            new_assay_data = {}
            for old_field, new_field in FIELD_MAPPING.items():
                old_value = assay['meta'].get(old_field)
                if old_value is not None:
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
            # Add fields from the original assay that are not in FIELD_MAPPING. This should not happen.
            for field in assay['meta']:
                if field not in FIELD_MAPPING:
                    raise ValueError(f'Field not in FIELD_MAPPING: {field}')
                    # new_assay_data[field] = assay['meta'][field]
            harmonised_data[assay['id']] = new_assay_data
    return harmonised_data


def main() -> Dict:
    response = query(_query)

    harmonised_data = harmonise_assay_data(response)

    # Update assay metadata
    for assay_id, assay_data in harmonised_data.items():
        assay_upsert = AssayUpsert(
            id=assay_id,
            type=None,
            external_ids=None,
            sample_id=None,
            meta=assay_data,
        )  # AssayUpsert |
        # Update Assay
        _ = api_instance.update_assay(assay_upsert)


if __name__ == '__main__':
    main()
