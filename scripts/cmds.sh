#!/opt/homebrew/Caskroom/miniforge/base/envs/gcp/bin/python

PROJECT = 'rdp-kidney'
SEARCH_PATH = 'gs://cpg-rdp-kidney-main-upload/'
SAMPLE_MAPPING = '/Users/edwfor/Code/upload_scripts/sample-metadata/metadata_to_ingest/Feb2023_MGIKD_Sample file mapping template.csv'
PED_FILE = '/Users/edwfor/Code/upload_scripts/sample-metadata/metadata_to_ingest/Feb2023_MGIKD_individuals_template.csv'
INDIVIDUAL_METADATA = '/Users/edwfor/Code/upload_scripts/sample-metadata/metadata_to_ingest/Feb2023_MGIKD_individuals_metadata.csv'


python parse_sample_file_map.py --project PROJECT --search-path SEARCH_PATH --dry-run SAMPLE_MAPPING

python parse_rd_metadata.py --project PROJECT --ped-file PED_FILE --individual-metadata INDIVIDUAL_METADATA