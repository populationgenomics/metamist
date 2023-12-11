PROJECT="tob-wgs-test"
SEARCH_PATH="gs://cpg-${PROJECT}/"
SUBDIR="cram/nagim/"
SEARCH_PATH+=$SUBDIR
ROOT_PATH="/Users/edwfor/Code/metamist/ingestion_metadata/"
DATA_DATE="2023-12-08"
FOLDER="${PROJECT}_${DATA_DATE}/"
ROOT_PATH+="${FOLDER}"
SAMPLE_MAPPING="${ROOT_PATH}sample_file_mapping.csv"
MAPPING_FILE="${ROOT_PATH}sample_mapping.csv"
PED_FILE="${ROOT_PATH}ped_file.csv"
FAMILY_FILE="${ROOT_PATH}families_metadata.csv"
INDIVIDUAL_METADATA="${ROOT_PATH}individuals_metadata.csv"
CRAM_REF="gs://cpg-common-main/references/hg38/v0/dragen_reference/Homo_sapiens_assembly38_masked.fasta"


python generate_sample_file_map.py -i $SAMPLE_MAPPING -p $SEARCH_PATH -g > $MAPPING_FILE


PROJECT="tob-wgs-test"
SEARCH_PATH="gs://cpg-tob-wgs-test/cram/nagim/"
MAPPING_FILE="sample_mapping.csv"
CRAM_REF="gs://cpg-common-main/references/hg38/v0/dragen_reference/Homo_sapiens_assembly38_masked.fasta" # Might need to be in the test bucket?

python parse_sample_file_map.py --project $PROJECT --search-path $SEARCH_PATH --ref $CRAM_REF --dry-run --allow-extra-files-in-search_path $MAPPING_FILE

python parse_rd_metadata.py --project $PROJECT --ped-file $PED_FILE