PROJECT="vcgs-clinical"
SEARCH_PATH="gs://cpg-${PROJECT}-main-upload/"
SUBDIR="2023-10-05_transfer2/reanalysis_6_20231005_130103"
SEARCH_PATH+=$SUBDIR
ROOT_PATH="/Users/edwfor/Code/metamist/ingestion_metadata/"
DATA_DATE="2023-09-26"
FOLDER="${PROJECT}_${DATA_DATE}/"
ROOT_PATH+="${FOLDER}"
SAMPLE_MAPPING="${ROOT_PATH}sample_file_mapping.csv"
MAPPING_FILE="${ROOT_PATH}sample_mapping_reanalysis6.csv"
PED_FILE="${ROOT_PATH}ped_fix_reanalysis6.csv"
FAMILY_FILE="${ROOT_PATH}families_metadata.csv"
INDIVIDUAL_METADATA="${ROOT_PATH}individuals_metadata.csv"


python generate_sample_file_map.py -i $SAMPLE_MAPPING -p $SEARCH_PATH -g > $MAPPING_FILE

python parse_sample_file_map.py --project $PROJECT --search-path $SEARCH_PATH --dry-run --allow-extra-files-in-search_path $MAPPING_FILE

python parse_rd_metadata.py --project $PROJECT --ped-file $PED_FILE