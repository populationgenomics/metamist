PROJECT=""
SEARCH_PATH="gs://cpg-${PROJECT}-main-upload/"
SUBDIR=""
SEARCH_PATH+=$SUBDIR
ROOT_PATH=""
DATA_DATE="20xx-xx-xx"
FOLDER="${PROJECT}_${DATA_DATE}/"
ROOT_PATH+="${FOLDER}"
SAMPLE_MAPPING="${ROOT_PATH}sample_file_mapping.csv"
MAPPING_FILE="${ROOT_PATH}sample_mapping.csv"
PED_FILE="${ROOT_PATH}ped_file.csv"
FAMILY_FILE="${ROOT_PATH}families_metadata.csv"
INDIVIDUAL_METADATA="${ROOT_PATH}individuals_metadata.csv"


python generate_sample_file_map.py -i $SAMPLE_MAPPING -p $SEARCH_PATH -g > $MAPPING_FILE

python parse_sample_file_map.py --project $PROJECT --search-path $SEARCH_PATH --dry-run --default-sequence-type $MAPPING_FILE

python parse_rd_metadata.py --project $PROJECT --ped-file $PED_FILE --individual-metadata $INDIVIDUAL_METADATA