SM_ENVIRONMENT=
PROJECT="rdp-autoimmune"
SEARCH_PATH="gs://cpg-${PROJECT}-main-upload/"
SUBDIR="2024-12-20"
# BATCH="R_241021_ALEBUT_DNA_P002"
SEARCH_PATH+=$SUBDIR
ROOT_PATH="/Users/edwfor/Code/metamist/ingestion_metadata/"
DATA_DATE="2024-11-21"
FOLDER="${PROJECT}_${DATA_DATE}/"
ROOT_PATH+="${FOLDER}"
# SAMPLE_MAPPING="${ROOT_PATH}sample_file_mapping_${BATCH}.csv"
# MAPPING_FILE="${ROOT_PATH}mapping_file_${BATCH}.csv"
SAMPLE_MAPPING="${ROOT_PATH}sample_file_mapping.csv"
MAPPING_FILE="${ROOT_PATH}mapping_file.csv"
PED_FILE="${ROOT_PATH}ped_file.csv"
FAMILY_FILE="${ROOT_PATH}families_metadata.csv"
INDIVIDUAL_METADATA="${ROOT_PATH}individuals_metadata.csv"
CRAM_REF="gs://cpg-common-main/references/hg38/v0/dragen_reference/Homo_sapiens_assembly38_masked.fasta"


python generate_sample_file_map.py -i $SAMPLE_MAPPING -p $SEARCH_PATH > $MAPPING_FILE

python parse_sample_file_map.py --project $PROJECT --search-path $SEARCH_PATH --allow-extra-files-in-search_path  --confirm $MAPPING_FILE  
 
python parse_rd_metadata.py --project $PROJECT --ped-file $PED_FILE --individual-metadata $INDIVIDUAL_METADATA --family-metadata $FAMILY_FILE