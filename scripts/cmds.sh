SM_ENVIRONMENT=
PROJECT="heartkids"
SEARCH_PATH="gs://cpg-${PROJECT}-main-upload/"
# SEARCH_PATH="gs://cpg-${PROJECT}-upload/"
SUBDIR="InglesLab_Batch_Feb2026/"
# BATCH="R_241021_ALEBUT_DNA_P002"
SEARCH_PATH+=$SUBDIR
ROOT_PATH="/Users/edwfor/Code/metamist/ingestion_metadata/"
DATA_DATE="2026-02-09"
FOLDER="${PROJECT}_${DATA_DATE}/"
ROOT_PATH+="${FOLDER}"
# SAMPLE_MAPPING="${ROOT_PATH}sample_file_mapping_${BATCH}.csv"
# MAPPING_FILE="${ROOT_PATH}mapping_file_${BATCH}.csv"
SAMPLE_MAPPING="${ROOT_PATH}sample_file_mapping.csv"
MAPPING_FILE="${ROOT_PATH}mapping_file.csv"
PED_FILE="${ROOT_PATH}ped_file.csv"
FAMILY_FILE="${ROOT_PATH}families_metadata.csv"
INDIVIDUAL_METADATA="${ROOT_PATH}individuals_metadata.csv"
INDIVIDUAL_METADATA_ORIGINAL="${ROOT_PATH}individuals_metadata_original.csv"
PED_FILE_ORIGINAL="${ROOT_PATH}ped_file_original.csv"
# CRAM_REF="gs://cpg-common-main/references/hg38/v0/dragen_reference/Homo_sapiens_assembly38_masked.fasta"
CRAM_REF="gs://cpg-common-main/references/hg38/v0/Homo_sapiens_assembly38.fasta"
# CRAM_REF="gs://cpg-genomic-autopsy-main-upload/2023-03-01/human_g1k_v37_decoy.fasta"
CRAM_REF="gs://cpg-chop-gliadx-main-upload/sequencing_files/hg19.fa"
CRAM_REF="gs://cpg-chop-gliadx-main-upload/sequencing_files/hg38.no_alt.fa"

python scripts/generate_sample_file_map.py -i $SAMPLE_MAPPING -a -p $SEARCH_PATH > $MAPPING_FILE

python scripts/parse_sample_file_map.py --project $PROJECT --search-path $SEARCH_PATH --allow-extra-files-in-search_path --dry-run --confirm $MAPPING_FILE

python scripts/clean_up_individual_metadata_manifest.py --input-file $INDIVIDUAL_METADATA_ORIGINAL --output-file $INDIVIDUAL_METADATA

python scripts/test_scripts/clean_up_pedigree.py --dataset $PROJECT --input-file $PED_FILE_ORIGINAL --output-file $PED_FILE


python scripts/parse_rd_metadata.py --project $PROJECT --ped-file $PED_FILE --individual-metadata $INDIVIDUAL_METADATA --family-metadata $FAMILY_FILE