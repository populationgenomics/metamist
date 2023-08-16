""" Parses acute-care redcap dump ingest into metamist """

import csv
import tempfile
import click

from redcap_parsing_utils import (
    PEDFILE_HEADERS,
    INDIVIDUAL_METADATA_HEADERS,
    FAMILY_METADATA_HEADERS,
    FILEMAP_HEADERS,
    Facility,
    find_fastq_pairs,
)

from metamist.apis import FamilyApi
from metamist.parser.generic_metadata_parser import run_as_sync
from metamist.parser.sample_file_map_parser import SampleFileMapParser


PROJECT = 'rdnow'
UPLOAD_BUCKET_SEARCH_PATH = 'gs://cpg-rdnow-main-upload/'
FACILITY = 'VCGS'


def get_individual_sample_ids(individual_id, column_keys, row):
    """
    Extracts sample IDs from a row of redcap data retuning a dictionary of sample_ids by
    individual id.
    """
    sample_id_to_individual_id = {}
    for key in column_keys:
        if row[key].strip():
            sample_id_to_individual_id[row[key].strip()] = individual_id
    return sample_id_to_individual_id


def convert_sex(sex_str: str) -> int:
    """converts from male/female to 1/2"""
    if sex_str.lower() in ['male', '1']:
        return 1
    if sex_str.lower() in ['female', '2']:
        return 2
    return 0


def process_family_row(row: dict):
    """
    Extract the pedigree information from the proband and relatives 1-10
    Assumptions include:
        - rel1 is mum
        - rel2 is dad
    """
    # Handle this if and when we run into it (ask cas)
    if row['rdn_rel_existing_proband'] != '0':
        assert False, f'Handle 2nd fam member: {row}'

    family_id = row['rdn_family_id']
    proband_id = row['rdn_individual_id']
    individuals = []

    # Proband
    mother_id = row['rdn_rel_1_study_id'] if row['rdn_rel_1_study_id'] else 0
    father_id = row['rdn_rel_2_study_id'] if row['rdn_rel_2_study_id'] else 0

    individuals.append(
        {
            'Family ID': family_id,
            'Individual ID': proband_id,
            'Paternal ID': father_id,
            'Maternal ID': mother_id,
            'Sex': convert_sex(row['rdn_sex']),
            'Affected Status': 2,
        }
    )

    # Add an individual for each defined relative
    for i in range(1, 9):
        # skip if ID not set
        if not row[f'rdn_rel_{i}_study_id']:
            continue

        # Construct parent IDs
        if row[f'rdn_rel{i}_mother'] and row[f'rdn_rel{i}_mother'].strip() != '-1':
            mother_int = row[f'rdn_rel{i}_mother']
            if mother_int == '0':
                mother_id = proband_id
            else:
                mother_id = row[f'rdn_rel_{mother_int}_study_id']
        else:
            mother_id = 0

        if row[f'rdn_rel{i}_father'] and row[f'rdn_rel{i}_father'].strip() != '-1':
            father_int = row[f'rdn_rel{i}_father']
            if father_int == '0':
                father_id = proband_id
            else:
                father_id = row[f'rdn_rel_{father_int}_study_id']
        else:
            father_id = 0

        individuals.append(
            {
                'Family ID': family_id,
                'Individual ID': row[f'rdn_rel_{i}_study_id'],
                'Paternal ID': father_id,
                'Maternal ID': mother_id,
                'Sex': convert_sex(row[f'rdn_rel_{i}_sex']),
                'Affected Status': row[f'rdn_rel_{i}_affected'],
            }
        )

    return proband_id, individuals


def process_sample_row(row: dict, family_row: dict, proband_id: str):
    """
    Extract all sample IDs for each individual. Returns a pair of dicts (DNA, RNA)
    mapping each sampleID to the matching individual (sample_id, tissue tuple
    for the RNA samples).

    """
    individuals_by_sample_id__dna = {}
    individuals_by_sample_id__rna = {}

    # Proband DNA samples
    if sample_id := row['rdn_th_seq_id_v2'].strip():
        individuals_by_sample_id__dna[sample_id] = proband_id

    if sample_id := row['rdn_transfer_id'].strip():
        individuals_by_sample_id__dna[sample_id] = proband_id

    # Proband RNA
    if sample_id := row['rdn_rna_ps_id'].strip():
        recorded_tissue = row['rdn_th_seq_specimen_v2'].strip()

        # Use some assumed knowledge to determine the sequenced tissue
        if recorded_tissue == 'blood':
            tissue = 'lcl'
        elif recorded_tissue == 'skin':
            tissue = 'fibroblast'
        elif recorded_tissue == 'other':
            if 'lymphoblast' in row['rdn_th_seq_spec_oth_v2'].lower():
                tissue = 'lcl'
            else:
                tissue = row['rdn_th_seq_spec_oth_v2'].strip()
        else:
            tissue = row['rdn_th_seq_specimen_v2'].strip()

        individuals_by_sample_id__rna[sample_id] = (proband_id, tissue)

    # Process relatives (DNA only)
    for i in range(1, 9):
        individual_id = family_row[f'rdn_rel_{i}_study_id'].strip()

        if individual_id and f'rdn_th_seq_id_rel{i}_v2' in family_row:
            if sample_id := row[f'rdn_th_seq_id_rel{i}_v2'].strip():
                print(f"found {sample_id}")

                # Handle some mess in how they have set up this redcap
                if sample_id == 'N/A':
                    continue
                if len(sample_id.split()) > 1:
                    for x in sample_id.split():
                        if 'PS' in x:
                            sample_id = x
                            break

                individuals_by_sample_id__dna[sample_id] = individual_id

    return individuals_by_sample_id__dna, individuals_by_sample_id__rna


@click.command()
@click.option('-p', '--search_path')
@click.option('-d', '--dry_run', is_flag=True, default=False)
@click.option('-t', '--test_dataset', is_flag=True, default=False)
@click.argument('redcap_csv')
@run_as_sync
async def main(
    redcap_csv: str, search_path: str, dry_run: bool, test_dataset: bool, facility: str = FACILITY
):
    """
    Parse a custom formatted CSV dump from the acute-care Redcap database
    and import metadata into metamist.

    Due to the structure of the redcap database, this script must infer some
    fields based on our knowledge of the project.

    Prepares temporary pedigree, individual and family metadata files then uses
    metamist api to trigger uploads.

    """

    # Sanity check: use redcap auto generated filename to ensure this file is from
    # the RDNow project
    assert 'RDNowParticipantData-' in redcap_csv, "Is this an RDNow redcap csv?"

    if test_dataset:
        # use test version of the dataset
        project = PROJECT + '-test'

    # Prepare temp out files
    pedfile = tempfile.NamedTemporaryFile(mode='w')
    ind_file = tempfile.NamedTemporaryFile(mode='w')
    fam_file = tempfile.NamedTemporaryFile(mode='w')
    filemap_file = tempfile.NamedTemporaryFile(mode='w')

    fam_writer = csv.DictWriter(fam_file, fieldnames=FAMILY_METADATA_HEADERS)
    ped_writer = csv.DictWriter(pedfile, fieldnames=PEDFILE_HEADERS, delimiter=',')
    ind_writer = csv.DictWriter(ind_file, fieldnames=INDIVIDUAL_METADATA_HEADERS)
    filemap_writer = csv.DictWriter(filemap_file, fieldnames=FILEMAP_HEADERS)

    ind_writer.writeheader()
    ped_writer.writeheader()
    fam_writer.writeheader()
    filemap_writer.writeheader()

    # Parse rows into family units
    print("Parsing redcap csv")
    with open(redcap_csv) as csvfile:
        # The columns in redcap reports, and even their relative order can change
        # without warning. To protect against this we explicitly use the column headers.
        reader = csv.DictReader(csvfile, delimiter='\t')

        individuals_by_sample_id__dna = {}
        individuals_by_sample_id__rna = {}
        family_row = None
        proband_id = None
        for row in reader:
            # Process family header row
            if row['rdn_individual_id']:
                family_row = row
                proband_id, individuals = process_family_row(row)

                # write individuals to pedfile
                for individual in individuals:
                    ped_writer.writerow(individual)

            else:
                # Process sample row
                dna_sample_map, rna_sample_map = process_sample_row(
                    row, family_row, proband_id
                )
                individuals_by_sample_id__dna.update(dna_sample_map)
                individuals_by_sample_id__rna.update(rna_sample_map)

    # Save ped and individual files to disk then write to api
    print('Saving pedfile:')
    pedfile.flush()
    with open(pedfile.name) as p:
        print(p.read())

    if not dry_run:
        with open(pedfile.name) as p:
            response = FamilyApi().import_pedigree(
                file=p,
                has_header=True,
                project=project,
                create_missing_participants=True,
            )
        print('API response:', response)

    # TODO: parse fields to extract individual metadata
    # print('\nSaving Individual metadatafile:')
    # ind_file.flush()
    # with open(ind_file.name) as f:
    #     print(f.read())

    # if not dry_run:
    #     with open(ind_file.name) as f:
    #         response = ImportApi().import_individual_metadata_manifest(
    #             file=f,
    #             project=project,
    #         )
    #         print('API response:', response)

    print('\n\nDNA sample map:', individuals_by_sample_id__dna, '\n\n')
    print('\n\nRNA sample map:', individuals_by_sample_id__rna, '\n\n')

    # Find fastqs in upload bucket
    if not search_path:
        print('Search_path not provided so skipping file mapping')
    else:
        # Find all fastqs in search path
        found_files = False
        for sample_id, fastq_pairs in find_fastq_pairs(
            search_path, Facility(facility), recursive=False
        ).items():

            # handle concatinated sample id
            if len(sample_id.split('-')) > 1:
                for x in sample_id.split('-'):
                    if 'PS' in x:
                        sample_id = x
                        break

            print('checking:', sample_id)
            # Process DNA samples
            if sample_id in individuals_by_sample_id__dna:
                for fastq_pair in fastq_pairs:
                    row = {
                        'Individual ID': individuals_by_sample_id__dna[sample_id],
                        'Sample ID': sample_id,
                        'Filenames': ','.join(
                            sorted([x.path.name for x in fastq_pair])
                        ),
                        'Type': fastq_pair[0].seq_type.value,
                    }
                    # temporarily skip RNA
                    if 'RNA' in fastq_pair[0].seq_type.value:
                        continue
                    filemap_writer.writerow(row)
                    found_files = True

            # Process RNA samples
            if sample_id in individuals_by_sample_id__rna:
                for fastq_pair in fastq_pairs:
                    row = {
                        'Individual ID': individuals_by_sample_id__rna[sample_id][0],
                        'Sample ID': sample_id,
                        'Filenames': ','.join(
                            sorted([x.path.name for x in fastq_pair])
                        ),
                        'Type': 'transcriptome',
                    }
                    filemap_writer.writerow(row)
                    found_files = True

        if found_files:
            print('\nSaving filemap')
            filemap_file.flush()
            with open(filemap_file.name) as f:
                print(f.read())

            parser = SampleFileMapParser(
                project=project,
                search_locations=[search_path],
                allow_extra_files_in_search_path=True,
            )

            resp = await parser.from_manifest_path(
                manifest=filemap_file.name,
                confirm=True,
                dry_run=dry_run,
            )
            print(resp)
        else:
            print(
                'WARNING: did not find any read files to ingest. Remember I do not do recursive search'
            )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
