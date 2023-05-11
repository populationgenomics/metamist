""" Parses acute-care redcap dump ingest into metamist """

import csv
import tempfile
import click

from sample_metadata.apis import FamilyApi, ImportApi
from sample_metadata.parser.generic_metadata_parser import run_as_sync
from sample_metadata.parser.sample_file_map_parser import SampleFileMapParser

from redcap_parsing_utils import (
    PEDFILE_HEADERS,
    INDIVIDUAL_METADATA_HEADERS,
    FAMILY_METADATA_HEADERS,
    FILEMAP_HEADERS,
    Facility,
    find_fastq_pairs,
)

PROJECT = "mcri-lrp"
UPLOAD_BUCKET_SEARCH_PATH = 'gs://cpg-mcri-lrp-main-upload/'

def get_individual_sample_ids(individual_id, column_keys, row):
    sample_id_to_individual_id = {}
    for key in column_keys:
        if row[key].strip():
            sample_id_to_individual_id[row[key].strip()] = individual_id
    return sample_id_to_individual_id


@click.command()
@click.option('-p', '--search_path')
@click.option('-d', '--dry_run', is_flag=True, default=False)
@click.option(
    '-f',
    '--facility',
    type=click.Choice(list(map(lambda x: x.name, Facility)), case_sensitive=False),
    default="VCGS",
    help="Facility to use for fastq file parsing.",
)
@click.argument('redcap_csv')
@run_as_sync
async def main(redcap_csv: str, search_path: str, facility: str, dry_run: bool):
    """
    Parse a custom formatted CSV dump from the acute-care Redcap database
    and import metadata into metamist.

    Due to the structure of the redcap database, this script must infer some
    fields based on our knowledge of the project.

    Prepares temporary pedigree, individual and family metadata files then uses
    metamist api to trigger uploads.

    Acute care uses a simple one-row-per-family redcap export so this script is a
    simpiler version of what is required for multi row per family reports.
    """

    # Sanity check: use redcap auto generated filename to ensure this file is from
    # the acute-care project
    assert 'AustralianLeukodystr' in redcap_csv, "Is this an LRP redcap csv?"

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
    samples_by_individual_id = {}
    with open(redcap_csv) as csvfile:
        # The columns in redcap reports, and even their relative order can change
        # without warning. To protect against this we explicitly use the column headers.
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            # Assumptions we can make for THIS project:
            # - Individual IDs given in pt_study_id
            # - family ID is the pt_study_id with out the '-A' suffix
            # - The proband is always affected
            # - The parents are always unaffected
            # - There are many different sample IDs - collect them all!

            # Infer individual ids from family id
            if not row['pt_study_id']:
                continue
            proband_id = row['pt_study_id']
            family_id = proband_id.rsplit('-',1)[0]

            paternal_id = f'{family_id}-2'
            maternal_id = f'{family_id}-1'

            proband_sample_id_columns = ['vcgs_exome_id','stored_dna_sampleno', 'vcgs_genome_id', 'research_rna_program_id']
            maternal_sample_id_columns = ['maternal_vcgs_exome_id','bio_mat_sam', 'maternal_vcgs_genome_id']
            paternal_sample_id_columns = ['paternal_vcgs_exome_id','bio_pat_sam', 'bio_pat_sam', 'paternal_vcgs_genome_id']

            # collect all of the different sample ids per individual
            samples_by_individual_id.update(get_individual_sample_ids(proband_id, proband_sample_id_columns, row))
            samples_by_individual_id.update(get_individual_sample_ids(paternal_id, paternal_sample_id_columns, row))
            samples_by_individual_id.update(get_individual_sample_ids(maternal_id, maternal_sample_id_columns, row))

            # Write proband pedfile line
            ped_writer.writerow({
                'Family ID': family_id,
                'Individual ID': proband_id,
                'Paternal ID': paternal_id or "0",
                'Maternal ID': maternal_id or "0",
                'Sex': row['dem_sex'],
                'Affected Status': "2",
                'Notes': "",
                })

            # Write maternal pedfile line
            if maternal_id and proband_id.endswith('-A'):
                ped_writer.writerow({
                    'Family ID': family_id,
                    'Individual ID': maternal_id,
                    'Paternal ID': "0",
                    'Maternal ID': "0",
                    'Sex': "2",
                    'Affected Status': "1",
                    'Notes': "",
                    })

            # Write paternal pedfile line
            if paternal_id and proband_id.endswith('-A'):
                ped_writer.writerow({
                    'Family ID': family_id,
                    'Individual ID': paternal_id,
                    'Paternal ID': "0",
                    'Maternal ID': "0",
                    'Sex': "1",
                    'Affected Status': "1",
                    'Notes': "",
                    })

            # Write individual metadata line (proband only)
            ind_writer.writerow({
                'Family ID': family_id,
                'Individual ID': proband_id,
                'Individual Notes': row['pt_comments'] + '\n\n' + row['dx_comments'],
            })

    # Save ped and individual files to disk then write to api
    print("Saving pedfile:")
    pedfile.flush()
    with open(pedfile.name) as p:
        print(p.read())

    if not dry_run:
        with open(pedfile.name) as p:
            response = FamilyApi().import_pedigree(
                file=p,
                has_header=True,
                project=PROJECT,
                create_missing_participants=True,
            )
        print('API response:', response)

    print("\nSaving Individual metadatafile:")
    ind_file.flush()
    with open(ind_file.name) as f:
        print(f.read())

    if not dry_run:
        with open(ind_file.name) as f:
            response = ImportApi().import_individual_metadata_manifest(
                file=f,
                project=PROJECT,
            )
            print('API response:', response)

    print('\n\nSample map:', samples_by_individual_id, '\n\n')

    # Find fastqs in upload bucket
    if not search_path:
        print("Search_path not provided so skipping file mapping")
    else:
        # Find all fastqs in search path
        found_files = False
        for sample_id, fastq_pairs in find_fastq_pairs(
            search_path, Facility(facility), recursive=False
        ).items():
            if sample_id in samples_by_individual_id:
                for fastq_pair in fastq_pairs:
                    row = {
                        'Individual ID': samples_by_individual_id[sample_id],
                        'Sample ID': sample_id,
                        'Filenames': ','.join(
                            sorted([x.path.name for x in fastq_pair])
                        ),
                        'Type': fastq_pair[0].seq_type.value,
                    }
                    ## temporarily skip RNA
                    if 'RNA' in fastq_pair[0].seq_type.value:
                        continue
                    filemap_writer.writerow(row)
                    found_files = True

        if found_files:
            print("\nSaving filemap")
            filemap_file.flush()
            with open(filemap_file.name) as f:
                print(f.read())

            parser = SampleFileMapParser(
                project=PROJECT,
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
