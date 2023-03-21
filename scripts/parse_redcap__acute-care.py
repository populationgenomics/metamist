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

PROJECT = "acute-care"
UPLOAD_BUCKET_SEARCH_PATH = 'gs://cpg-acute-care-main-upload/'


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
    assert 'AcuteCare' in redcap_csv, "Is this an acute-care redcap csv?"

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
            # - Individual IDs are derived from the family ID + a suffix
            # - The proband is always affected
            # - The parents are always unaffected
            # - If the parent does not have a sample ID then skip it

            # Infer individual ids from family id
            family_id = row['record_id']
            proband_id = family_id + '_proband'
            paternal_id = family_id + '_father'
            maternal_id = family_id + '_mother'

            # Save sample id : individual id mapping
            samples_by_individual_id[row['bio_patient_sampleid']] = proband_id
            if row['bio_pat_sampleid_3']:
                samples_by_individual_id[row['bio_pat_sampleid_3']] = paternal_id
            if row['bio_mat_sampleid_2']:
                samples_by_individual_id[row['bio_mat_sampleid_2']] = maternal_id

            # Get sex
            if row['dem_sex_cdc'] == "male":
                proband_sex = 1
            elif row['dem_sex_cdc'] == "female":
                proband_sex = 2
            else:
                proband_sex = 0

            # Write proband pedfile line
            ped_writer.writerow({
                'Family ID': family_id,
                'Individual ID': proband_id,
                'Paternal ID': paternal_id or "0",
                'Maternal ID': maternal_id or "0",
                'Sex': proband_sex,
                'Affected Status': "2",
                'Notes': "",
                })

            # Write maternal pedfile line
            if maternal_id:
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
            if paternal_id:
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
            hpo_present = [
                value.split('|')[0]
                for key, value in row.items()
                if key.startswith('ac_pheno_hpo')
            ]
            ind_writer.writerow({
                'Family ID': family_id,
                'Individual ID': proband_id,
                'HPO Terms (present)': ','.join(hpo_present),
                'HPO Terms (absent)': "",
                'Individual Notes': "",
                'Consanguinity': "0",
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
