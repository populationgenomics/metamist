""" Parses UDN-Aus redcap dump ingest into metamist """

import csv
import tempfile
import sys
from collections import defaultdict
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

PROJECT = "udn-aus"
UPLOAD_BUCKET_SEARCH_PATH = 'gs://cpg-udn-aus-main-upload/'


def parse_redcap(redcap_csv: str):
    """Group rows by family and individual"""
    families = []
    samples = {}
    family_rows = {'individual_data': defaultdict(dict)}
    with open(redcap_csv) as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            # Identify first row of family (family_metadata)
            if row['redcap_event_name'] == 'family_information_arm_1':
                if not row['redcap_repeat_instrument']:
                    # Save previous family
                    if 'family_metadata' in family_rows:
                        families.append(family_rows)
                        family_rows = {'individual_data': defaultdict(dict)}

                    row['fam_id'] = row['proband_id'].strip('PR')
                    family_rows['family_metadata'] = row
                    continue
                else:
                    print(
                        f'WARNING: redcap_event_name == "family_information_arm_1" AND redcap_repeat_instrument == {row["redcap_repeat_instrument"]}. SKIPPING'
                    )
                    continue

            # Process individual level rows (sample_info and individual_metadata)
            if '_infor' in row['redcap_event_name']:
                individual_label = (
                    row['redcap_event_name'].split('_infor')[0].replace('_', '')
                )
                individual_id = family_rows['family_metadata'][individual_label + '_id']

                if row['redcap_repeat_instrument'] == "":
                    row_type = 'individual_metadata'
                elif (
                    row['redcap_repeat_instrument']
                    == "biological_samples_and_diagnosis"
                ):
                    row_type = 'sample_metadata'
                    if row['sample_id']:
                        samples[row['sample_id']] = individual_id
                    elif row['sendaway_tracking_num']:
                        # Some sample IDs end up in this column
                        samples[row['sendaway_tracking_num']] = individual_id
                else:
                    assert False, f'I was not expecting this row: {row}'
            else:
                assert False, f'I was not expecting this row: {row}'

            # Add row to individual
            family_rows['individual_data'][individual_id][row_type] = row

    # Save final family
    if 'family_metadata' in family_rows:
        families.append(family_rows)

    return families, samples


def recover_missing_individuals(family):
    """
    If the parental ID(s) are provided in the 'family_information_arm_1' row,
    but either parent does not have their own individual row like:
        'redcap_event_name' == 'maternal_informati_arm_1'
        OR
        'redcap_event_name' == 'paternal_informati_arm_1',
    then we must create the individual data for the missing parent(s) and add it
    to the family.
    """
    family_metadata = family['family_metadata']
    individual_data = family['individual_data']
    individual_ids = individual_data.keys()

    # If at least one of parents missing, use the proband metadata as a template to add the missing parent(s)
    if (
        family_metadata['maternal_id']
        and family_metadata['maternal_id'] not in individual_ids
    ) or (
        family_metadata['paternal_id']
        and family_metadata['paternal_id'] not in individual_ids
    ):
        proband_data = individual_data[family_metadata['proband_id']]
        proband_metadata = proband_data['individual_metadata']

        # Copy the proband metadata keys without copying the values to create parent metadata entries
        if (
            family_metadata['maternal_id']
            and family_metadata['maternal_id'] not in individual_ids
        ):
            maternal_metadata = {}.fromkeys(proband_metadata, '')
            maternal_metadata['sex'] = 2

            print(
                f'Adding individual: {family_metadata["maternal_id"]} to family: {family_metadata["fam_id"]}'
            )
            individual_data[family_metadata['maternal_id']] = {
                'individual_metadata': maternal_metadata
            }

        if (
            family_metadata['paternal_id']
            and family_metadata['paternal_id'] not in individual_ids
        ):
            paternal_metadata = {}.fromkeys(proband_metadata, '')
            paternal_metadata['sex'] = 1

            print(
                f'Adding individual: {family_metadata["paternal_id"]} to family: {family_metadata["fam_id"]}'
            )
            individual_data[family_metadata['paternal_id']] = {
                'individual_metadata': paternal_metadata
            }

        # Save the added parent(s) to the family
        family['individual_data'] = individual_data

    return family


def prepare_ped_row(individual_id, individual_metadata, family_metadata):
    "Return populated dict for pedfile row"
    # Parental IDs
    if individual_id.endswith("PR"):
        maternal_id = family_metadata["maternal_id"] or "0"
        paternal_id = family_metadata["paternal_id"] or "0"
    else:
        maternal_id = "0"
        paternal_id = "0"

    # notes
    notes = individual_metadata['udn_clin_summ_indiv_notes']
    if individual_metadata['udn_accepted_reason']:
        notes += (
            '\n\nUDN Accepted Reason:\n' + individual_metadata['udn_accepted_reason']
        )

    return {
        'Family ID': family_metadata['fam_id'],
        'Individual ID': individual_id,
        'Paternal ID': paternal_id,
        'Maternal ID': maternal_id,
        'Sex': individual_metadata['sex'],
        'Affected Status': individual_metadata['affected_status'] or "0",
        'Notes': notes,
    }


def prepare_ind_row(individual_id, individual_metadata, family_metadata):
    "Return populated dict for individual metadata row"

    hpo_present = [
        individual_metadata[key].split('|')[0]
        for key in individual_metadata.keys()
        if key.startswith('indiv_pheno_hpo') and individual_metadata[key]
    ]
    return {
        'Family ID': family_metadata['fam_id'],
        'Individual ID': individual_id,
        'HPO Terms (present)': ','.join(hpo_present),
        'HPO Terms (absent)': "",
        'Individual Notes': individual_metadata['udn_clin_summ_indiv_notes'],
        'Consanguinity': individual_metadata['consang'],
    }


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
    Parse a custom formatted CSV dump from the UDN-Aus Redcap database
    and import metadata into metamist.

    Due to the structure of the redcap database, this script must infer some
    fields based on our knowledge of the project.

    Prepares temporary pedigree, individual and family metadata files then uses
    metamist api to trigger uploads.
    """

    # Sanity check: use redcap auto generated filename to ensure this file is from
    # the UDN-Aus project
    assert (
        'UDNAusParticipantDat-CPGMetadata' in redcap_csv
    ), "Is this a UDN-Aus redcap csv?"

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
    families, samples = parse_redcap(redcap_csv)

    print("Preparing metadata files for upload")
    for fam in families:
        # Check if any parents in family are missing their individual row
        fam = recover_missing_individuals(fam)

        family_metadata = fam['family_metadata']
        for individual_id, individual_data in fam['individual_data'].items():
            # Skip incomplete families
            if not individual_id:
                continue

            # Get pedfile row
            ped_writer.writerow(
                prepare_ped_row(
                    individual_id,
                    individual_data['individual_metadata'],
                    family_metadata,
                )
            )

            # get individual metadata row
            ind_writer.writerow(
                prepare_ind_row(
                    individual_id,
                    individual_data['individual_metadata'],
                    family_metadata,
                )
            )

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
        print("search_path not provided so skipping file mapping")
    else:
        # Find all fastqs in search path
        found_files = False
        for sample_id, fastq_pairs in find_fastq_pairs(
            search_path, Facility(facility), recursive=False
        ).items():
            if sample_id in samples:
                for fastq_pair in fastq_pairs:
                    row = {
                        'Individual ID': samples[sample_id],
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
