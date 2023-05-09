""" A simple script to import metadata files for RD projects """

from pathlib import Path
import sys

import click
import pandas as pd
from sample_metadata.apis import FamilyApi, ImportApi


def save_xlsx_as_csv(filepath: str):
    """Uses pandas to resave a .xlsx file as a csv with identical path"""
    # Open the xlsx with pandas
    read_file = pd.read_excel(filepath)

    # Write the excel to an identical csv
    filepath = filepath[: len(filepath) - 4] + 'csv'
    read_file.to_csv(filepath, index=None, header=True, encoding='utf-8-sig')

    return filepath


@click.command()
@click.option('--project', 'project', required=True)
@click.option('--ped-file', 'ped_file_path')
@click.option('--individual-metadata', 'individual_metadata')
@click.option('--family-metadata', 'family_metadata')
def main(
    project: str,
    ped_file_path: str = '',
    individual_metadata: str = '',
    family_metadata: str = '',
):
    """
    A simple metamist API wrapper for local imports of pedigree, individual and
    family metadata files.
    Converts .xlsx files to csv and saves them to the original directory.
    """

    if not any([ped_file_path, individual_metadata, family_metadata]):
        print('Please provide at least one metadata file to import', file=sys.stderr)
        sys.exit(1)

    if ped_file_path:
        if ped_file_path.endswith('xlsx'):
            ped_file_path = save_xlsx_as_csv(ped_file_path)
            print(f'Converted pedigree excel to csv.')

        print(f'processing pedigree file: {ped_file_path}')
        with Path(ped_file_path).open() as ped_file:
            response = FamilyApi().import_pedigree(
                file=ped_file,
                has_header=True,
                project=project,
                create_missing_participants=True,
            )
            print('API response:', response)

    if individual_metadata:
        if individual_metadata.endswith('xlsx'):
            individual_metadata = save_xlsx_as_csv(individual_metadata)
            print(f'Converted individual_metadata excel to csv.')

        print(f'\nProcessing individual_metadata file: {individual_metadata}')
        with Path(individual_metadata).open() as individual_m_file:
            response = ImportApi().import_individual_metadata_manifest(
                file=individual_m_file,
                project=project,
            )
            print('API response:', response)

    if family_metadata:
        if family_metadata.endswith('xlsx'):
            family_metadata = save_xlsx_as_csv(family_metadata)
            print(f'Converted family_metadata excel to csv.')

        print(f'\nProcessing family_metadata file: {family_metadata}')
        with Path(family_metadata).open() as fam_m_file:
            response = FamilyApi().import_families(
                file=fam_m_file, project=project, has_header=True
            )
            print('API response:', response)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
