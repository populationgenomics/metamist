""" A simple script to import metadata files for RD projects """

import sys
import click
from pathlib import Path

from sample_metadata.apis import FamilyApi, ImportApi


@click.command()
@click.option('--project', 'project', required=True)
@click.option('--ped-file', 'ped_file_path')
@click.option('--individual-metadata', 'individual_metadata')
@click.option('--family-metadata', 'family_metadata')
def main(
    project: str,
    ped_file_path: str = "",
    individual_metadata: str = "",
    family_metadata: str = "",
):
    """
    A simple metamist API wrapper for local imports of pedigree, individual and
    family metadata files.
    """

    if not any([ped_file_path, individual_metadata, family_metadata]):
        print("Please provide at least one metadata file to import", file=sys.stderr)
        sys.exit(1)

    if ped_file_path:
        print(f"processing pedigree file: {ped_file_path}")
        with Path(ped_file_path).open() as ped_file:
            response = FamilyApi().import_pedigree(
                file=ped_file,
                has_header=True,
                project=project,
                create_missing_participants=True,
            )
            print('API response:', response)

    if individual_metadata:
        print(f"\nProcessing individual_metadata file: {individual_metadata}")
        with Path(individual_metadata).open() as individual_m_file:
            response = ImportApi().import_individual_metadata_manifest(
                file=individual_m_file,
                project=project,
            )
            print('API response:', response)

    if family_metadata:
        print(f"\nProcessing family_metadata file: {family_metadata}")
        with Path(family_metadata).open() as fam_m_file:
            response = FamilyApi().import_families(
                file=fam_m_file, project=project, has_header=True
            )
            print('API response:', response)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
