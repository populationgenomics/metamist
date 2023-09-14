""" A really simple script to import a pedigree file """

import click
from cloudpathlib import AnyPath

from metamist.apis import (
    FamilyApi,
)


@click.command()
@click.option('--ped-file-path', 'ped_file_path')
@click.option('--project', 'project')
def main(ped_file_path: str, project: str):
    """Pull ped file from GCS and import to SM"""

    fapi = FamilyApi()

    with AnyPath(ped_file_path).open() as ped_file:  # type: ignore
        fapi.import_pedigree(
            file=ped_file,
            has_header=True,
            project=project,
            create_missing_participants=True,
        )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
