# import os
import click

# import sample_metadata

# from typing import Set


@click.command()
@click.argument('gs_dir')
def main():
    """Main from CLI"""
    # projects = get_all_projects()


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
