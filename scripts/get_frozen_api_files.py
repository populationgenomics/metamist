#!/usr/bin/env python
# pylint: disable=no-member,consider-using-with

"""
Download frozen api client files from the metamist-legacy-client branch where they are
committed

usage:

    python scripts/get_frozen_api_files.py <ref>

"""
import argparse
import glob
import os
import shutil
import tempfile
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

FILE_GLOBS = [
    'metamist/api/*.py',
    'metamist/apis/*.py',
    'metamist/model/*.py',
    'metamist/models/*.py',
    'metamist/*.py',
    'web/src/static/sm_docs/*.md',
]


def main(args=None):
    """Main function, parses sys.argv"""

    parser = argparse.ArgumentParser('Get frozen api client files')

    parser.add_argument('ref')
    parsed_args = parser.parse_args(args)

    ref = parsed_args.ref
    url = f'https://github.com/populationgenomics/metamist/archive/{ref}.zip'

    tmpdir = tempfile.mkdtemp()

    with urlopen(url) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            zfile.extractall(tmpdir)

    unzipped_folder = os.listdir(tmpdir)[0]
    frozen_metamist = os.path.join(tmpdir, unzipped_folder)
    this_dir = os.path.dirname(os.path.realpath(__file__))

    for glob_path in FILE_GLOBS:
        # remove existing files
        for file in glob.glob(os.path.abspath(os.path.join(this_dir, '..', glob_path))):
            print(f'removing existing {file}')
            os.remove(file)

        for file in glob.glob(os.path.join(frozen_metamist, glob_path)):
            dest_rel = os.path.relpath(file, frozen_metamist)
            dest = os.path.abspath(os.path.join(this_dir, '..', dest_rel))
            print(f'copying {dest_rel}')
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy(file, dest)

    shutil.rmtree(tmpdir)


if __name__ == '__main__':
    main()
