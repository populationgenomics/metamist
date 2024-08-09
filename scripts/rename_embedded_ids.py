#!/usr/bin/env python3
"""
Copies CRAM files and associated .crai index files to URLs based on new IDs,
also replacing SM:{id} values on the CRAM files' @RG header lines.
Each command-line argument is

    OLDURL[,OLDID],NEWID

If OLDID is not specified, a pattern matching /CPGdd...dd/ is used instead
to match CPG-style IDs in the URL and @RG SM fields.

This script should usually be run via analysis-runner. It requires an image
that contains samtools.
"""

import logging
import re
import subprocess
import sys

logger = logging.getLogger(__file__)
logging.basicConfig(format='%(levelname)s (%(name)s %(lineno)s): %(message)s')
logger.setLevel(logging.INFO)


def reheader_cram(old_url: str, old_id: str, new_id: str):
    """
    Rewrites @RG headers to include SM:{new_id} and writes the resulting CRAM output
    to {old_url} with ids within the URL similarly replaced by {new_id}. Also writes
    a corresponding .crai index file alongside the CRAM output.
    """
    new_url = re.sub(old_id, new_id, old_url)
    if new_url == old_url:
        raise ValueError(f'Existing url {old_url} does not contain {old_id}')

    # Because we're streaming we don't use in-place reheadering so we need to regenerate
    # the .crai index file, not just copy it. Do this via process substitution so we don't
    # need to reread the CRAM data from the bucket.
    command = f"""
        gsutil cp {old_url} - |
        samtools reheader --no-PG -c 'sed /^@RG/s/SM:{old_id}/SM:{new_id}/g' /dev/stdin |
        tee >(samtools index -o - - | gsutil cp - {new_url}.crai) |
        gsutil cp - {new_url}
        """
    logger.info(command.replace('\n', ' '))
    subprocess.run(command, shell=True, executable='/bin/bash', check=True)
    logger.info(f'Rewrote {old_url} to {new_url}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)

    for arg in sys.argv[1:]:
        field = arg.split(',')
        if len(field) >= 3:
            reheader_cram(field[0], field[1], field[2])
        else:
            reheader_cram(field[0], 'CPG[0-9]*', field[1])
