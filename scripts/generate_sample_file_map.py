"""
    Given a map of extIDs - Facility Seq IDs find all matching fastq-pairs
    in a bucket path and print a sample map file ready for ingest.

 """

import csv
import sys
import click

import pandas as pd
from cloudpathlib import CloudPath

from redcap_parsing_utils import Facility, find_fastq_pairs, FILEMAP_HEADERS


@click.command()
@click.option('-i', '--id-map-path', required=True)
@click.option('-p', '--search-path', required=True)
@click.option('-s', '--sample_id_column', default='Sample ID')
@click.option('-g', '--garvan', is_flag=True)
def main(search_path: str, id_map_path: str, garvan: bool, sample_id_column):
    """
    Helper script to preapare a sample file map for ingest.

    Given a map of extIDs -> Facility Seq IDs, finds all fastq-pairs for
    each sample id in the nominated bucket. Writes a sample map file ready
    metamist for ingest.

    Currently only works for the following seq facilities:
    - VCGS [default]:
        - Modern VCGS file name format only (>= ~2018)
        - Detects genome, exome and RNA types

    - Garvan/KCCG
        - Assumes WGS type

    """
    if garvan:
        facility = Facility.GARVAN
    else:
        facility = Facility.VCGS

    # Find all fastq pairs
    search_dir = CloudPath(search_path)
    assert search_dir.is_dir()

    read_pairs_by_sample_id = find_fastq_pairs(search_path, facility)

    # Read id map - first convert xlsx to csv
    if id_map_path.endswith('xlsx'):
        # Open an xlsx with pandas
        read_file = pd.read_excel(id_map_path)
        # Write the excel to an identical csv
        id_map_path = id_map_path[: len(id_map_path) - 4] + 'csv'
        read_file.to_csv(id_map_path, index=None, header=True, encoding='utf-8-sig')

    with open(id_map_path, encoding='utf-8-sig') as id_map:
        reader = csv.DictReader(id_map)
        writer = csv.DictWriter(sys.stdout, fieldnames=FILEMAP_HEADERS, delimiter=',')
        writer.writeheader()

        sample_ids_found = set()
        sample_ids_in_manifest = set()
        for line in reader:
            sample_ids_in_manifest.add(line[sample_id_column])
            for pair in read_pairs_by_sample_id[line[sample_id_column]]:
                writer.writerow(
                    {
                        'Individual ID': line['Individual ID'],
                        'Sample ID': line[sample_id_column],
                        'Filenames': ','.join(sorted([x.path.name for x in pair])),
                        'Type': pair[0].seq_type.value,
                    }
                )
                sample_ids_found.add(line[sample_id_column])
    
    if no_match_samples := sample_ids_in_manifest - sample_ids_found:
        raise ValueError(f'Found matches for {len(sample_ids_found)}/{len(sample_ids_in_manifest)}. No matches found for {len(no_match_samples)} samples: {no_match_samples}')


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
