from collections import namedtuple
import math
import os
from shlex import quote

import click
import hailtop.batch as hb
from cpg_utils.hail_batch import remote_tmpdir
from cpg_utils.config import get_config

from sample_metadata.apis import SampleApi, AssayApi

LocationTuple = namedtuple(
    'LocationTuple', ['cpg_sample_id', 'location', 'checksum', 'size']
)


def validate_samples(
    cpg_sample_ids: list[str], datasets: list[str], stream_files: bool
):
    """Validate files from internal CPG sample IDs"""

    if cpg_sample_ids and datasets:
        raise ValueError('Must only specify one of cpg_sample_ids OR datasets')

    if not cpg_sample_ids:
        if not datasets:
            raise ValueError('If not specifying cpg_sample_ids, MUST specify a dataset')

        sapi = SampleApi()
        cpg_sample_ids = []
        for dataset in datasets:
            cpg_sample_ids.extend(
                sapi.get_all_sample_id_map_by_internal(dataset).keys()
            )
    cpg_sample_ids = list(cpg_sample_ids)

    batch_config = get_config()
    driver_image = batch_config['workflow'].get(
        'driver_image',
    )

    batch_name = 'validate_md5s'
    if datasets:
        batch_name += f' ({", ".join(datasets)})'

    backend = hb.ServiceBackend(
        billing_project='seqr',
        remote_tmpdir=remote_tmpdir(),
        token=os.environ.get('HAIL_TOKEN'),
    )
    b = hb.Batch(batch_name, backend=backend)

    triples = get_file_path_md5_pairs_from_samples(cpg_sample_ids=cpg_sample_ids)

    for obj in triples:
        job = b.new_job(
            f'validate_{obj.cpg_sample_id}_{os.path.basename(obj.location)}'
        )
        job.image(driver_image)

        validate_md5(b, job, obj, stream_file=stream_files)

    b.run(wait=False)


def get_file_path_md5_pairs_from_samples(
    cpg_sample_ids: list[str],
) -> list[LocationTuple]:
    """
    From a list of cpg_sample_ids, grab its sequences and determin
    a list of:
        (cpg_sample_id, file_path, expected_md5)
    """
    seqapi = AssayApi()
    sequences = seqapi.get_assays_by_criteria(sample_ids=cpg_sample_ids)

    pairs = []
    for seq in sequences:
        meta = seq.get('meta', {})
        reads = meta.get('reads')
        if not reads:
            continue

        pairs.extend(get_location_info_tuples(seq['sample_id'], reads))

    return pairs


def get_location_info_tuples(
    cpg_sample_id: str,
    file_obj: list[dict] | dict,
) -> list[LocationTuple]:
    """
    Get the tuples of (cpg_id, filename, md5)
    from a file object (or list of file objects).
    """
    if isinstance(file_obj, list):
        pairs = []
        for o in file_obj:
            pairs.extend(get_location_info_tuples(cpg_sample_id, o))
        return pairs

    if 'checksum' not in file_obj:
        return []

    return [
        LocationTuple(
            cpg_sample_id, file_obj['location'], file_obj['checksum'], file_obj['size']
        )
    ]


def validate_md5(
    batch: hb.Batch, job: hb.batch.job, obj: LocationTuple, stream_file
) -> hb.batch.job:
    """
    This adds the command to perform a md5 checksum on a file
    and compare to its expected md5 - failing if they are not a match.
    """

    # Calculate md5 checksum.
    job.command('set -euxo pipefail')

    if stream_file:
        job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
        job.command(
            'gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS'
        )
        file_contents_command = f'gsutil cat {quote(obj.location)}'
    else:
        # hail batch file

        file_contents_command = f'cat {batch.read_input(obj.location)}'
        size_in_gb = math.ceil(1.2 * obj.size / 2**30)
        job.storage(f'{size_in_gb}Gi')

    job.command(
        f"""\
{file_contents_command} | md5sum | cut -d " " -f1 > /tmp/uploaded.md5
diff /tmp/uploaded.md5 <(echo {quote(obj.checksum)} | cut -d " " -f1)
    """
    )

    return job


@click.command()
@click.option('--dataset', multiple=True, help='Only specify cpg_sample_ids OR dataset')
@click.option(
    '--copy-files',
    is_flag=True,
    help='Load files to disk before calculating MD5 sums, might reduce some transient copy errors',
)
@click.argument('cpg_sample_ids', nargs=-1)
def main(cpg_sample_ids: list[str], dataset: list[str], copy_files: bool = False):
    """Main from CLI"""
    validate_samples(
        cpg_sample_ids=cpg_sample_ids, datasets=dataset, stream_files=not copy_files
    )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
