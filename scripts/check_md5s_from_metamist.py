import os
from shlex import quote

import click
import hailtop.batch as hb
from cpg_utils.hail_batch import remote_tmpdir
from cpg_utils.config import get_config

from sample_metadata.apis import SampleApi, SequenceApi


def validate_samples(cpg_sample_ids: list[str], datasets: list[str]):
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

    for cpg_sample_id, filename, md5 in triples:
        job = b.new_job(f'validate_{cpg_sample_id}_{os.path.basename(filename)}')
        job.image(driver_image)
        validate_md5(job, filename, md5)

    b.run(wait=False)


def get_file_path_md5_pairs_from_samples(
    cpg_sample_ids: list[str],
) -> list[tuple[str, str, str]]:
    """
    From a list of cpg_sample_ids, grab its sequences and determin
    a list of:
        (cpg_sample_id, file_path, expected_md5)
    """
    seqapi = SequenceApi()
    sequences = seqapi.get_sequences_by_sample_ids(
        request_body=cpg_sample_ids, get_latest_sequence_only=False
    )

    pairs = []
    for seq in sequences:
        meta = seq.get('meta', {})
        reads = meta.get('reads')
        if not reads:
            continue

        pairs.extend(get_location_md5_pairs_from_file_obj(seq['sample_id'], reads))

    return pairs


def get_location_md5_pairs_from_file_obj(
    cpg_sample_id: str,
    file_obj: list[dict] | dict,
) -> list[tuple[str, str, str]]:
    """
    Get the tuples of (cpg_id, filename, md5)
    from a file object (or list of file objects).
    """
    if isinstance(file_obj, list):
        pairs = []
        for o in file_obj:
            pairs.extend(get_location_md5_pairs_from_file_obj(cpg_sample_id, o))
        return pairs

    if 'checksum' not in file_obj:
        return []

    return [(cpg_sample_id, file_obj['location'], file_obj['checksum'])]


def validate_md5(job: hb.batch.job, file, md5) -> hb.batch.job:
    """
    This adds the command to perform a md5 checksum on a file
    and compare to its expected md5 - failing if they are not a match.
    """

    # Calculate md5 checksum.
    job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    job.command(
        f"""\
gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
gsutil cat {quote(file)} | md5sum | cut -d " " -f1 > /tmp/uploaded.md5
diff <(cat /tmp/uploaded.md5) <(echo {quote(md5)} | cut -d " " -f1)
    """
    )

    return job


@click.command()
@click.option('--dataset', multiple=True, help='Only specify cpg_sample_ids OR dataset')
@click.argument('cpg_sample_ids', nargs=-1)
def main(cpg_sample_ids: list[str], dataset: list[str]):
    """Main from CLI"""
    validate_samples(cpg_sample_ids=cpg_sample_ids, datasets=dataset)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
