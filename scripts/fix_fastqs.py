import asyncio
from typing import Any, Dict

from sample_metadata.apis import SampleApi, SequenceApi, ProjectApi
from sample_metadata.model.sequence_update_model import SequenceUpdateModel
from sample_metadata.parser.generic_parser import chunk, GenericParser

sapi = SampleApi()
seqapi = SequenceApi()


async def main(project: str):
    """
    Fix fastqs for the specific project by getting all samples,
    finding any files that don't match the expected structure,
    then batching updates to the sequences.
    """

    print(f'{project} :: loading')
    try:
        resp = await sapi.get_all_sample_id_map_by_internal_async(project)
        if not resp:
            print(f'{project} :: No samples')
            return
        sequences = await seqapi.get_sequences_by_sample_ids_async(
            get_latest_sequence_only=False, request_body=list(resp.keys())
        )
    # pylint: disable=broad-except
    except Exception as e:
        print(f'{project} :: Failed to load sequences: {e}')
        return

    sequences_to_update: Dict[int, Dict[str, Any]] = {}
    for seq in sequences:
        is_affected = seq['meta']['reads_type'] == 'fastq' and any(
            len(sp) != 2 for sp in seq['meta'].get('reads', [])
        )
        if not is_affected:
            continue

        reads = seq['meta']['reads']
        reads_map = {r['location']: r for grp in reads for r in grp}

        fastq_order = GenericParser.parse_fastqs_structure(list(reads_map.keys()))

        fixed_reads = []
        for fq_grp in fastq_order:
            fixed_reads.append([reads_map[r] for r in fq_grp])

        if any(len(sp) != 2 for sp in fixed_reads):
            raise ValueError(f'An error occurred with the update of {seq["id"]}')

        sequences_to_update[seq['id']] = {'reads': fixed_reads}

    if len(sequences_to_update) == 0:
        return print(f'{project} :: no sequences to update')
    for chnk in chunk(list(sequences_to_update.items())):
        promises = []
        seq_ids = []
        for seq_id, meta in chnk:
            seq_ids.append(seq_id)
            promises.append(
                seqapi.update_sequence_async(seq_id, SequenceUpdateModel(meta=meta))
            )

        print(f'{project} :: Updating {len(promises)}: {seq_ids}')
        await asyncio.gather(*promises)

async def for_all_projects():
    """
    Fix fastqs for all of MY projects
    """
    datasets = ProjectApi().get_my_projects()
    for dataset in datasets:
        await main(dataset)


if __name__ == '__main__':
    asyncio.new_event_loop().run_until_complete(for_all_projects())
