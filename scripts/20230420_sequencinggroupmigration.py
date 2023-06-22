# pylint: disable=missing-function-docstring,too-many-locals
"""
This script performs the major migration to using sequencing groups!
Importantly, this upgrade changes the regular sample "CPG" IDs to be
the sequencingroup IDs, and samples will get a new ID (XPG) with a
different offset.

So effectively, at the end of this script, only samples with more
than one type of sequencing will require some renaming.

It's easier for this script to directly run on the database, so this
script is not intended to be run via the normal upgrade process.

This script will:

- Split sequences into assays (one read per assay)
- Create sequencing groups for each sample for each sequencing type
    grouping the sequences
- Log the list of samples that need to be renamed
- Migrate analysis_sample to analysis_sequencing_group, making
    a GUESS where there are multiple sequencing groups.

Noting, this script WILL modify the database, it's not easy to generate
a list of SQL statements to run, because the script requires inserted IDS.
"""
import asyncio
import json
from collections import defaultdict
from textwrap import dedent
from typing import Any, List, Dict, Tuple

import click
from databases import Database

SEQTYPE_ORDER = ('genome', 'exome', 'mtseq', 'transcriptome', 'chip')

# just to improve type annotation below
SampleId = int
SequenceGroupId = int
AssayId = int
SequenceType = str


def _get_connection_string():
    # pylint: disable=import-outside-toplevel
    from db.python.connect import CredentialedDatabaseConfiguration

    config = CredentialedDatabaseConfiguration(dbname='sm_dev', username='root')
    return config.get_connection_string()


def check_number_of_renames(connection):
    query = dedent(
        """
SELECT p.name, s.id, ss.type
FROM (
    SELECT sample.id, sample.project
    FROM sample
    INNER JOIN sample_sequencing ss ON sample.id = ss.sample_id
    GROUP BY sample.id
    HAVING COUNT(*) > 1
) as s
INNER JOIN sample_sequencing ss ON s.id = ss.sample_id
INNER JOIN project p ON s.project = p.id
WHERE p.name NOT LIKE '%-test';
    """
    )
    rows = connection.fetch_all(query)
    print('Samples that need to be renamed:')
    for row in rows:
        print(f'  {row["name"]} - {row["id"]} - {row["type"]}')


def get_platform_from_technology(technology: str) -> str:
    if technology == 'short-read':
        return 'illumina'
    if technology == 'long-read':
        return 'oxford-nanopore'
    if technology == 'bulk-rna-seq':
        return 'illumina'
    if technology == 'single-cell-rna-seq':
        return 'illumina'
    raise ValueError(f'Unknown technology: {technology}')


async def check_assay_types_before_starting(connection: Database):
    query = 'SELECT DISTINCT(type) FROM sample_sequencing'
    rows = await connection.fetch_all(query)
    types = set(row['type'] for row in rows)
    missing_types = types - set(SEQTYPE_ORDER)
    if missing_types:
        raise ValueError('Missing sequencing types: ' + ', '.join(missing_types))
    return True


async def mutate_fetch_one(connection, query, values, dry_run):
    if dry_run:
        print(f'Running query: {query} with values: {values}')
    else:
        return await connection.fetch_one(query, values)


async def execute_many(connection, query, inserts, dry_run):
    if dry_run:
        print(f'Inserting {len(inserts)} assays with query: {query}')
    else:
        await connection.execute_many(query, inserts)


async def migrate_sequences_to_assays(connection: Database, dry_run=True):
    """
    Split sequences into assays (one read per assay)
    """
    # Get all the sequences
    has_assays = (await connection.fetch_val('SELECT COUNT(*) FROM assay')) > 0
    if has_assays:
        print('Assays already exist, skipping')
        return

    sequences = await connection.fetch_all('SELECT * FROM sample_sequencing')
    inserts = []

    for row in sequences:
        meta = json.loads(row['meta'])
        technology = row['technology'] or 'short-read'

        seq_meta_values = {
            'sequencing_type': row['type'],
            'sequencing_technology': technology,
            'sequencing_platform': get_platform_from_technology(technology),
        }
        reads = meta.get('reads')
        if reads:
            # split into multiple assays
            for read in reads:
                inserts.append(
                    {
                        'sample_id': row['sample_id'],
                        'meta': json.dumps(
                            {
                                **meta,
                                **seq_meta_values,
                                'reads': read,
                                # we've dropped the assay status, but keep it in meta for now
                                'status': row['status'],
                            }
                        ),
                        'type': 'sequencing',
                        'author': row['author'],
                    }
                )
        else:
            # no reads, just insert as is
            inserts.append(
                {
                    'sample_id': row['sample_id'],
                    'meta': json.dumps(
                        {
                            **meta,
                            **seq_meta_values,
                        }
                    ),
                    'type': 'sequencing',
                    'author': row['author'],
                }
            )

    query = dedent(
        """
        INSERT INTO assay (sample_id, meta, type, author)
        VALUES (:sample_id, :meta, :type, :author)
        """
    )

    await execute_many(connection, query, inserts, dry_run)


async def get_sequencing_eids_by_sample_type(
    connection: Database,
) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    query = dedent(
        """
    SELECT seq_eid.project, seq.type, seq.sample_id, seq_eid.external_id, seq_eid.name
    FROM sample_sequencing_eid seq_eid
    INNER JOIN sample_sequencing seq
        ON seq_eid.sequencing_id = seq.id
     """
    )
    rows = await connection.fetch_all(query)

    sequencing_eids_by_sample_type: Dict[Tuple[str, str], list] = defaultdict(list)
    for row in rows:
        sample_id = row['sample_id']
        seq_type = row['type']
        sequencing_eids_by_sample_type[(sample_id, seq_type)].append(
            {
                'project': row['project'],
                'external_id': row['external_id'],
                'name': row['name'],
            }
        )

    return sequencing_eids_by_sample_type


async def create_sequencing_groups(connection: Database, author: str, dry_run=True):
    assays = await connection.fetch_all('SELECT * FROM assay')
    # we can't safely put them on assays, so let's put it on the sequencing-group
    seq_eids = await get_sequencing_eids_by_sample_type(connection)

    # group assays by sample, then by sequencing type
    grouped_assays: Dict[SampleId, Dict[SequenceType, List[dict]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for assay in assays:
        sample_id = assay['sample_id']
        assay_meta = json.loads(assay['meta'])
        seq_type: SequenceType = assay_meta['sequencing_type']
        grouped_assays[sample_id][seq_type].append(dict(assay))

    seq_groups_to_insert: Dict[SequenceGroupId, Dict[str, Any]] = {}
    seq_group_assays_to_insert: Dict[SequenceGroupId, List[AssayId]] = defaultdict(list)

    # sequenceGroup, [assayIDs]
    seq_groups_to_insert_later: List[Tuple[dict, List[AssayId]]] = []
    sample_seqtype_to_sg_id = {}

    for sample_id, sample_assays in grouped_assays.items():
        for seqtype in SEQTYPE_ORDER:
            if seqtype not in sample_assays:
                continue
            sample_seqgroup_assays = sample_assays[seqtype]
            sample_seqgroup_assay_ids = [a['id'] for a in sample_seqgroup_assays]

            assay_meta = json.loads(sample_seqgroup_assays[0]['meta'])
            technology = assay_meta['sequencing_technology']
            seqgroup = {
                'sample_id': sample_id,
                'type': seqtype,
                'technology': technology,
                'platform': get_platform_from_technology(technology),
                'meta': '{}',
                'archived': False,
                'author': author,
            }

            if sample_id in seq_groups_to_insert:
                # we've already inserted a sequencing group for this sample so,
                # we'll generate a new ID for it, and insert it later
                seq_groups_to_insert_later.append((seqgroup, sample_seqgroup_assay_ids))
            else:
                seq_groups_to_insert[sample_id] = {
                    # this is the crucial bit, we're ensuring the
                    # sequencing group ID and sample ID match
                    'id': sample_id,
                    **seqgroup,
                }
                sample_seqtype_to_sg_id[(sample_id, seqtype)] = sample_id
                seq_group_assays_to_insert[sample_id] = sample_seqgroup_assay_ids

    # insert sequencing groups
    seq_group_insert_query = dedent(
        """
        INSERT INTO sequencing_group (id, sample_id, type, technology, platform, meta, archived, author)
        VALUES (:id, :sample_id, :type, :technology, :platform, :meta, :archived, :author)
        """
    )
    await execute_many(
        connection, seq_group_insert_query, seq_groups_to_insert.values(), dry_run
    )

    # insert sequencing groups that we need to generate IDs for
    seq_group_insert_returning_id_query = dedent(
        """
        INSERT INTO sequencing_group (sample_id, type, technology, platform, meta, archived, author)
        VALUES (:sample_id, :type, :technology, :platform, :meta, :archived, :author)
        RETURNING id
        """
    )
    for seqgroup, assay_ids in seq_groups_to_insert_later:
        # need to do these one at a time to get the returned ID
        new_seqgroup = await mutate_fetch_one(
            connection, seq_group_insert_returning_id_query, seqgroup, dry_run
        )
        sample_id = seqgroup['sample_id']
        seqtype = seqgroup['type']
        sample_seqtype_to_sg_id[(sample_id, seqtype)] = new_seqgroup['id']
        seq_group_assays_to_insert[new_seqgroup['id']] = assay_ids

    seq_group_assay_insert_query = dedent(
        """
        INSERT INTO sequencing_group_assay (sequencing_group_id, assay_id, author)
        VALUES (:sequencing_group_id, :assay_id, :author)
        """
    )
    prepared_assay_insert_values = [
        {'sequencing_group_id': seqgroupid, 'assay_id': aid, 'author': author}
        for seqgroupid, assayids in seq_group_assays_to_insert.items()
        for aid in assayids
    ]
    await execute_many(
        connection, seq_group_assay_insert_query, prepared_assay_insert_values, dry_run
    )

    sg_eids_to_insert = []
    sg_eid_to_insert_query = dedent(
        """
    INSERT INTO sequencing_group_external_id
        (project, sequencing_group_id, external_id, name, author)
    VALUES (:project, :sequencing_group_id, :external_id, :name, :author)
    """
    )
    for (sid, seqtype), seqs in seq_eids.items():
        for seq_eid in seqs:
            sg_eids_to_insert.append(
                {
                    'project': seq_eid['project'],
                    'sequencing_group_id': sample_seqtype_to_sg_id[(sid, seqtype)],
                    'external_id': seq_eid['external_id'],
                    'name': seq_eid['name'],
                    'author': author,
                }
            )

    await execute_many(connection, sg_eid_to_insert_query, sg_eids_to_insert, dry_run)


async def migrate_analyses(connection: Database, dry_run: bool = True):
    """
    Migrate analyses to new format
    """
    analyses_query = dedent(
        """
SELECT a.id, a_s.sample_id, a.meta
FROM analysis a
INNER JOIN analysis_sample a_s ON a.id = a_s.analysis_id
    """
    )
    analysis_samples = await connection.fetch_all(analyses_query)

    sequence_group_ids_of_duplicate_samples_query = dedent(
        """
SELECT sg.sample_id, sg.id, sg.type
FROM sequencing_group sg
INNER JOIN (
  SELECT sample_id
  FROM sequencing_group
  GROUP BY sample_id
  HAVING COUNT(*) > 1
) duplicates
ON sg.sample_id = duplicates.sample_id
ORDER BY sg.sample_id DESC;
    """
    )
    sequence_group_ids_of_duplicate_samples = await connection.fetch_all(
        sequence_group_ids_of_duplicate_samples_query
    )
    duplicate_sg_id_map: Dict[
        SampleId, Dict[SequenceType, SequenceGroupId]
    ] = defaultdict(dict)
    for row in sequence_group_ids_of_duplicate_samples:
        duplicate_sg_id_map[row['sample_id']][row['type']] = row['id']

    values_to_insert: List[Tuple[int, SequenceGroupId]] = []
    potential_issues: List[Tuple[int, SequenceGroupId]] = []
    for analysis in analysis_samples:
        analysis_id = analysis['id']
        sample_id = analysis['sample_id']

        if sample_id not in duplicate_sg_id_map:
            # easy, the IDs will be the same!
            values_to_insert.append((analysis_id, sample_id))
        else:
            # we need to find the sequencing group ID for this analysis
            # keyed by sequencing type
            potential_sg_ids = duplicate_sg_id_map[sample_id]
            meta = json.loads(analysis['meta'])
            seqtype = meta.get('sequencing_type') or meta.get('sequence_type')
            sg_id = potential_sg_ids.get(seqtype)
            if sg_id:
                values_to_insert.append((analysis_id, sg_id))
            else:
                # we can't find the sequencing group ID for this analysis
                # we'll use the order to make the best guess, and we'll log it
                # so we can fix it later
                for seqtype in SEQTYPE_ORDER:
                    if seqtype in potential_sg_ids:
                        sg_id = potential_sg_ids[seqtype]
                        values_to_insert.append((analysis_id, sg_id))
                        potential_issues.append((analysis_id, sg_id))
                        break

    if potential_issues:
        print('Potential analysis remap issues:')
        for analysis_id, sg_id in potential_issues:
            print(f'\tAnalysis {analysis_id} -> SG {sg_id} (may be incorrect)')

    insert_query = dedent(
        """
INSERT INTO analysis_sequencing_group (analysis_id, sequencing_group_id)
VALUES (:analysis_id, :sequencing_group_id)
    """
    )
    remapped_values = [
        {'analysis_id': analysis_id, 'sequencing_group_id': sg_id}
        for analysis_id, sg_id in values_to_insert
    ]
    sg_ids = set(a['sequencing_group_id'] for a in remapped_values)
    sg_ids_inserted_rows = await connection.fetch_all('SELECT id FROM sequencing_group')
    sg_ids_inserted = set(a['id'] for a in sg_ids_inserted_rows)
    bad_sg_ids = sg_ids - sg_ids_inserted
    if bad_sg_ids:
        bad_analysis = [
            a for a in remapped_values if a['sequencing_group_id'] in bad_sg_ids
        ]
        remapped_values = [
            a for a in remapped_values if a['sequencing_group_id'] not in bad_sg_ids
        ]
        print(f'Bad analysis: {bad_analysis}')
    await execute_many(connection, insert_query, remapped_values, dry_run)


@click.command()
@click.option('--dry-run/--no-dry-run', default=True)
@click.option('--connection-string', default=None)
@click.argument('author', default='sequencing-group-migration')
def main_sync(author, dry_run: bool = True, connection_string: str = None):
    """Run synchronisation"""
    asyncio.get_event_loop().run_until_complete(
        main(author, dry_run=dry_run, connection_string=connection_string)
    )


async def main(author, dry_run: bool = True, connection_string: str = None):
    """Run synchronisation"""
    connection = Database(connection_string or _get_connection_string(), echo=True)
    await connection.connect()
    async with connection.transaction():
        await check_assay_types_before_starting(connection)
        await migrate_sequences_to_assays(connection, dry_run=dry_run)
        await create_sequencing_groups(connection, author, dry_run=dry_run)
        await migrate_analyses(connection, dry_run=dry_run)
    await connection.disconnect()


if __name__ == '__main__':
    main_sync()  # pylint: disable=no-value-for-parameter
