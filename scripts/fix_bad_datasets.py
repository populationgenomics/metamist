"""
Production-pipelines has landed a few analysis in the wrong dataset, and now that the
change has been correct (github.com/populationgenomics/production-pipelines/pull/291)
it's time to fix the analysis entries.

This script prints out the SQL to fix the mistakes,
    it doesn't actually fix the mistakes.

To run this script, you need generate the map from the database:

    mysql -u root sm_production -e '
        SELECT a.type, a.id, JSON_VALUE(a.meta, "$.dataset"), p.name
        FROM analysis a
        INNER JOIN project p ON a.project = p.id
        WHERE
            JSON_EXISTS(a.meta, "$.dataset")
            AND p.name <> JSON_VALUE(a.meta, "$.dataset")
        ;' > bad-analysis.txt

Then you can run it with:

    python3 scripts/fix_bad_datasets.py bad-analysis.txt

"""
import sys
import csv
from collections import defaultdict

from sample_metadata.apis import ProjectApi

projects = ProjectApi().get_all_projects()
project_name_map = {p.name: p.id for p in projects}


def generate_updates_for_bad_analysis_tsv(filename: str, output_filename: str = None):
    """
    Opens the TSV, groups by second column, maps the project name to the
    project id, and generates the SQL to update the analysis table
    """

    # columns: type	id	dataset	name
    interesting_stats: dict[str, int] = defaultdict(int)
    with open(filename, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        updates = defaultdict(list)
        for row in reader:
            _atype, aid, dataset, _old_project_name = row

            updates[dataset].append(aid)
            interesting_stats[f'{_old_project_name} -> {dataset}'] += 1

    updaters = []
    for dataset, analysis_ids in updates.items():
        project_id = project_name_map[dataset]
        updater = f"""
# Updating {len(analysis_ids)} analysis items in {dataset}
UPDATE analysis
    SET project = {project_id}
    WHERE
        JSON_VALUE(meta, '$.dataset') = '{dataset}'
        AND id IN ({','.join(map(str, analysis_ids))});
"""
        updaters.append(updater)

    project_swap_str = '\n '.join(f'# {k}: {v}' for k, v in interesting_stats.items())
    print('# Project swap state: ' + project_swap_str)

    if output_filename:
        with open(output_filename, 'w') as f:
            f.write(''.join(updaters))
    else:
        print(''.join(updaters))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Usage: {sys.argv[0]} <bad_analysis.tsv> [<output-filename.sql>]')
        sys.exit(1)

    generate_updates_for_bad_analysis_tsv(*sys.argv[1:])
