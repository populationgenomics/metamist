import csv
from collections import defaultdict

from metamist.apis import ProjectApi

papi = ProjectApi()

projects = papi.get_all_projects()
pname_to_id = {p.name: p.id for p in projects}
pid_to_name = {p.id: p.name for p in projects}

project_id_to_analysis = defaultdict(list)

with open('/Users/michael.franklin/Desktop/tmp/metamist/bad-analysis.txt') as f:
    # headers
    # analysis_id, old_project_name, actual_dataset, old_project_id

    # dict write to read csv
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        new_project_id = pname_to_id[row['actual_dataset'.strip('"')]]
        project_id_to_analysis[new_project_id].append(row['analysis_id'.strip('"')])

with open(
    '/Users/michael.franklin/Desktop/tmp/metamist/bad-analysis-fixer.sql', 'w+'
) as f:
    for project_id, analysis_ids in project_id_to_analysis.items():
        f.write(
            '# Update analysis to {}\nUPDATE analysis SET project_id = {} WHERE id IN ({})\n\n'.format(
                pid_to_name[project_id], project_id, ','.join(map(str, analysis_ids))
            )
        )
