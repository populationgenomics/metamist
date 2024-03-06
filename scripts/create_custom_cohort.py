""" A script to create a custom cohort """
import argparse

from metamist.apis import CohortApi
from metamist.models import CohortBody, CohortCriteria


def main(
    project: str,
    cohort_body_spec: CohortBody,
    projects: list[str],
    sg_ids_internal: list[str],
    excluded_sg_ids: list[str],
    sg_technologies: list[str],
    sg_platforms: list[str],
    sg_types: list[str],
    dry_run: bool = False
):
    """ Create a custom cohort"""

    capi = CohortApi()
    cohort_criteria = CohortCriteria(
        projects=projects or [],
        sg_ids_internal=sg_ids_internal or [],
        excluded_sgs_internal=excluded_sg_ids or [],
        sg_technology=sg_technologies or [],
        sg_platform=sg_platforms or [],
        sg_type=sg_types or []
    )

    cohort = capi.create_cohort_from_criteria(
        project=project,
        body_create_cohort_from_criteria={'cohort_spec': cohort_body_spec, 'cohort_criteria': cohort_criteria, 'dry_run': dry_run}
    )

    print(f'Awesome! You have created a custom cohort with id {cohort["cohort_id"]}')


def get_cohort_spec(
    cohort_name: str,
    cohort_description: str,
    cohort_template_id: int
) -> CohortBody:
    """ Get the cohort spec """

    cohort_body_spec: dict[str, int | str] = {}

    if cohort_name:
        cohort_body_spec['name'] = cohort_name
    if cohort_description:
        cohort_body_spec['description'] = cohort_description
    if cohort_template_id:
        cohort_body_spec['derived_from'] = cohort_template_id

    return CohortBody(**cohort_body_spec)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create a custom cohort')
    parser.add_argument('--project', type=str, help='The project to create the cohort in')
    parser.add_argument('--name', type=str, help='The name of the cohort')
    parser.add_argument('--description', type=str, help='The description of the cohort')
    parser.add_argument('--template_id', required=False, type=int, help='The template id of the cohort')
    parser.add_argument('--projects', required=False, type=str, nargs='*', help='Pull sequencing groups from these projects')
    parser.add_argument('--sg_ids_internal', required=False, type=list[str], help='Include the following sequencing groups')
    parser.add_argument('--excluded_sgs_internal', required=False, type=list[str], help='Exclude the following sequencing groups')
    parser.add_argument('--sg_technology', required=False, type=list[str], help='Sequencing group technologies')
    parser.add_argument('--sg_platform', required=False, type=list[str], help='Sequencing group platforms')
    parser.add_argument('--sg_type', required=False, type=list[str], help='Sequencing group types, e.g. exome, genome')
    parser.add_argument('--dry_run', required=False, type=bool, help='Dry run mode')

    args = parser.parse_args()

    cohort_spec = get_cohort_spec(
        cohort_name=args.name,
        cohort_description=args.description,
        cohort_template_id=args.template_id
    )

    main(
        project=args.project,
        cohort_body_spec=cohort_spec,
        projects=args.projects,
        sg_ids_internal=args.sg_ids_internal,
        excluded_sg_ids=args.excluded_sgs_internal,
        sg_technologies=args.sg_technology,
        sg_platforms=args.sg_platform,
        sg_types=args.sg_type,
        dry_run=args.dry_run
    )
