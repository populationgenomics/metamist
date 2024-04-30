""" A script to create a custom cohort """

import argparse

from metamist.apis import CohortApi
from metamist.models import CohortBody, CohortCriteria


def main(
    project: str,
    cohort_body_spec: CohortBody,
    projects: list[str] | None = None,
    sg_ids_internal: list[str] | None = None,
    excluded_sg_ids: list[str] | None = None,
    sg_technologies: list[str] | None = None,
    sg_platforms: list[str] | None = None,
    sg_types: list[str] | None = None,
    sample_types: list[str] | None = None,
    dry_run: bool = False,
):
    """Create a custom cohort"""

    capi = CohortApi()
    cohort_criteria = CohortCriteria(
        projects=projects or [],
        sg_ids_internal=sg_ids_internal or [],
        excluded_sgs_internal=excluded_sg_ids or [],
        sg_technology=sg_technologies or [],
        sg_platform=sg_platforms or [],
        sg_type=sg_types or [],
        sample_type=sample_types or [],
    )

    cohort = capi.create_cohort_from_criteria(
        project=project,
        body_create_cohort_from_criteria={
            'cohort_spec': cohort_body_spec,
            'cohort_criteria': cohort_criteria,
            'dry_run': dry_run,
        },
    )

    print(f'Awesome! You have created a custom cohort {cohort}')
    return cohort


def get_cohort_spec(
    cohort_name: str | None, cohort_description: str | None, cohort_template_id: str | None
) -> CohortBody:
    """Get the cohort spec"""

    cohort_body_spec: dict[str, str] = {}

    if cohort_name:
        cohort_body_spec['name'] = cohort_name
    if cohort_description:
        cohort_body_spec['description'] = cohort_description
    if cohort_template_id:
        cohort_body_spec['template_id'] = cohort_template_id

    return CohortBody(**cohort_body_spec)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create a custom cohort')
    parser.add_argument(
        '--project', type=str, help='The project to create the cohort in'
    )
    parser.add_argument('--name', type=str, help='The name of the cohort')
    parser.add_argument('--description', type=str, help='The description of the cohort')
    parser.add_argument(
        '--template_id', required=False, type=str, help='The template id of the cohort'
    )
    parser.add_argument(
        '--projects',
        required=False,
        type=str,
        nargs='*',
        help='Pull sequencing groups from these projects',
    )
    parser.add_argument(
        '--sg_ids_internal',
        required=False,
        type=list[str],
        help='Include the following sequencing groups',
    )
    parser.add_argument(
        '--excluded_sgs_internal',
        required=False,
        type=list[str],
        help='Exclude the following sequencing groups',
    )
    parser.add_argument(
        '--sg_technology',
        required=False,
        type=list[str],
        help='Sequencing group technologies',
    )
    parser.add_argument(
        '--sg_platform',
        required=False,
        type=list[str],
        help='Sequencing group platforms',
    )
    parser.add_argument(
        '--sg_type',
        required=False,
        type=list[str],
        help='Sequencing group types, e.g. exome, genome',
    )
    parser.add_argument(
        '--sample_type', required=False, type=list[str], help='sample type'
    )
    parser.add_argument('--dry-run', '--dry_run', action='store_true', help='Dry run mode')

    args = parser.parse_args()

    cohort_spec = get_cohort_spec(
        cohort_name=args.name,
        cohort_description=args.description,
        cohort_template_id=args.template_id,
    )

    if not args.projects and not args.template_id:
        raise ValueError('You must provide either projects or a template id')

    main(
        project=args.project,
        cohort_body_spec=cohort_spec,
        projects=args.projects,
        sg_ids_internal=args.sg_ids_internal,
        excluded_sg_ids=args.excluded_sgs_internal,
        sg_technologies=args.sg_technology,
        sg_platforms=args.sg_platform,
        sg_types=args.sg_type,
        sample_types=args.sample_type,
        dry_run=args.dry_run,
    )
