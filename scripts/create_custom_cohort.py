"""A script to create a custom cohort"""

import argparse

from metamist.apis import CohortApi
from metamist.models import CohortBody, CohortCriteria


def main(
    project: str,
    cohort_body_spec: CohortBody,
    cohort_criteria_spec: CohortCriteria,
    dry_run: bool = False,
    exclude_ineligible_sg_ids_internal: bool = False,
):
    """Create a custom cohort"""

    capi = CohortApi()

    cohort = capi.create_cohort_from_criteria(
        project=project,
        body_create_cohort_from_criteria={
            'cohort_spec': cohort_body_spec,
            'cohort_criteria': cohort_criteria_spec,
        },
        dry_run=dry_run,
        exclude_ineligible_sg_ids_internal=exclude_ineligible_sg_ids_internal,
    )

    print(f'Awesome! You have created a custom cohort {cohort}')
    return cohort


def get_cohort_spec(
    cohort_name: str | None,
    cohort_description: str | None,
    cohort_template_id: str | None,
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
        nargs='*',
        help='Include the following sequencing groups',
    )
    parser.add_argument(
        '--excluded_sgs_internal',
        required=False,
        nargs='*',
        help='Exclude the following sequencing groups',
    )
    parser.add_argument(
        '--sg_technology',
        required=False,
        nargs='*',
        help='Sequencing group technologies',
    )
    parser.add_argument(
        '--sg_platform',
        required=False,
        nargs='*',
        help='Sequencing group platforms',
    )
    parser.add_argument(
        '--sg_type',
        required=False,
        nargs='*',
        help='Sequencing group types, e.g. exome, genome',
    )
    parser.add_argument('--sample_type', required=False, nargs='*', help='sample type')
    parser.add_argument(
        '--dry-run', '--dry_run', action='store_true', help='Dry run mode'
    )

    parser.add_argument(
        '--exclude_ineligible_sg_ids_internal',
        '--exclude_ineligible_sg_ids_internal',
        action='store_true',
        help='Exclude Ineligible sequencing groups',
    )
    args = parser.parse_args()

    cohort_spec = get_cohort_spec(
        cohort_name=args.name,
        cohort_description=args.description,
        cohort_template_id=args.template_id,
    )

    if not (args.sg_ids_internal or args.projects or args.template_id):
        raise ValueError('You must provide sg_ids_internal, projects, or a template_id')

    cohort_criteria = CohortCriteria(
        projects=args.projects or [],
        sg_ids_internal=args.sg_ids_internal or [],
        excluded_sgs_internal=args.excluded_sgs_internal or [],
        sg_technology=args.sg_technology or [],
        sg_platform=args.sg_platform or [],
        sg_type=args.sg_type or [],
        sample_type=args.sample_type or [],
    )

    main(
        project=args.project,
        cohort_body_spec=cohort_spec,
        cohort_criteria_spec=cohort_criteria,
        dry_run=args.dry_run,
        exclude_ineligible_sg_ids_internal=args.exclude_ineligible_sg_ids_internal,
    )
