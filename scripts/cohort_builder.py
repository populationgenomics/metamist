import argparse
import ast

from metamist.apis import CohortApi

capi = CohortApi()

def main(projects, sequencing_groups, sequencing_groups_meta):
    print("Projects:", projects)
    print("Sequencing Groups:", sequencing_groups)
    print("Sequencing Groups Meta:", sequencing_groups_meta)

    #1. Get SG's for the cohort
    #Replace this with a graphql query. 
    sgs = capi.get_sgs_for_cohort(projects)
    print(sgs)
    #2. Create cohort
    cohort_id = capi.create_cohort(project='vb-fewgenomes', cohort_name='second_test', description='a second test', request_body=sgs)
    print(cohort_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Example script with argparse")

    parser.add_argument(
        "--projects",
        nargs="+",
        help="List of project names",
        required=False,
    )
    parser.add_argument(
        "--sequencing-groups",
        nargs="+",
        help="List of sequencing group names",
        required=False,
    )
    parser.add_argument(
        "--sequencing-groups-meta",
        type=ast.literal_eval,
        help="Dictionary containing sequencing group metadata",
        required=False,
    )

    args = parser.parse_args()

    projects = args.projects
    sequencing_groups = args.sequencing_groups
    sequencing_groups_meta = args.sequencing_groups_meta

    main(projects, sequencing_groups, sequencing_groups_meta)
