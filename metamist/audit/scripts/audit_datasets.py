"""
Runs the upload bucket auditor for the specified datasets.
"""

from argparse import ArgumentParser
from datetime import datetime
import sys
from metamist.apis import ProjectApi
from analysis_runner.cli_analysisrunner import (
    run_analysis_runner,  # pylint: disable=import-error
)


def cli_main():
    """
    Run the upload bucket audit on the specified datasets using analysis-runner.
    """
    parser = ArgumentParser()
    parser.add_argument(
        '--test', action='store_true', help='Test mode with limited datasets'
    )
    parser.add_argument(
        '--dataset', help='Dataset to run the audit on', action='append', required=True
    )
    parser.add_argument(
        '--sequencing-types',
        help='Sequencing types to include',
        action='append',
    )
    parser.add_argument(
        '--sequencing-technologies',
        help='Sequencing technologies to include',
        action='append',
    )
    parser.add_argument(
        '--sequencing-platforms',
        help='Sequencing platforms to include',
        action='append',
    )
    parser.add_argument(
        '--analysis-types', help='Analysis types to include', action='append'
    )
    parser.add_argument('--file-types', help='File types to include', action='append')
    parser.add_argument(
        '--excluded-prefixes', help='Prefixes to exclude', action='append'
    )
    parser.add_argument('--results-folder', help='Results folder name', default=None)
    parser.add_argument('--config', help='Path to config .toml file', default=None)
    args = parser.parse_args()

    available_datasets = ProjectApi().get_my_projects()
    datasets = [ds for ds in args.dataset if ds in available_datasets]
    invalid_datasets = set(args.dataset) - set(datasets)
    if invalid_datasets:
        print(
            f"Invalid datasets specified: {', '.join(invalid_datasets)}",
            file=sys.stderr,
        )
        sys.exit(1)

    results_base = args.results_folder or datetime.now().strftime('%Y-%m-%d_%H%M%S')

    for dataset in datasets:
        script = build_command(args, dataset)
        run_analysis_runner(
            dataset=dataset,
            access_level='test' if args.test else 'full',
            config=args.config,
            output_dir=results_base,
            description='Upload Bucket Audit',
            script=script,
        )


def build_command(args, dataset: str) -> list[str]:
    """
    Build the command to run the upload bucket audit for a specific dataset.
    """
    command = [
        'python ./metamist/audit/cli/upload_bucket_audit.py',
        '--dataset',
        dataset,
    ]

    if args.sequencing_types:
        for st in args.sequencing_types:
            command.extend(['--sequencing-types', st])

    if args.sequencing_technologies:
        for st in args.sequencing_technologies:
            command.extend(['--sequencing-technologies', st])

    if args.sequencing_platforms:
        for sp in args.sequencing_platforms:
            command.extend(['--sequencing-platforms', sp])

    if args.analysis_types:
        for at in args.analysis_types:
            command.extend(['--analysis-types', at])

    if args.file_types:
        for ft in args.file_types:
            command.extend(['--file-types', ft])

    if args.excluded_prefixes:
        for ep in args.excluded_prefixes:
            command.extend(['--excluded-prefixes', ep])

    if args.results_folder:
        command.extend(['--results-folder', args.results_folder])

    return command


if __name__ == '__main__':
    cli_main()
