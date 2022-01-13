#!/usr/bin/env python
# pylint: disable=no-member
"""
Run the sample-metadata API through the analysis runner
in a very generic, customisable way!

For example:

    python scripts/arbitrary_sm.py \
        sample get_sample_by_external_id \
        --json '{"project": "acute-care", "external_id": "<external-id>"}'

"""
import subprocess
from typing import List

import argparse
import json

from sample_metadata import apis


def run_sm(
    api_name: str, method_name: str, args: List[str] = None, kwargs: dict = None
):
    """
    Use the sample metadata API based on:
    :param api_name: pure name of API, eg: 'analysis'
    :param method_name: name of method in snake case
    :param args: positional args of endpoint
    :param kwargs: keyword arguments of endpoint, note:
        POST requests have funny kwarg names, eg:
        'body_get_samples_by_criteria_api_v1_sample_post'
    """
    api_class_name = api_name.title() + 'Api'
    api = getattr(apis, api_class_name)
    api_instance = api()
    response = getattr(api_instance, method_name)(*(args or []), **(kwargs or {}))

    return response


def from_args(args):
    """Collect args from argparser, and call 'run_sm'"""
    positional_args: List[str] = args.args
    kwargs = {}

    if args.file_to_localise:
        for file in args.file_to_localise:
            subprocess.check_output(['gsutil', 'cp', file, '.'])

    json_str = args.json
    if json_str:
        kwargs = json.loads(json_str)

    return run_sm(
        api_name=args.api_name,
        method_name=args.method_name,
        args=positional_args,
        kwargs=kwargs,
    )


def main(args=None):
    """Main function, parses sys.argv"""

    parser = argparse.ArgumentParser('Arbitrary sample-metadata script')
    parser.add_argument('api_name')
    parser.add_argument('method_name')
    parser.add_argument(
        '--file-to-localise', action='append', help='List of GS files to localise'
    )
    parser.add_argument('--json', help='JSON encoded dictionary for kwargs')
    parser.add_argument(
        '--args',
        nargs='+',
        help='any positional arguments to pass to the API',
    )

    response = from_args(parser.parse_args(args))
    print(response)


if __name__ == '__main__':
    main()
