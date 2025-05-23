#!/usr/bin/env python
# pylint: disable=no-member,consider-using-with
"""
Run the metamist API through the analysis runner
in a very generic, customisable way!

For example:

    python scripts/arbitrary_sm.py \
        sample get_sample_by_external_id \
        --json '{"project": "acute-care", "external_id": "<external-id>"}'

"""

import argparse
import json
import logging
import os.path
import subprocess
from typing import List

from metamist import apis
from metamist.model_utils import file_type


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

    # the latest metamist API wants an IOBase, so let's
    # scan through the params, open and substitute the files
    openapi_types = getattr(api_instance, f'{method_name}_endpoint').__dict__[
        'openapi_types'
    ]
    params_to_open = [k for k, t in openapi_types.items() if file_type in t]
    files_to_close = []
    modified_kwargs = {**kwargs}
    for k in params_to_open:
        potential_path = kwargs.get(k)
        if potential_path and os.path.exists(potential_path):
            logging.info(f'Opening {k!r}: {potential_path}')
            files_to_close.append(open(potential_path))
            modified_kwargs[k] = files_to_close[-1]
        else:
            logging.info(f'Skipping opening {k}')

    response = getattr(api_instance, method_name)(
        *(args or []), **(modified_kwargs or {})
    )

    for f in files_to_close:
        f.close()

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

    parser = argparse.ArgumentParser('Arbitrary metamist script')
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
