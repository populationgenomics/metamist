#!/usr/bin/env python3

# import os
import asyncio
import logging
import click

# from typing import Set
from sample_metadata.apis import ProjectApi, WebApi

# Global vars
logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

projapi = ProjectApi()
webapi = WebApi()


async def get_all_sequences():
    """
    Get all the sequences across all the projects from sample_metadata
    """
    projects = projapi.get_my_projects()
    # Uncomment for get_all_projects
    # projects = list(set([proj['name'] for proj in projects]))
    projects.sort()

    print(projects)

    jobs = [webapi.get_project_summary_async(proj) for proj in projects]
    summaries = await asyncio.gather(*jobs)
    logging.info(summaries)


@click.command()
def main():
    """Main from CLI"""
    asyncio.get_event_loop().run_until_complete(get_all_sequences())


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
