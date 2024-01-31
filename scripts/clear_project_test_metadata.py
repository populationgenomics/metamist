import click

from metamist.apis import ProjectApi


@click.command()
@click.option('--project', required=True, help='Project to delete data from')
def main(project: str):
    """
    Deletes all data from a specified project in Metamist.

    This function uses the Metamist API to delete all data associated with a given project.

    Args:
        project (str): The name of the project from which to delete data.

    Returns:
        None
    """
    papi = ProjectApi()

    # Delete Project Data
    papi_response = papi.delete_project_data(project=project)

    print(papi_response)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
