from metamist.apis import ProjectApi
import click


@click.command()
@click.option('--project', required=True, help='Project to delete data from')
def main(project: str):
    papi = ProjectApi()

    # Delete Project Data
    papi_response = papi.delete_project_data(project=project)

    print(papi_response)


if __name__ == '__main__':
    main()
