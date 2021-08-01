import csv
import io
import os
import json
import click
from google.cloud import storage
import requests


# CREATE DEV ENVIRONMENT VARIABLES.


@click.command()
@click.argument('bucket_name')
@click.argument('csv_path')
@click.argument('batch')
def process_csv(bucket_name: str, csv_path: str, batch: int):
    """Pull given csv from GCS bucket """
    project = os.getenv('GCP_PROJECT')
    # TODO: Update URL
    url = 'http://127.0.0.1:8080/upload_sample'
    headers = {'content-type': 'application/json'}

    client = storage.Client()
    gcs_bucket = client.get_bucket(bucket_name)

    csv_as_text = gcs_bucket.get_blob(csv_path).download_as_text()
    csv_reader = csv.DictReader(io.StringIO(csv_as_text))

    sample_metadata = []

    for sample_dict in csv_reader:
        sample_metadata.append({sample_dict['sample.sample_name']: dict(sample_dict)})

    for dictionary in sample_metadata:
        for key, value in dictionary.items():
            print(key)
            payload = {
                'project': project,
                'sample': value['sample.sample_name'],
                'status': 'uploaded',
                'batch': batch,
                'metadata': value,
            }
            requests.put(url, data=json.dumps(payload), headers=headers)


# Send put request for each sample in the CSV.
if __name__ == '__main__':

    process_csv()  # pylint: disable=E1120
