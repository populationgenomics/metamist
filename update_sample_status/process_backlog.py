import csv
import io
import json
from typing import Tuple, List
from google.cloud import storage
import requests


# CREATE DEV ENVIRONMENT VARIABLES.

# Takes in CSV and Batch Number
def get_csv(bucket_name: str, csv_path: str) -> Tuple[csv.DictReader, str]:
    """Pull given csv from GCS bucket """
    client = storage.Client()
    gcs_bucket = client.get_bucket(bucket_name)

    # all_blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    # csv_path = next(filter(lambda blob: blob.name.endswith('.csv'), all_blobs)).name

    csv_as_text = gcs_bucket.get_blob(csv_path).download_as_text()
    csv_reader = csv.DictReader(io.StringIO(csv_as_text))

    return csv_reader


# Formats data to create request.


# Send put request for each sample in the CSV.
if __name__ == '__main__':
    # TODO: Switch to command line var.
    # csv_reader = get_csv('vivian-dev-upload', 'test.csv')

    # sample_metadata = []

    # for sample_dict in csv_reader:
    #     sample_metadata.append({sample_dict['sample.sample_name']: dict(sample_dict)})

    # for dictionary in sample_metadata:
    #     for key, value in dictionary.items():
    #         print(key)
    #         print(value)

    print("Testing request")
    # request = requests.put('http://127.0.0.1:9999/test', data={'key': 'value'})
    # request_json = request.get_json()
    # print(request)
    # print(request_json)
    url = 'http://127.0.0.1:8080/test_function'
    payload = {
        'project': 'viviandev',
        'sample': 'viviandevtest1',
        'status': 'uploaded',
        'batch': '0',
        'metadata': {'freemix': '0.1', 'chimeras': '0.1'},
    }
    headers = {'content-type': 'application/json'}

    requests.put(url, data=json.dumps(payload), headers=headers)
