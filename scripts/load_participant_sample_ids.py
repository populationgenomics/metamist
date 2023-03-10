import click
import csv
from collections import defaultdict

from sample_metadata.api.participant_api import ParticipantApi

papi = ParticipantApi()

def read_external_participants_ids_file(file_path):
    """Reads a csv of project, external id mappings into a dict with project keys"""
    projects_ext_ids = defaultdict(list)
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            projects_ext_ids[row[0]].append(row[1])
    
    return projects_ext_ids

def map_external_participant_to_internal_sample_id(project, ext_participant_ids):
    """Returns the sample """
    full_participants_map = papi.get_external_participant_id_to_internal_sample_id(project=project)
    participants_samples = [(project, i[0], i[1]) for i in full_participants_map if i[0] in ext_participant_ids]

    return participants_samples

def write_participant_sample_map_to_file(participants_samples, output_csv_path, header):
    """Writes project, external participant id, sample id, to each row of a csv file"""
    with open(output_csv_path, 'w') as sample_file:
        writer = csv.writer(sample_file)
        if header:
            writer.writerow(['Project', 'External_ID', 'Sample_ID'])
        for project, participant, sample_id in participants_samples:
            writer.writerow([project, participant, sample_id])
        
        sample_file.close()

@click.command()
@click.option('--map-file', help='Path to the project : external participant id mapping csv')
@click.option('--output-path', help='Path for the output csv', default='~/')
@click.option('--header', help='Adds header row to output csv', default=False)
def main(map_file, output_path, header):
    """
    Takes a csv file of project ids, external participant ids and returns the csv with an extra
    column containing the sample id(s) assosciated with that project & external participant id
    """
    projects_external_ids = read_external_participants_ids_file(map_file)

    participants_samples = []
    for project, ext_participant_id_list in projects_external_ids.items():
        map = map_external_participant_to_internal_sample_id(project, ext_participant_id_list)
        participants_samples += map

    write_participant_sample_map_to_file(participants_samples, output_path, header)


if __name__ == '__main__':
    main()