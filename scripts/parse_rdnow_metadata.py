import csv
from collections import defaultdict
import click


def convert_sex(sex_str: str) -> int:
    """converts from male/female to 1/2"""
    if sex_str.lower() == 'male':
        return 1
    if sex_str.lower() == 'female':
        return 2

    return 0


def convert_sequence_type(seq_type_str: str) -> str:
    """converts sequence type from es/gs to WES/WGS"""
    if seq_type_str.lower() == 'es':
        return 'WES'
    if seq_type_str.lower() == 'gs':
        return 'WGS'

    return ''


def read_genomic_history_csv(gh_data_path):
    """Parse the genomic history csv"""
    # Families we actually have data for are in the GenomicTestingHistory csv, so we log each family in an array
    family_ids = []
    families = []
    family_rows = {}
    with open(gh_data_path, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            if row['rdn_family_id']:
                # Save previous family
                if 'family_id' in family_rows:
                    families.append(family_rows)
                    family_ids.append(family_rows.get('family_id'))
                    family_rows = {}

                family_rows['family_id'] = row['rdn_family_id']

            if row['rdn_individual_id']:
                family_rows['individual_id'] = row['rdn_individual_id']

            # Check if row is for genomes or exomes
            seq_type = False
            if row['rdn_th_seqtype']:
                family_rows['sequence_type'] = convert_sequence_type(
                    row['rdn_th_seqtype']
                )
                seq_type = True

            # Define different fields depending on if the sequence types are exome or genome
            if seq_type:
                if family_rows['sequence_type'] == 'WES':
                    if row['rdn_th_seq_id']:
                        family_rows['individual_sample_id_WES'] = row['rdn_th_seq_id']

                    if row['rdn_th_seq_id_rel1']:
                        family_rows['relative1_sample_id_WES'] = row[
                            'rdn_th_seq_id_rel1'
                        ]

                    if row['rdn_th_seq_id_rel2']:
                        family_rows['relative2_sample_id_WES'] = row[
                            'rdn_th_seq_id_rel2'
                        ]

                elif family_rows['sequence_type'] == 'WGS':
                    if row['rdn_th_seq_id']:
                        family_rows['individual_sample_id_WGS'] = row['rdn_th_seq_id']

                    if row['rdn_th_seq_id_rel1']:
                        family_rows['relative1_sample_id_WGS'] = row[
                            'rdn_th_seq_id_rel1'
                        ]

                    if row['rdn_th_seq_id_rel2']:
                        family_rows['relative2_sample_id_WGS'] = row[
                            'rdn_th_seq_id_rel2'
                        ]

    return family_ids, families


def read_pedigree_csv(ped_data_path, family_ids):
    """Reads the pedigree csv and creates the pedigree dictionary, filters out families not in the family_ids list created from the genomic history csv"""
    # The pedigree csv has rows for ALL families, beyond those which we just have data for
    pedigrees = defaultdict(dict)
    with open(ped_data_path, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            family_id = row['rdn_family_id']
            # For each family, check if its in the first csv by looking at the family_ids array
            if family_id not in family_ids:
                continue

            individual_id = row['rdn_individual_id']
            sex = convert_sex(row['rdn_sex'])

            relative1_id = row['rdn_rel_1_study_id']
            relative1_sex = int(row['rdn_rel_1_sex'])
            relative1_affected = int(row['rdn_rel_1_affected'])

            relative2_id = row['rdn_rel_2_study_id']
            relative2_sex = int(row['rdn_rel_2_sex'])
            relative2_affected = int(row['rdn_rel_2_affected'])

            # Add the proband + relative pedrow dictionaries to the pedigrees dictionary, with the family id as the master key
            pedigrees[family_id] = {
                'proband': {
                    'Family ID': family_id,
                    'Individual ID': individual_id,
                    'Paternal ID': 0,
                    'Maternal ID': 0,
                    'Sex': sex,
                    'Affected Status': 2,
                },
                'relative1': {
                    'Family ID': family_id,
                    'Individual ID': relative1_id,
                    'Paternal ID': 0,
                    'Maternal ID': 0,
                    'Sex': relative1_sex,
                    'Affected Status': relative1_affected,
                },
                'relative2': {
                    'Family ID': family_id,
                    'Individual ID': relative2_id,
                    'Paternal ID': 0,
                    'Maternal ID': 0,
                    'Sex': relative2_sex,
                    'Affected Status': relative2_affected,
                },
            }

    return pedigrees


def create_sample_mapping_rows(families, pedigrees):
    """Creates the list of rows to insert into the sample_mapping csv"""
    sample_mapping_rows = []
    for family in families:
        # Both family ID and individual ID will exist for every family
        family_id = family['family_id']
        individual_id = family['individual_id']

        # Add a row for proband WES sample
        if 'individual_sample_id_WES' in family:
            sample_id = family['individual_sample_id_WES']
            sample_mapping_rows.append((individual_id, sample_id, '', 'WES'))

        # Add a row for proband WGS sample
        if 'individual_sample_id_WGS' in family:
            sample_id = family['individual_sample_id_WGS']
            sample_mapping_rows.append((individual_id, sample_id, '', 'WGS'))

        # Check if relative 1 exists in the family
        if 'relative1_sample_id_WES' in family or 'relative1_sample_id_WGS' in family:
            relative1_ped = pedigrees.get(family_id)['relative1']
            relative1_individual_id = relative1_ped.get('Individual ID')
            # Add a row for relative 1 WES sample
            if 'relative1_sample_id_WES' in family:
                relative1_sample_id = family['relative1_sample_id_WES']
                sample_mapping_rows.append(
                    (relative1_individual_id, relative1_sample_id, '', 'WES')
                )
            # Add a row for relative 1 WGS sample
            if 'relative1_sample_id_WGS' in family:
                relative1_sample_id = family['relative1_sample_id_WGS']
                sample_mapping_rows.append(
                    (relative1_individual_id, relative1_sample_id, '', 'WGS')
                )

        # Check if relative 2 exists in the family
        if 'relative2_sample_id_WES' in family or 'relative2_sample_id_WGS' in family:
            relative2_ped = pedigrees.get(family_id)['relative2']
            relative2_individual_id = relative2_ped.get('Individual ID')
            # Add a row for relative 2 WGS sample
            if 'relative2_sample_id_WES' in family:
                relative2_sample_id = family['relative2_sample_id_WES']
                sample_mapping_rows.append(
                    (relative2_individual_id, relative2_sample_id, '', 'WES')
                )
            # Add a row for relative 2 WGS sample
            if 'relative2_sample_id_WGS' in family:
                relative2_sample_id = family['relative2_sample_id_WGS']
                sample_mapping_rows.append(
                    (relative2_individual_id, relative2_sample_id, '', 'WGS')
                )

    return sample_mapping_rows


def create_pedigree_rows(sample_mapping_rows, family_ids, pedigrees):
    """Creates the pedigree rows list from the pedigree dictionary"""
    pedigree_rows = []
    individuals_with_data = [mapping_row[0] for mapping_row in sample_mapping_rows]
    for family_id in family_ids:
        family_pedigree = pedigrees.get(family_id)

        proband_ped = family_pedigree.get('proband')
        relative1_ped = family_pedigree.get('relative1')
        relative2_ped = family_pedigree.get('relative2')

        if proband_ped.get('Individual ID') in individuals_with_data:
            pedigree_rows.append(list(proband_ped.values()))

        if relative1_ped.get('Individual ID') in individuals_with_data:
            pedigree_rows.append(list(relative1_ped.values()))

        if relative2_ped.get('Individual ID') in individuals_with_data:
            pedigree_rows.append(list(relative2_ped.values()))

    return pedigree_rows


def write_sample_mapping_file(output_dir, sample_mapping_rows):
    """Writes the sample mapping csv file"""
    with open(f'{output_dir}/sample_mapping_file.csv', 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Individual ID', 'Sample ID', 'Filenames', 'Type'])
        for mapping_row in sample_mapping_rows:
            writer.writerow(mapping_row)


def write_pedigree_file(output_dir, pedigree_rows):
    """Writes the pedigree csv file"""
    with open(f'{output_dir}/ped_file.csv', 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                'Family ID',
                'Individual ID',
                'Paternal ID',
                'Maternal ID',
                'Sex',
                'Affected Status',
            ]
        )
        for ped_row in pedigree_rows:
            writer.writerow(ped_row)


@click.command()
@click.option('-gh', '--genomic-history-path')
@click.option('-p', '--ped-path')
@click.argument('output-dir')
def main(genomic_history_path: str, ped_path: str, output_dir: str):
    """Reads the genomic testing history and pedigree csvs for rdnow project metadata"""
    family_ids, families = read_genomic_history_csv(genomic_history_path)
    pedigrees = read_pedigree_csv(ped_path, family_ids)

    sample_mapping_rows = create_sample_mapping_rows(families, pedigrees)
    pedigree_rows = create_pedigree_rows(sample_mapping_rows, family_ids, pedigrees)

    write_sample_mapping_file(output_dir, sample_mapping_rows)
    write_pedigree_file(output_dir, pedigree_rows)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
