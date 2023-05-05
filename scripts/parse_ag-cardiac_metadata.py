import csv
from collections import defaultdict
import click

INDIVIDUAL_METADATA_FIELDS = [
    'Family ID',
    'Individual ID',
    'HPO Terms (present)',
    'HPO Terms (absent)',
    'Birth Year',
    'Death Year',
    'Age of Onset',
    'Individual Notes',
    'Consanguinity',
    'Other Affected Relatives',
    'Maternal Ancestry',
    'Paternal Ancestry',
]


def read_redcap_input(file_path):
    """
    Read individuals redcap tsv and store its content into a list of dictionaries.

    :param file_path: str
    :return: list of dicts
    """
    individuals = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            individuals.append(row)

    return individuals


def process_individuals(individuals):
    """
    Process individuals data and generate individual metadata rows and pedigree dictionaries.
    Append the various fields by their column headers and values into a long string to use as the Individual Notes field.

    :param individuals: list of dicts
    :return: tuple (list of dicts, defaultdict of dicts)
    """
    consanguinity_map = {'Yes': 'TRUE', 'No': 'FALSE', 'Unknown': ''}

    individual_notes_headers = [
        'Age at diagnosis (years)',
        'Participant ethnicity summary',
        'Is there an affected sibling?',
        'Is there an affected child?',
        'Number of affected first degree relatives',
        'Number of affected second degree relatives',
        'Presumed Inheritance pattern',
        'Clinical area',
        'Diagnosis',
        'CHD Diagnosis',
        'Gene panel used',
    ]

    individual_ids = []
    individuals_metadata = []
    individuals_pedigree = defaultdict(dict)

    for individual in individuals:
        individual_id = individual.get('Study Number')
        individual_ids.append(individual_id)
        individual_notes = (
            ''  # Initialise the notes as empty string, we will append to this
        )

        for field, value in individual.items():
            if field in individual_notes_headers and value:
                individual_notes += field + ': '
                individual_notes += value + '; '

        # Get the pedigree fields
        (
            sex,
            mat_phenotype,
            pat_phenotype,
            sib_phenotype,
            child_phenotype,
        ) = process_pedigree_fields(individual)

        # Create a pedigree entry for each individual
        individuals_pedigree[individual_id] = {
            'sex': sex,
            'mat_phenotype': mat_phenotype,
            'pat_phenotype': pat_phenotype,
            'sib_phenotype': sib_phenotype,
            'child_phenotype': child_phenotype,
        }

        # oar = 'Other Affected Relatives'
        oar = (
            'TRUE'
            if mat_phenotype == 2
            or pat_phenotype == 2
            or sib_phenotype == 2
            or child_phenotype == 2
            else 'FALSE'
        )

        # Create the individual metadata entry for each individual
        individual_metadata_row = generate_individual_metadata_row(
            individual, individual_notes, consanguinity_map, oar
        )
        individuals_metadata.append(individual_metadata_row)

    return individual_ids, individuals_metadata, individuals_pedigree


def process_pedigree_fields(individual):
    """
    Process individual sex and phenotypes of relatives

    :param individual: dict
    :return: tuple (int, int, int, int, int)
    """
    sex = get_sex(individual)

    # Maternal phenotype
    if individual.get('Is mother affected?') == 'Yes':
        mat_phenotype = 2
    elif individual.get('Is mother affected?') == 'No':
        mat_phenotype = 1
    else:
        mat_phenotype = 0

    # Paternal phenotype
    if individual.get('Is father affected?') == 'Yes':
        pat_phenotype = 2
    elif individual.get('Is father affected?') == 'No':
        pat_phenotype = 1
    else:
        pat_phenotype = 0

    # Sibling phenotype
    if 'Yes' in individual.get('Is there an affected sibling?'):
        sib_phenotype = 2
    # Some values in this field are 'Not known' (aka unknown) - so we exclude 'Not' when we check if the field contains 'No'
    elif 'No' in individual.get(
        'Is there an affected sibling?'
    ) and 'Not' not in individual.get('Is there an affected sibling?'):
        sib_phenotype = 1
    else:
        sib_phenotype = 0

    # Child phenotype
    if 'Yes' in individual.get('Is there an affected child?'):
        child_phenotype = 2
    elif 'No' in individual.get(
        'Is there an affected child?'
    ) and 'Not' not in individual.get('Is there an affected child?'):
        child_phenotype = 1
    else:
        child_phenotype = 0

    return sex, mat_phenotype, pat_phenotype, sib_phenotype, child_phenotype


def get_sex(individual):
    """
    Get sex value based on the 'Sex of patient' field.

    :param individual: dict
    :return: int
    """
    if individual.get('Sex of patient') == 'Male':
        return 1
    if individual.get('Sex of patient') == 'Female':
        return 2
    return 0


def generate_individual_metadata_row(
    individual, individual_notes, consanguinity_map, oar
):
    """
    Generate a metadata row for an individual.

    :param individual: dict
    :param individual_notes: str
    :param consanguinity_map: dict
    :param oar: str
    :return: dict
    """
    return {
        'Family ID': individual.get('Study Number'),
        'Individual ID': individual.get('Study Number'),
        'HPO Terms (present)': '',
        'HPO Terms (absent)': '',
        'Birth Year': '',
        'Death Year': '',
        'Age of Onset': '',
        'Individual Notes': individual_notes,
        'Consanguinity': consanguinity_map.get(individual.get('Consanguinity')),
        'Other Affected Relatives': oar,
        'Maternal Ancestry': individual.get('Maternal ethnicity summary'),
        'Paternal Ancestry': individual.get('Paternal ethnicity summary'),
    }


def write_individuals_output(file_path, individuals_metadata):
    """
    Write individuals metadata to a CSV file.

    :param file_path: str
    :param individuals_metadata: list of dicts
    :return: None
    """
    with open(file_path, 'w') as f:
        writer = csv.DictWriter(f, delimiter=',', fieldnames=INDIVIDUAL_METADATA_FIELDS)
        writer.writeheader()

        for individual in individuals_metadata:
            writer.writerow(individual)


def get_families_from_manifest(file_path):
    """
    Read input manifest file and store its content into a list of tuples.

    :param file_path: str
    :return: list of tuples
    """
    families = defaultdict(list)
    participants = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(
            f,
            delimiter='\t',
        )
        for row in reader:
            if row['agha_study_id'] in participants:
                continue
            participants.append(row['agha_study_id'])
            process_manifest_row(families, row)

    return families


def process_manifest_row(families, row):
    """
    Process a row from the input manifest file and update the families dictionary.

    :param families: defaultdict of lists
    :param row: list
    :return: None
    """
    if '_' not in row['agha_study_id']:
        family_id = row['agha_study_id']
        families[family_id].append(row['agha_study_id'])
    else:
        family_id = row['agha_study_id'].split('_')[0]
        families[family_id].append(row['agha_study_id'])


def get_family_members(families):
    """
    Get family members from the families dictionary and generate dictionaries for each family member type.

    :param families: defaultdict of lists
    :return: tuple (dict, dict, dict, dict, dict)
    """
    family_pat = {}
    family_mat = {}
    family_sib = {}
    family_relo1 = {}
    family_relo2 = {}
    for family_id, family_members in families.items():
        for family_member in family_members:
            if 'pat' in family_member:
                family_pat[family_id] = family_member
            elif 'mat' in family_member:
                family_mat[family_id] = family_member
            elif 'sib' in family_member:
                family_sib[family_id] = family_member
            elif 'R1' in family_member:
                family_relo1[family_id] = family_member
            elif 'R2' in family_member:
                family_relo2[family_id] = family_member
            else:
                continue

    return family_pat, family_mat, family_sib, family_relo1, family_relo2


def generate_ped_rows(families, individuals_pedigree, family_pat, family_mat):
    """
    Generate PED rows from families and individuals_pedigree dictionaries.

    :param families: defaultdict of lists
    :param individuals_pedigree: defaultdict of dicts
    :param family_pat: dict
    :param family_mat: dict
    :param family_sib: dict
    :return: list of tuples
    """
    ped_rows = []
    for family_id, family_members in families.items():
        if len(family_members) == 1:
            sex = individuals_pedigree.get(family_members[0])['sex']
            ped_rows.append((family_id, family_members[0], 0, 0, sex, 2))
            continue

        for family_member in family_members:
            process_family_member(
                family_member,
                family_id,
                individuals_pedigree,
                ped_rows,
                family_pat,
                family_mat,
            )

    return ped_rows


def process_family_member(
    family_member,
    family_id,
    individuals_pedigree,
    ped_rows,
    family_pat,
    family_mat,
):
    """
    Process a family member and append a PED row to the ped_rows list.

    :param family_member: str
    :param family_id: str
    :param individuals_pedigree: defaultdict of dicts
    :param ped_rows: list of tuples
    :param family_pat: dict
    :param family_mat: dict
    :return: None
    """
    if '_' not in family_member:
        sex = individuals_pedigree.get(family_member)['sex']
        try:
            pat = family_pat.get(family_id)
        except KeyError:
            pat = 0
        try:
            mat = family_mat.get(family_id)
        except KeyError:
            mat = 0
        ped_rows.append((family_id, family_member, pat, mat, sex, 2))

    elif '_pat' in family_member:
        sex = 1
        pat_phenotype = individuals_pedigree.get(family_id)['pat_phenotype']
        ped_rows.append((family_id, family_member, 0, 0, sex, pat_phenotype))

    elif '_mat' in family_member:
        sex = 2
        mat_phenotype = individuals_pedigree.get(family_id)['mat_phenotype']
        ped_rows.append((family_id, family_member, 0, 0, sex, mat_phenotype))

    elif '_sib' in family_member:
        sex = 0
        sib_phenotype = individuals_pedigree.get(family_id)['sib_phenotype']
        try:
            pat = family_pat.get(family_id)
        except KeyError:
            pat = 0
        try:
            mat = family_mat.get(family_id)
        except KeyError:
            mat = 0
        ped_rows.append((family_id, family_member, pat, mat, sex, sib_phenotype))

    elif '_R' in family_member:
        sex = 0
        r_phenotype = 0
        ped_rows.append((family_id, family_member, 0, 0, sex, r_phenotype))


def write_ped_rows(filename, ped_rows):
    """Writes each pedigree row in ped_rows to the output ped file"""
    with open(filename, 'w') as f:
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
        for row in ped_rows:
            writer.writerow(row)


def append_indiv_metadata_rows_from_ped(
    individals_metadata_file, ped_rows, individual_ids, individuals_metadata
):
    """Add the individuals from the manifest (ped_rows) that are not in the redcap to the individual metadata csv"""
    individuals_metadata_dict = {}
    for entry in individuals_metadata:
        individuals_metadata_dict[entry.get('Individual ID')] = entry

    affected_status_map = {}
    for row in ped_rows:
        # {Individual ID : Affected Status} mapping
        affected_status_map[row[1]] = row[5]

    with open(
        individals_metadata_file,
        'a',
    ) as f:
        writer = csv.DictWriter(f, delimiter=',', fieldnames=INDIVIDUAL_METADATA_FIELDS)

        for ped_row in ped_rows:
            family_id = ped_row[0]
            individual_id = ped_row[1]

            # Filter to the individuals not already in the metadata file
            if individual_id not in individual_ids:
                oar = ''
                consanguinity = ''

                # Set consang to the same value if sibling of proband
                if '_sib' in individual_id:
                    consanguinity = individuals_metadata_dict.get(family_id)[
                        'Consanguinity'
                    ]

                    # Set other affected rels to same value if sibling also affected
                    if affected_status_map.get(individual_id) == 2:
                        oar = individuals_metadata_dict.get(family_id)[
                            'Other Affected Relatives'
                        ]

                writer.writerow(
                    {
                        'Family ID': family_id,
                        'Individual ID': individual_id,
                        'HPO Terms (present)': '',
                        'HPO Terms (absent)': '',
                        'Birth Year': '',
                        'Death Year': '',
                        'Age of Onset': '',
                        'Individual Notes': '',
                        'Consanguinity': consanguinity,
                        'Other Affected Relatives': oar,
                        'Maternal Ancestry': '',
                        'Paternal Ancestry': '',
                    }
                )


def create_sample_maps_from_manifest(manifest_path):
    """
    Maps each individual ID to its sample ID
    Maps each individual ID to its sequence read files
    Maps each individual ID to the total size of all its sequence files
    """
    study_id_sample_id_map = {}
    study_id_sequence_paths = defaultdict(list)
    study_id_filesizes = defaultdict(int)

    with open(manifest_path, 'r') as f:
        reader = csv.reader(
            f,
            delimiter='\t',
        )
        next(reader)
        for row in reader:
            # Read each row of the manifest and extract the values
            study_id = row[0]
            seq_path = row[1]
            filesize = int(row[2])

            # The number of underscores determines how we extract the sample ID from the file path
            split_path = seq_path.split('_')

            if len(split_path) == 9:
                # VCGS
                sample_id = split_path[4]
                sample_id = sample_id.rsplit('-', 1)[0]

                if sample_id not in study_id_sample_id_map:
                    study_id_sample_id_map[study_id] = sample_id

                study_id_sequence_paths[study_id].append(seq_path)
                study_id_filesizes[study_id] += filesize

            elif len(split_path) == 12:
                # Garvan
                sample_id = split_path[3]

                if sample_id not in study_id_sample_id_map:
                    study_id_sample_id_map[study_id] = sample_id

                study_id_sequence_paths[study_id].append(seq_path)
                study_id_filesizes[study_id] += filesize

            elif len(split_path) == 7:
                # Unknown
                # Some filenames which split into 7 components have the sample ID at the 3rd index, some at 4th, some at 5th
                if len(split_path[3]) >= 10:
                    sample_id = split_path[3]
                    if sample_id not in study_id_sample_id_map:
                        study_id_sample_id_map[study_id] = sample_id
                    study_id_sequence_paths[study_id].append(seq_path)
                    study_id_filesizes[study_id] += filesize
                    continue

                if len(split_path[4]) >= 10:
                    sample_id = split_path[4]
                    if sample_id not in study_id_sample_id_map:
                        study_id_sample_id_map[study_id] = sample_id
                    study_id_sequence_paths[study_id].append(seq_path)
                    study_id_filesizes[study_id] += filesize
                    continue

                sample_id = split_path[5]
                if sample_id not in study_id_sample_id_map:
                    study_id_sample_id_map[study_id] = sample_id
                study_id_sequence_paths[study_id].append(seq_path)
                study_id_filesizes[study_id] += filesize

            elif len(split_path) == 8:
                # Unknown
                # Some filenames which split into 8 components have the sample ID at the 4th index, some at 5th
                if len(split_path[4]) >= 10:
                    sample_id = split_path[4]
                    if sample_id not in study_id_sample_id_map:
                        study_id_sample_id_map[study_id] = sample_id
                    study_id_sequence_paths[study_id].append(seq_path)
                    study_id_filesizes[study_id] += filesize
                    continue

                sample_id = split_path[5]
                if sample_id not in study_id_sample_id_map:
                    study_id_sample_id_map[study_id] = sample_id
                study_id_sequence_paths[study_id].append(seq_path)
                study_id_filesizes[study_id] += filesize

            elif len(split_path) == 13:
                # Alternate Garvan? Like a Garvan filename but with an extra string at the front.
                sample_id = split_path[4]
                if sample_id not in study_id_sample_id_map:
                    study_id_sample_id_map[study_id] = sample_id
                study_id_sequence_paths[study_id].append(seq_path)
                study_id_filesizes[study_id] += filesize

    return study_id_sample_id_map, study_id_sequence_paths, study_id_filesizes


def guess_sequence_type(study_id_filesizes):
    """Guesses sequence type from sequeunce read file sizes"""
    study_id_sequence_type = {}
    for study_id, filesize in study_id_filesizes.items():
        filesize_gb = int(filesize) / 1e9
        if float(filesize_gb) < 45:
            study_id_sequence_type[study_id] = 'WES'
        else:
            study_id_sequence_type[study_id] = 'WGS'

    return study_id_sequence_type


def write_sample_map(
    sample_mapping_file,
    study_id_sample_id_map,
    study_id_sequence_paths,
    study_id_sequence_type,
):
    """Creates the standard CPG sample mapping file, determining WGS or WES from file size"""
    with open(sample_mapping_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                'Individual ID',
                'Sample ID',
                'File names',
                'Type',
            ]
        )

        for study_id in study_id_sample_id_map.keys():
            sample_id = study_id_sample_id_map.get(study_id)
            files = study_id_sequence_paths.get(study_id)
            sequence_type = study_id_sequence_type.get(study_id)

            writer.writerow([study_id, sample_id, files, sequence_type])


@click.command(
    help='Transform the AG-cardiac raw metadata into cpg ingestable metadata files'
)
@click.option('--redcap-csv-path')
@click.option('--manifest-path')
@click.option('--output-folder')
def main(
    redcap_csv_path,
    manifest_path,
    output_folder,
):
    """
    Takes 3 paths, the first two are the input metadata files from Aus Genomics
    and the last is the path to the folder to store the outputs.
    """

    individuals = read_redcap_input(redcap_csv_path)

    individual_ids, individuals_metadata, individuals_pedigree = process_individuals(
        individuals
    )

    write_individuals_output(
        f'{output_folder}/individuals_metadata.csv', individuals_metadata
    )

    families = get_families_from_manifest(manifest_path)
    family_pat, family_mat, _, _, _ = get_family_members(families)

    ped_rows = generate_ped_rows(families, individuals_pedigree, family_pat, family_mat)

    write_ped_rows(f'{output_folder}/ped_file.csv', ped_rows)
    append_indiv_metadata_rows_from_ped(
        f'{output_folder}/individuals_metadata.csv',
        ped_rows,
        individual_ids,
        individuals_metadata,
    )

    (
        study_id_sample_id_map,
        study_id_sequence_paths,
        study_id_filesizes,
    ) = create_sample_maps_from_manifest(manifest_path)
    study_id_sequence_type = guess_sequence_type(study_id_filesizes)

    write_sample_map(
        f'{output_folder}/mapping_file.csv',
        study_id_sample_id_map,
        study_id_sequence_paths,
        study_id_sequence_type,
    )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
