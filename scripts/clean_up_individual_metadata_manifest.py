"""
A script that cleans up and processes a CSV file containing individual metadata.
- It extracts HPO terms from a specified column in the CSV file and ensures they are in a consistent format.
- It makes sure multiline fields are handled correctly
- It saves the cleaned data to a new CSV file.

- HPO terms -
Saves the extracted terms as a comma-separated list.

E.g.:

"Ventricular fibrillation HP:0001663
Torsade de pointes HP:0001664
Ventricular tachycardia HP:0004756
Aborted sudden cardiac death HP:0031628
Mitral regurgitation HP:0001653
Prolonged QT interval HP:0001657
Syncope HP:0001279
Hypomagnesemia HP:0002917
Hypokalemia HP:0002900
Cervical insufficiency HP:0030009"

->

"HP:0001663, HP:0001664, HP:0004756, HP:0031628, HP:0001653, HP:0001657, HP:0001279, HP:0002917, HP:0002900, HP:0030009"

- Individual Notes - 

E.g.:

"Note1
Note2
Note3"
->

"Note1. Note2. Note3"

- Birth / Death year -
Ensures that birth and death years are cleaned up, removing any extraneous characters or whitespace. Only a four-digit year is retained.

- Age of onset -
Converts numerical age of onset values to a consistent format using the allowed values defined in the script.

- Boolean fields - 
(Consanguinity, Other Affected Relatives, Fertility medications, Intrauterine Insemination, In Vitro Fertilization, Intra-Cytoplasmic Sperm Injection, Gestational Surrogacy, Donor Egg, Donor Sperm)
Converts various string values to 'true' and 'false'.

- Expected Mode of Inheritance (EMOI) -
Cleans up the expected mode of inheritance field to ensure it is in a consistent format using the allowed values defined in the script.
The allowed values are defined in the script, and the value can be a comma-separated list of these values.

- Anything else -
Other fields which don't fit the schema are appended to the individual notes field.
    E.g. "HPO Terms (absent) = 'Left atrium dilation'" 
does not match the schema, so it's appended to the individual notes field:
    "Individual Notes = 'Note1. Note2. Note3. HPO Terms (absent): Left atrium dilation'"

"""

import csv
import click

BOOLEAN_FIELDS = [
    # Only true/false allowed in these fields
    'Consanguinity', 
    'Other Affected Relatives', 
    'Fertility Medications', 
    'Intrauterine Insemination', 
    'In Vitro Fertilization', 
    'Intra-Cytoplasmic Sperm Injection', 
    'Gestational Surrogacy', 
    'Donor Egg', 
    'Donor Sperm'
]

AGE_OF_ONSET_VALUES = {
    (0, 0.1): 'Neonatal onset',
    (0.1, 1): 'Infantile onset',
    (1, 5): 'Childhood onset',
    (5, 17): 'Juvenile onset',
    (17, 25): 'Young adult onset',
    (25, 36): 'Adult onset',
    (36, 55): 'Middle age onset',
    (55, 150): 'Late onset',
}

EMOI_VALUES = [
    'Sporadic',
    'Autosomal dominant inheritance',
    'Sex-limited autosomal dominant',
    'Male-limited autosomal dominant',
    'Autosomal dominant contiguous gene syndrome',
    'Autosomal recessive inheritance',
    'Gonosomal inheritance',
    'X-linked inheritance',
    'X-linked recessive inheritance',
    'Y-linked inheritance',
    'X-linked dominant inheritance',
    'Multifactorial inheritance',
    'Mitochondrial inheritance'
]

 
@click.command()
@click.option('--input-file', type=click.Path(exists=True))
@click.option('--output-file', type=click.Path())
@click.option('--hpo-column-name', type=str, default='HPO Terms (present)')
def main(input_file: str, output_file: str, hpo_column_name: str):
    """
    Command line interface to process the CSV file.
    """
    if not output_file:
        output_file = input_file.replace('.csv', '_cleaned.csv')
    process_csv(input_file, output_file, hpo_column_name)
    print(f"Processed {input_file} and saved cleaned data to {output_file}.")


def extract_hpo_terms(hpo_terms_string: str) -> str:
    """
    Extracts HPO terms from a string and reformates them.
    """
    hpo_terms = set()  # Use a set to avoid duplicates
    # print(f"Original terms: {hpo_terms_string}")
    # Split the terms by newline and strip whitespace
    terms = [term.strip() for term in hpo_terms_string.split('\n') if term.strip()]
    # Split the terms by whitespace and strip whitespace
    terms = [term.strip().removesuffix(':').removesuffix(';').removesuffix(',') for term in ' '.join(terms).split(' ') if term.strip()]
    # Remove any terms that are not in the format "HP:XXXXXXX"
    for term in terms:
        if term.startswith('HP:'):
            hpo_terms.add(term)
    # Update the row with the extracted HPO terms
    return ', '.join(sorted(hpo_terms))

def clean_multiline_field(field: str) -> str:
    """
    Cleans up multiline fields by replacing newlines with periods and spaces.
    """    
    # Replace newlines with periods and spaces
    cleaned_field = field.replace('\n', '. ').replace('\r', '')
    # Remove extra spaces
    cleaned_field = ' '.join(cleaned_field.split())
    # Correct any instances of multiple periods
    while '. .' in cleaned_field:
        cleaned_field = cleaned_field.replace('. .', '.')
    while '..' in cleaned_field:
        cleaned_field = cleaned_field.replace('..', '.')
    while '; .' in cleaned_field:
        cleaned_field = cleaned_field.replace('; .', ',')
    while ' . ' in cleaned_field:
        cleaned_field = cleaned_field.replace(' . ', '. ')

    return cleaned_field

def clean_birth_death_year(value: str) -> str:
    """
    Cleans up birth and death year fields to ensure they are in a four-digit format.
    """
    if '-' in value:
        year_split = value.split('-')
    if '/' in value:
        year_split = value.split('/')
    else:
        year_split = [value]
    # Filter out non-numeric values and ensure we only keep four-digit years
    cleaned_years = [y.strip() for y in year_split if y.strip().isdigit() and len(y.strip()) == 4]
    # Return the first valid year or an empty string if none found
    return cleaned_years[0] if cleaned_years else ''

def clean_boolean_field(value: str) -> str:
    """
    Converts various string values to 'true' or 'false'.
    """
    true_values = {'true', 'yes', '1', 'y', 't'}
    false_values = {'false', 'no', '0', 'n', 'f'}

    value_lower = value.strip().lower()
    if value_lower in true_values:
        return 'true'
    elif value_lower in false_values:
        return 'false'
    else:
        return ''  # Return empty string for unrecognized values

def clean_age_of_onset(value: str) -> str:
    """
    Cleans up the age of onset field to ensure it is in a consistent format.
    """
    if value.strip() in AGE_OF_ONSET_VALUES.values():
        return value.strip()  # If it's already a valid value, return it as is
    try:
        age = float(value.strip())
        for age_range, label in AGE_OF_ONSET_VALUES.items():
            if age_range[0] <= age < age_range[1]:
                return label
        return ''  # If no range matches, return nothing
    except ValueError:
        return ''  # Return nothing for non-numeric values

def clean_emoi(value: str) -> str:
    """
    Cleans up the expected mode of inheritance field to ensure it is in a consistent format.
    """
    # Check if the value is a comma-separated list, and if so, if each value is in the allowed EMOI values
    if ',' in value:
        emoi_terms = [term.strip() for term in value.split(',')]
        cleaned_terms = [term for term in emoi_terms if term in EMOI_VALUES]
        return ', '.join(cleaned_terms) if cleaned_terms else ''
    else:
        # If it's a single value, check if it's in the allowed EMOI values
        return value.strip() if value.strip() in EMOI_VALUES else ''

def validate_field(row: dict, field: str) -> bool:
    """
    Validates if a field in the row matches the expected schema.
    Returns True if it matches, False otherwise.
    """
    # Check if the field is in the row and not empty
    if field in row and row[field].strip():
        # If the field is a boolean field, check if it is 'true' or 'false'
        if field in BOOLEAN_FIELDS:
            return row[field].strip().lower() in {'true', 'false'}
        # If the field is a birth or death year, check if it is a valid four-digit year
        elif field in ('Birth Year', 'Death Year'):
            return len(row[field].strip()) == 4 and row[field].strip().isdigit()
        # If the field is age of onset, check if it is one of the allowed values
        elif field == 'Age of Onset':
            return row[field].strip() in AGE_OF_ONSET_VALUES.values()
        # If the field is expected mode of inheritance, check if it is one of the allowed values
        elif field == 'Expected Mode of Inheritance':
            return row[field].strip() in EMOI_VALUES
        elif field == 'HPO Terms (absent)':
            if row[field].strip() and not extract_hpo_terms(row[field].strip()):
                return False  # If HPO terms are present but not valid, return False
            return True  # If HPO terms are valid or empty, return True
        else:
            return True  # For other fields, we assume they match the schema
    return False  # If the field does not exist or is empty, it does not match the schema

def clean_row(row: dict, field: str) -> dict:
    """
    Cleans up a row by appending fields that do not match the schema to the individual notes field.
    """
    if field not in BOOLEAN_FIELDS + ['Family ID', 'Individual ID', 'HPO Terms (present)', 'Birth Year', 'Death Year', 'Age of Onset', 'Expected Mode of Inheritance', 'Pre-discovery OMIM disorders', 'Previously Tested Genes', 'Candidate Genes']:
        if validate_field(row, field):
            # If the field matches the schema, we can keep it as is
            return row
        # If the field does not match the schema, append it to the individual notes field
        print(f"Field '{field}' for {row['Individual ID']} does not match the schema. Appending to Individual Notes.")
        if 'Individual Notes' in row:
            row['Individual Notes'] += f". {field}: {row[field]}"
        else:
            row['Individual Notes'] = f"{field}: {row[field]}"
        # Blank out the field from the row
        row[field] = ''
    return row

def process_csv(input_file: str, output_file: str, hpo_column_name: str):
    """
    Writes a new CSV file with cleaned up individual metadata.
    """
    with open(input_file, 'r', newline='', encoding='latin-1') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames

        # Check if the HPO column exists
        if hpo_column_name not in fieldnames:
            raise ValueError(f"Column '{hpo_column_name}' not found in the input file.")

        # Create a new list to hold the cleaned rows
        cleaned_rows = []

        for row in reader:
            for field in fieldnames:
                if field != hpo_column_name:
                    # Clean up multiline fields
                    row[field] = clean_multiline_field(row[field])
                
                if field in BOOLEAN_FIELDS:
                    # Clean boolean fields
                    row[field] = clean_boolean_field(row[field])
                    
                if field in ('Birth Year', 'Death Year'):
                    # Clean birth and death years
                    row[field] = clean_birth_death_year(row[field])
                    
                if field == 'Age of Onset':
                    # Clean age of onset
                    row[field] = clean_age_of_onset(row[field])
                    
                if field == 'Expected Mode of Inheritance':
                    # Clean expected mode of inheritance
                    row[field] = clean_emoi(row[field])
                
                else:
                    if row[field] == '':
                        # If the field is empty, skip cleaning
                        continue
                    else:
                        row = clean_row(row, field)

            # Extract and clean HPO terms
            row[hpo_column_name] = extract_hpo_terms(row[hpo_column_name])            
            cleaned_rows.append(row)
            
    # Write the cleaned rows to a new CSV file
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_rows)
    
if __name__ == "__main__":
    # Define the input and output file paths
    main()