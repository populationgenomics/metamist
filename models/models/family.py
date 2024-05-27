import logging

from pydantic import BaseModel


class FamilySimpleInternal(BaseModel):
    """Simple family model for internal use"""

    id: int
    external_id: str

    def to_external(self):
        """Convert to external model"""
        return FamilySimple(
            id=self.id,
            external_id=self.external_id,
        )


class FamilyInternal(BaseModel):
    """Family model"""

    id: int
    external_id: str
    project: int
    description: str | None = None
    coded_phenotype: str | None = None

    @staticmethod
    def from_db(d):
        """From DB fields"""
        return FamilyInternal(**d)

    def to_external(self):
        """Convert to external model"""
        return Family(
            id=self.id,
            external_id=self.external_id,
            project=self.project,
            description=self.description,
            coded_phenotype=self.coded_phenotype,
        )


class FamilySimple(BaseModel):
    """Simple family model, mostly for web access"""

    id: int
    external_id: str


class Family(BaseModel):
    """Family model"""

    id: int | None
    external_id: str
    project: int
    description: str | None = None
    coded_phenotype: str | None = None

    def to_internal(self):
        """Convert to internal model"""
        return FamilyInternal(
            id=self.id,
            external_id=self.external_id,
            project=self.project,
            description=self.description,
            coded_phenotype=self.coded_phenotype,
        )


class PedRowInternal:
    """Class for capturing a row in a pedigree"""

    def __init__(
        self,
        family_id: int,
        individual_id: int,
        paternal_id: int | None,
        maternal_id: int | None,
        sex: int | None,
        affected: int | None,
        notes: str | None,
    ):
        self.family_id = family_id
        self.individual_id = individual_id
        self.paternal_id = paternal_id
        self.maternal_id = maternal_id
        self.sex = sex
        self.affected = affected
        self.notes = notes

    def to_dict(self):
        """Convert to dictionary"""
        return {
            'family_id': self.family_id,
            'individual_id': self.individual_id,
            'paternal_id': self.paternal_id,
            'maternal_id': self.maternal_id,
            'sex': self.sex,
            'affected': self.affected,
            'notes': self.notes,
        }


class PedRow:
    """Class for capturing a row in a pedigree"""

    ALLOWED_SEX_VALUES = [0, 1, 2]
    ALLOWED_AFFECTED_VALUES = [-9, 0, 1, 2]

    PedRowKeys = {
        # seqr individual template:
        # Family ID, Individual ID, Paternal ID, Maternal ID, Sex, Affected, Status, Notes
        'family_id': {'familyid', 'family id', 'family', 'family_id'},
        'individual_id': {'individualid', 'id', 'individual_id', 'individual id'},
        'paternal_id': {'paternal_id', 'paternal id', 'paternalid', 'father'},
        'maternal_id': {'maternal_id', 'maternal id', 'maternalid', 'mother'},
        'sex': {'sex', 'gender'},
        'affected': {
            'phenotype',
            'affected',
            'phenotypes',
            'affected status',
            'affection',
            'affection status',
        },
        'notes': {'notes'},
    }

    @staticmethod
    def default_header():
        """Default header (corresponds to the __init__ keys)"""
        return [
            'family_id',
            'individual_id',
            'paternal_id',
            'maternal_id',
            'sex',
            'affected',
            'notes',
        ]

    @staticmethod
    def row_header():
        """Default RowHeader for output"""
        return [
            '#Family ID',
            'Individual ID',
            'Paternal ID',
            'Maternal ID',
            'Sex',
            'Affected',
        ]

    def __init__(
        self,
        family_id,
        individual_id,
        paternal_id,
        maternal_id,
        sex,
        affected,
        notes=None,
    ):
        self.family_id = family_id.strip()
        self.individual_id = individual_id.strip()
        self.paternal_id = None
        self.maternal_id = None
        self.paternal_id = self.check_linking_id(paternal_id, 'paternal_id')
        self.maternal_id = self.check_linking_id(maternal_id, 'maternal_id')
        self.sex = self.parse_sex(sex)
        self.affected = self.parse_affected_status(affected)
        self.notes = notes

    @staticmethod
    def check_linking_id(linking_id, description: str, blank_values=('0', '')):
        """Check that the ID is a valid value, or return None if it's a blank value"""
        if linking_id is None:
            return None
        if isinstance(linking_id, int):
            linking_id = str(linking_id).strip()

        if isinstance(linking_id, str):
            if linking_id.strip().lower() in blank_values:
                return None
            return linking_id.strip()

        raise TypeError(
            f'Unexpected type {type(linking_id)} ({linking_id}) '
            f'for {description}, expected "str"'
        )

    @staticmethod
    def parse_sex(sex: str | int) -> int:
        """
        Parse the pedigree SEX value:
            0: unknown
            1: male (also accepts 'm')
            2: female (also accepts 'f')
        """
        if isinstance(sex, str) and sex.isdigit():
            sex = int(sex)
        if isinstance(sex, int):
            if sex in PedRow.ALLOWED_SEX_VALUES:
                return sex
            raise ValueError(
                f'Sex value ({sex}) was not an expected value {PedRow.ALLOWED_SEX_VALUES}.'
            )

        sl = sex.lower()
        if sl in ('m', 'male'):
            return 1
        if sl in ('f', 'female'):
            return 2
        if sl in ('u', 'unknown'):
            return 0

        if sl == 'sex':
            raise ValueError(
                f'Unknown sex {sex!r}, did you mean to call import_pedigree with has_headers=True?'
            )
        raise ValueError(
            f'Unknown sex {sex!r}, please ensure sex is in {PedRow.ALLOWED_SEX_VALUES}'
        )

    @staticmethod
    def parse_affected_status(affected):
        """
        Parse the pedigree "AFFECTED" value:
            -9 / 0: unknown
            1: unaffected
            2: affected
        """
        if isinstance(affected, str) and not affected.isdigit():
            affected = affected.lower().strip()
            if affected in ['unknown']:
                return 0
            if affected in ['n', 'no']:
                return 1
            if affected in ['y', 'yes', 'affected']:
                return 2

        affected = int(affected)
        if affected not in PedRow.ALLOWED_AFFECTED_VALUES:
            raise ValueError(
                f'Affected value {affected} was not in expected value: {PedRow.ALLOWED_AFFECTED_VALUES}'
            )

        return affected

    def __str__(self):
        return f'PedRow: {self.individual_id} ({self.sex})'

    @staticmethod
    def order(rows: list['PedRow']) -> list['PedRow']:
        """
        Order a list of PedRows, but also validates:
        - There are no circular dependencies
        - All maternal / paternal IDs are found in the pedigree
        """
        rows_to_order: list['PedRow'] = [*rows]
        ordered = []
        seen_individuals = set()
        remaining_iterations_in_round = len(rows_to_order)

        while len(rows_to_order) > 0:
            row = rows_to_order.pop(0)
            reqs = [row.paternal_id, row.maternal_id]
            if all(r is None or r in seen_individuals for r in reqs):
                remaining_iterations_in_round = len(rows_to_order)
                ordered.append(row)
                seen_individuals.add(row.individual_id)
            else:
                remaining_iterations_in_round -= 1
                rows_to_order.append(row)

            # makes more sense to keep this comparison separate:
            #   - If remaining iterations is or AND we still have rows
            #   - Then raise an Exception
            # pylint: disable=chained-comparison
            if remaining_iterations_in_round <= 0 and len(rows_to_order) > 0:
                participant_ids = ', '.join(
                    f'{r.individual_id} ({r.paternal_id} | {r.maternal_id})'
                    for r in rows_to_order
                )
                raise ValueError(
                    "There was an issue in the pedigree, either a parent wasn't "
                    'found in the pedigree, or a circular dependency detected '
                    "(eg: someone's child is an ancestor's parent). "
                    f"Can't resolve participants with parental IDs: {participant_ids}"
                )

        return ordered

    @staticmethod
    def validate_sexes(rows: list['PedRow'], throws=True) -> bool:
        """
        Validate that individuals listed as mothers and fathers
        have either unknown sex, male if paternal, and female if maternal.

        Future note: The pedigree has a simplified view of sex, especially
        how it relates families together. This function might not handle
        more complex cases around intersex disorders within families. The
        best advice is either to skip this check, or provide sex as 0 (unknown)

        :param throws: If True is provided (default), raise a ValueError, else just return False
        """
        keyed: dict[str, PedRow] = {r.individual_id: r for r in rows}
        paternal_ids = [r.paternal_id for r in rows if r.paternal_id]
        mismatched_pat_sex = [
            pid for pid in paternal_ids if keyed[pid].sex not in (0, 1)
        ]
        maternal_ids = [r.maternal_id for r in rows if r.maternal_id]
        mismatched_mat_sex = [
            mid for mid in maternal_ids if keyed[mid].sex not in (0, 2)
        ]

        messages = []
        if mismatched_pat_sex:
            actual_values = ', '.join(
                f'{pid} ({keyed[pid].sex})' for pid in mismatched_pat_sex
            )
            messages.append('(0, 1) as they are listed as fathers: ' + actual_values)
        if mismatched_mat_sex:
            actual_values = ', '.join(
                f'{pid} ({keyed[pid].sex})' for pid in mismatched_mat_sex
            )
            messages.append('(0, 2) as they are listed as mothers: ' + actual_values)

        if messages:
            message = 'Expected individuals have sex values:' + ''.join(
                '\n\t' + m for m in messages
            )
            if throws:
                raise ValueError(message)
            logging.warning(message)
            return False

        return True

    @staticmethod
    def parse_header_order(header: list[str]):
        """
        Takes a list of unformatted headers, and returns a list of ordered init_keys

        >>> PedRow.parse_header_order(['family', 'mother', 'paternal id', 'phenotypes', 'gender'])
        ['family_id', 'maternal_id', 'paternal_id', 'affected', 'sex']

        >>> PedRow.parse_header_order(['#family id'])
        ['family_id']

        >>> PedRow.parse_header_order(['unexpected header'])
        Traceback (most recent call last):
        ValueError: Unable to identity header elements: "unexpected header"
        """
        ordered_init_keys = []
        unmatched = []
        for item in header:
            litem = item.lower().strip().strip('#')
            found = False
            for h, options in PedRow.PedRowKeys.items():
                for potential_key in options:
                    if potential_key == litem:
                        ordered_init_keys.append(h)
                        found = True
                        break
                if found:
                    break

            if not found:
                unmatched.append(item)

        if unmatched:
            # repr casts to string and quotes if applicable
            unmatched_headers_str = ', '.join(map(repr, unmatched))
            raise ValueError(
                'Unable to identity header elements: ' + unmatched_headers_str
            )

        return ordered_init_keys
