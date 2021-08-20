from typing import List, Union, Optional

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.family import FamilyTable
from db.python.tables.family_participant import FamilyParticipantTable
from db.python.tables.participant import ParticipantTable
from db.python.tables.project import ProjectId


class PedRow:
    """Class for capturing a row in a pedigree"""

    PedRowKeys = {
        # seqr individual template:
        # Family ID, Individual ID, Paternal ID, Maternal ID, Sex, Affected, Status, Notes
        'family_id': {'familyid', 'family id', 'family', 'family_id'},
        'individual_id': {'individualid', 'id', 'individual_id', 'individual id'},
        'paternal_id': {'paternal_id', 'paternal id', 'paternalid', 'father'},
        'maternal_id': {'maternal_id', 'maternal id', 'maternalid', 'mother'},
        'sex': {'sex', 'gender'},
        'affected': {'phenotype', 'affected', 'phenotypes', 'affected status'},
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
        self.family_id = family_id
        self.individual_id = individual_id
        self.paternal_id = None
        self.maternal_id = None
        if paternal_id is not None and paternal_id not in ('0', 0):
            self.paternal_id = paternal_id
        if maternal_id is not None and maternal_id not in ('0', 0):
            self.maternal_id = maternal_id
        self.sex = self.parse_sex(sex)
        self.affected = int(affected)
        self.notes = notes

    @staticmethod
    def parse_sex(sex: Union[str, int]):
        """
        Parse the pedigree SEX value:
            0: unknown
            1: male (also accepts 'm')
            2: female (also accepts 'f')

        """
        if isinstance(sex, str) and sex.isdigit():
            sex = int(sex)
        if isinstance(sex, int):
            if 0 <= sex <= 2:
                return sex
            raise ValueError(f'Sex value ({sex}) was not an expected value [0, 1, 2].')

        sl = sex.lower()
        if sl == 'm':
            return 1
        if sl == 'f':
            return 2
        raise ValueError(f'Unknown sex "{sex}", please ensure sex is in (0, 1, 2)')

    def __str__(self):
        return f'PedRow: {self.individual_id} ({self.sex})'

    @staticmethod
    def order(rows: List['PedRow']) -> List['PedRow']:
        """
        Order a list of PedRows, but also validates:
        - There are no circular dependencies
        - All maternal / paternal IDs are found in the pedigree
        """
        rows_to_order: List[PedRow] = [*rows]
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
                participant_ids = ', '.join(r.individual_id for r in rows_to_order)
                raise Exception(
                    "There was an issue in the pedigree, either a parent wasn't found in the pedigree, "
                    "or a circular dependency detected (eg: someone's child is an ancestor's parent). "
                    f"Can't resolve participants: {participant_ids}"
                )

        return ordered

    @staticmethod
    def parse_header_order(header: List[str]):
        """
        Takes a list of unformatted headers, and returns a list of ordered init_keys

        >>> PedRow.parse_header_order(['family', 'mother', 'paternal id', 'phenotypes', 'gender'])
        ['family_id', 'maternal_id', 'paternal_id', 'affected', 'sex']
        """
        ordered_init_keys = []
        for item in header:
            litem = item.lower()
            found = False
            for h, options in PedRow.PedRowKeys.items():
                for potential_key in options:
                    if potential_key == litem:
                        ordered_init_keys.append(h)
                        found = True
                        break
                if found:
                    break

        return ordered_init_keys


class FamilyLayer(BaseLayer):
    """Layer for import logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.ftable = FamilyTable(connection)
        self.fptable = FamilyParticipantTable(self.connection)

    async def get_pedigree(
        self,
        project: ProjectId,
        family_ids: List[int] = None,
        # pylint: disable=invalid-name
        replace_with_participant_external_ids=False,
        # pylint: disable=invalid-name
        replace_with_family_external_ids=False,
        empty_participant_value='',
    ) -> List[List[Optional[str]]]:
        """
        Generate pedigree file for ALL families in project
        (unless internal_family_ids is specified).

        Use internal IDs unless specific options are specified.
        """

        # this is important because a PED file MUST be ordered like this
        ordered_keys = [
            'family_id',
            'participant_id',
            'paternal_participant_id',
            'maternal_participant_id',
            'sex',
            'affected',
        ]
        pid_fields = {
            'participant_id',
            'paternal_participant_id',
            'maternal_participant_id',
        }

        rows = await self.fptable.get_rows(project=project, family_ids=family_ids)
        pmap, fmap = {}, {}
        if replace_with_participant_external_ids:
            participant_ids = set(
                s
                for r in rows
                for s in [r[pfield] for pfield in pid_fields]
                if s is not None
            )
            ptable = ParticipantTable(connection=self.connection)
            pmap = await ptable.get_id_map_by_internal_ids(list(participant_ids))

        if replace_with_family_external_ids:
            family_ids = set(r['family_id'] for r in rows if r['family_id'] is not None)
            fmap = await self.ftable.get_id_map_by_internal_ids(list(family_ids))

        formatted_rows = []
        for row in rows:
            formatted_row = []
            for field in ordered_keys:
                value = row[field]
                if field == 'family_id':
                    formatted_row.append(fmap.get(value, value))
                elif field in pid_fields:
                    formatted_row.append(
                        pmap.get(value, value) or empty_participant_value
                    )
                else:
                    formatted_row.append(value)
            formatted_rows.append(formatted_row)

        return formatted_rows

    async def import_pedigree(self, header: Optional[List[str]], rows: List[List[str]]):
        """
        Import pedigree file
        """
        if header is None:
            _header = PedRow.default_header()
        else:
            _header = PedRow.parse_header_order(header)

        if len(rows) == 0:
            return None

        max_row_length = len(rows[0])
        if max_row_length > len(_header):
            raise ValueError(
                f"The parsed header {_header} isn't long enough "
                f'to cover row length ({len(_header)} < {len(rows[0])})'
            )
        if len(_header) > max_row_length:
            _header = _header[:max_row_length]

        pedrows: List[PedRow] = [
            PedRow(**{_header[i]: r[i] for i in range(len(_header))}) for r in rows
        ]
        # this validates a lot of the pedigree too
        pedrows: List[PedRow] = PedRow.order(pedrows)

        external_family_ids = set(r.family_id for r in pedrows)
        # get set of all individual, paternal, maternal participant ids
        external_participant_ids = set(
            pid
            for r in pedrows
            for pid in [r.individual_id, r.paternal_id, r.maternal_id]
            if pid
        )

        participant_table = ParticipantTable(self.connection)

        external_family_id_map = await self.ftable.get_id_map_by_external_ids(
            list(external_family_ids), allow_missing=True
        )
        missing_external_family_ids = [
            f for f in external_family_ids if f not in external_family_id_map
        ]
        # these will fail if any of them are missing
        external_participant_ids = await participant_table.get_id_map_by_external_ids(
            list(external_participant_ids)
        )

        async with self.connection.connection.transaction():
            for external_family_id in missing_external_family_ids:
                internal_family_id = await self.ftable.create_family(
                    external_id=external_family_id,
                    description=None,
                    coded_phenotype=None,
                )
                external_family_id_map[external_family_id] = internal_family_id

            # now let's map participants back
            insertable_rows = [
                {
                    'family_id': external_family_id_map[row.family_id],
                    'participant_id': external_participant_ids[row.individual_id],
                    'paternal_participant_id': external_participant_ids.get(
                        row.paternal_id
                    ),
                    'maternal_participant_id': external_participant_ids.get(
                        row.maternal_id
                    ),
                    'sex': row.sex,
                    'affected': row.affected,
                    'notes': row.notes,
                }
                for row in pedrows
            ]
            await self.fptable.create_rows(insertable_rows)

        return True
