from typing import List, Dict, Iterable, Union, Optional

from db.python.tables.family import FamilyTable
from db.python.tables.family_participant import FamilyParticipantTable
from db.python.tables.participant import ParticipantTable
from models.enums import SampleType, SequenceType, SequenceStatus
from models.models.sequence import SampleSequencing

from db.python.tables.sample_map import SampleMapTable
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing import SampleSequencingTable

from db.python.layers.base import BaseLayer


class PedRow:
    PedRowKeys = {
        # seqr individual template:
        # Family ID, Individual ID, Paternal ID, Maternal ID, Sex, Affected, Status, Notes
        'family_id': {'familyid', "family id", 'family', 'family_id'},
        'individual_id': {'individualid', 'id', 'individual_id', 'individual id'},
        'paternal_id': {'paternal_id', 'paternal id', 'paternalid', 'father'},
        'maternal_id': {'maternal_id', 'maternal id', 'maternalid', 'mother'},
        'sex': {'sex', 'gender'},
        'phenotypes': {'affected', 'phenotypes', 'phenotype'},
        'notes': {'notes'},
    }

    @staticmethod
    def default_header():
        return [
            'family id',
            'individual id',
            'paternal id',
            'maternal id',
            'sex',
            'affected',
        ]

    def __init__(
        self,
        family_id,
        individual_id,
        paternal_id,
        maternal_id,
        sex,
        phenotype,
        notes=None,
    ):
        self.family_id = family_id
        self.individual_id = individual_id
        self.paternal_id = paternal_id if paternal_id else None
        self.maternal_id = maternal_id if maternal_id else None
        self.sex = self.parse_sex(sex)
        self.phenotype = int(phenotype)
        self.notes = notes

    @staticmethod
    def parse_sex(sex: Union[str, int]):
        if isinstance(sex, int):
            return sex
        if sex.isdigit():
            return int(sex)

        sl = sex.lower()
        if sl == 'm':
            return 1
        if sl == 'f':
            return 2
        raise ValueError(f'Unknown sex "{sex}", please ensure sex is in (0, 1, 2)')

    def __str__(self):
        return f"PedRow: {self.individual_id} ({self.sex})"

    @staticmethod
    def order(rows: List['PedRow']):

        rows_to_order: List[PedRow] = [*rows]
        ordered = []
        seen_individuals = set()
        iterations_remaining_to_next_add = len(rows_to_order)

        while len(rows_to_order) > 0:
            row = rows_to_order.pop(0)
            reqs = [row.paternal_id, row.maternal_id]
            if all(r is None or r in seen_individuals for r in reqs):
                iterations_remaining_to_next_add = len(rows_to_order)
                ordered.append(row)
                seen_individuals.add(row.individual_id)
            else:
                iterations_remaining_to_next_add -= 1
                rows_to_order.append(row)

            if iterations_remaining_to_next_add <= 0 and len(rows_to_order) > 0:
                participant_ids = ', '.join(r.individual_id for r in rows_to_order)
                raise Exception(
                    "Circular dependency detected (eg: someone's child is an ancestor's parent). "
                    f"Can't resolve participants: {participant_ids}"
                )

        return ordered

    @staticmethod
    def parse_header_order(header: List[str]):
        """
        Takes a list of unformatted headers, and returns a list of ordered init_keys

        >>> PedRow.parse_header_order(['family', 'mother', 'paternal id', 'affected', 'gender'])
        ['family_id', 'maternal_id', 'paternal_id', 'phenotypes', 'sex']
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
                f"to cover row length ({len(_header)} < {len(rows[0])})"
            )

        rows = [PedRow(**{_header[i]: r[i] for i in range(len(_header))}) for r in rows]
        rows = PedRow.order(rows)

        external_family_ids = set(r.family_id for r in rows)
        # get set of all individual, paternal, maternal participant ids
        external_participant_ids = set(
            pid
            for r in rows
            for pid in [r.individual_id, r.paternal_id, r.maternal_id]
            if pid
        )

        family_table = FamilyTable(self.connection)
        participant_table = ParticipantTable(self.connection)
        family_participant_table = FamilyParticipantTable(self.connection)

        external_family_id_map = await family_table.get_id_map_by_external_ids(
            list(external_family_ids), allow_missing=True
        )
        missing_external_family_ids = [
            f for f in external_family_ids if f not in external_family_id_map
        ]
        # these will fail if any of them are missing
        external_participant_ids = await participant_table.get_id_map_by_external_ids(
            list(external_participant_ids)
        )

        with self.connection.connection.transaction():
            for external_family_id in missing_external_family_ids:
                internal_family_id = await family_table.create_family(
                    external_id=external_family_id,
                    description=None,
                    coded_phenotype=None,
                )
                external_family_id_map[external_family_id] = internal_family_id

            # now let's map participants back
            for row in rows:
                await family_participant_table.create_row(
                    family_id=external_family_id_map[row.family_id],
                    participant_id=external_participant_ids[row.individual_id],
                    paternal_id=external_participant_ids[row.paternal_id]
                    if row.paternal_id
                    else None,
                    maternal_id=external_participant_ids[row.maternal_id]
                    if row.maternal_id
                    else None,
                    sex=row.sex,
                    affected=row.phenotype,
                    notes=row.notes,
                )

        return True
