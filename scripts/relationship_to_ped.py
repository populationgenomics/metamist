import csv
import typing
from collections import defaultdict
from enum import Enum

from db.python.layers.family import PedRow


class Relationship(Enum):
    # direct
    MOTHER = 'mother'
    FATHER = 'father'
    SON = 'son'
    DAUGHTER = 'daughter'

    # 1 layer
    SISTER = 'sister'
    BROTHER = 'brother'
    GRANDMOTHER = 'grandmother'
    GRANDDAUGHTER = 'granddaughter'
    GRANDSON = 'grandson'
    GRANDFATHER = 'grandfather'

    @staticmethod
    def parse(value):
        if isinstance(value, Relationship):
            return value
        value = value.lower()
        if value in ('mother', 'mom', 'mum'):
            return Relationship.MOTHER
        if value in ('father', 'dad'):
            return Relationship.FATHER
        return Relationship(value)

    @staticmethod
    def p1_is_female(relationship: 'Relationship') -> bool:
        return relationship in (
            Relationship.MOTHER,
            Relationship.GRANDMOTHER,
            Relationship.DAUGHTER,
            Relationship.GRANDDAUGHTER,
        )

    @staticmethod
    def p1_is_male(relationship: 'Relationship') -> bool:
        return relationship in (
            Relationship.FATHER,
            Relationship.GRANDFATHER,
            Relationship.SON,
            Relationship.GRANDSON,
        )




class Relation:
    def __init__(self, participant_1, participant_2, relationship: Relationship):
        self.participant_1 = participant_1
        self.participant_2 = participant_2
        self.relationship = relationship

def main(file):
    participant_map = {}
    relations: typing.List[Relation] = []

    sex_conflict = defaultdict(list)

    with open(file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            pid = row['participantid']
            relationship = Relationship.parse(row['inter-related'])
            relation = Relation(
                participant_1=pid,
                participant_2=row['IR_ID'], relationship=relationship
            )
            relations.append(relation)
            sex = PedRow.parse_sex(row['sex'].strip())
            if pid not in participant_map:

                pedrow = PedRow(
                    family_id='',
                    individual_id=pid,
                    paternal_id=None,
                    maternal_id=None,
                    sex=sex,
                    affected=-9,
                )
                participant_map[pedrow.individual_id] = pedrow

    # fill in maternal / paternal relations
    relationship = None
    relation = None
    conflicts = []
    for row in relations:
        if row.relationship == Relationship.MOTHER:
            child = participant_map[row.participant_1]
            if not child.maternal_id:
                child.maternal_id = row.participant_2
            elif child.maternal_id != row.participant_2:
                conflicts.append(
                    f'{row.participant_2} is NOT the MOTHER of {row.participant_1}, correct: {child.maternal_id}'
                )
            else:
                print('relationship is correct')
        elif row.relationship == Relationship.FATHER:
            child = participant_map[row.participant_1]
            if not child.paternal_id:
                child.paternal_id = row.participant_2
            elif child.paternal_id != row.participant_2:
                conflicts.append(
                    f'{row.participant_2} is NOT the FATHER of {row.participant_1}, correct: {child.paternal_id}'
                )
            else:
                print('relationship is correct')

        # expect participant 2 to be female
        if Relationship.p1_is_female(row.relationship) and participant_map[row.participant_2].sex != 2:
            sex_conflict[pid].append(
                f'Expected female as participant is {row.relationship.value} of {row.participant_2}'
            )
        if Relationship.p1_is_male(row.relationship) and participant_map[row.participant_2].sex != 1:
            sex_conflict[pid].append(
                f'Expected male as participant is {row.relationship.value} of {row.participant_2}'
            )

    for row in relations:
        if row.relationship in (Relationship.DAUGHTER, Relationship.SON):
            child = participant_map[row.participant_1]
            ids = (child.paternal_id,  child.maternal_id)
            if all(ids) and row.participant_2 not in ids:
                conflicts.append(
                    f'{row.participant_2} is NOT the {row.relationship.value} of {row.participant_1}'
                )
        if row.relationship == Relationship.GRANDMOTHER:
            self = participant_map[row.participant_1]
            # maternal / paternal grandmother
            parents = [self.maternal_id, self.paternal_id]
            grandmothers = [participant_map[p].maternal_id for p in parents if p and participant_map[p].maternal_id]
            if not row.participant_2 in grandmothers:
                conflicts.append(
                    f'{row.participant_2} is NOT the {row.relationship.value} of {row.participant_1}'
                )
        if row.relationship == Relationship.GRANDFATHER:
            self = participant_map[row.participant_1]
            # maternal / paternal grandfather
            parents = [self.maternal_id, self.paternal_id]
            grandfathers = [participant_map[p].paternal_id for p in parents if p and participant_map[p].paternal_id]
            if not row.participant_2 in grandfathers:
                conflicts.append(
                    f'{row.participant_2} is NOT the {row.relationship.value} of {row.participant_1}'
                )

        if row.relationship in (Relationship.GRANDDAUGHTER, Relationship.GRANDSON):

            # difficult because it's not indexed this way
            pass

    if sex_conflict:
        print('Sex conflicts', sex_conflict)
    if conflicts:
        print("\n".join(conflicts))
        raise ValueError(str(conflicts))



    pedrows = PedRow.order(list(participant_map.values()))
    print('\n'.join('\t'.join(str(s) for s in r.as_row(empty_participant_value=0)) for r in pedrows))


if __name__ == "__main__":
    file = '/Users/michael.franklin/source/sample-metadata/scripts/prophecy-interelated-data.tsv'
    main(file)
