# pylint: disable=missing-function-docstring,missing-class-docstring

import csv
import json
import math
from typing import Dict, List, Any
from collections import defaultdict
from enum import Enum

from db.python.layers.family import PedRow


class Relationship(Enum):
    # direct
    MOTHER = 'mother'
    FATHER = 'father'
    # correctable with basic inference
    SON = 'son'
    DAUGHTER = 'daughter'

    # 1 layer, could synthesize
    SISTER = 'sister'
    BROTHER = 'brother'

    # 2 layers - uncorrectable
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


class PedRowRules(PedRow):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rules = []


def main(file):
    participant_map: Dict[Any, PedRowRules] = {}
    relations: List[Relation] = []

    with open(file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            pid = row['participantid']
            relationship = Relationship.parse(row['inter-related'])
            relation = Relation(
                participant_1=pid, participant_2=row['IR_ID'], relationship=relationship
            )
            relations.append(relation)
            sex = PedRowRules.parse_sex(row['sex'].strip())
            if pid not in participant_map:
                pedrow = PedRowRules(
                    family_id='',
                    individual_id=pid,
                    paternal_id=None,
                    maternal_id=None,
                    sex=sex,
                    affected=-9,
                )
                participant_map[pedrow.individual_id] = pedrow

    # fill in maternal / paternal relations

    conflicts = mutating_build_direct_parent_relationship(relations, participant_map)
    conflicts.extend(mutating_check_children(relations, participant_map))

    # we can check grandparents
    warnings = check_grandparents(relations, participant_map)
    warnings.extend(check_siblings(relations, participant_map))

    sex_conflicts = check_sexes(relations, participant_map)

    if sex_conflicts:
        print('Sex conflicts', json.dumps(sex_conflicts, indent=4))

    if conflicts:
        print('\n'.join(conflicts))
        raise ValueError(str(conflicts))

    if warnings:
        print('\n'.join(warnings))

    pedrows = PedRow.order(list(participant_map.values()))

    mutating_synthesize_family_ids(
        ordered_individuals=pedrows, individuals_map=participant_map
    )

    rows = [PedRow.row_header()] + [
        [str(s) for s in r.as_row(empty_participant_value=0)] for r in pedrows
    ]
    print('\n'.join('\t'.join(r) for r in rows))


def mutating_build_direct_parent_relationship(
    relations: List[Relation], participant_map: Dict[Any, PedRowRules]
):
    conflicts = []
    for rule in relations:
        # participant_1's {relationship} is participant_2
        if rule.relationship == Relationship.MOTHER:
            child = participant_map[rule.participant_1]
            if not child.maternal_id:
                child.maternal_id = rule.participant_2
                child.rules.append(rule)
            elif child.maternal_id != rule.participant_2:
                conflicts.append(
                    f'{rule.participant_2} is NOT the MOTHER of {rule.participant_1}, correct: {child.maternal_id}'
                )
            else:
                print('relationship is correct')
        elif rule.relationship == Relationship.FATHER:
            child = participant_map[rule.participant_1]
            if not child.paternal_id:
                child.paternal_id = rule.participant_2
                child.rules.append(rule)
            elif child.paternal_id != rule.participant_2:
                conflicts.append(
                    f'{rule.participant_2} is NOT the FATHER of {rule.participant_1}, correct: {child.paternal_id}'
                )
            else:
                print('relationship is correct')

    return conflicts


def check_sexes(
    relations: List[Relation], participant_map: Dict[Any, PedRowRules]
) -> Dict[Any, List[str]]:
    sex_conflict = defaultdict(list)
    # expect participant 2 to be female
    for rule in relations:
        pid = rule.participant_1
        if (
            Relationship.p1_is_female(rule.relationship)
            and participant_map[rule.participant_2].sex != 2
        ):
            sex_conflict[pid].append(
                f'Expected female as participant is {rule.relationship.value} of {rule.participant_2}'
            )
        if (
            Relationship.p1_is_male(rule.relationship)
            and participant_map[rule.participant_2].sex != 1
        ):
            sex_conflict[pid].append(
                f'Expected male as participant is {rule.relationship.value} of {rule.participant_2}'
            )

    return sex_conflict


def mutating_check_children(
    relations: List[Relation], participant_map: Dict[Any, PedRowRules]
) -> List[str]:
    conflicts = []

    for rule in relations:
        if rule.relationship in (Relationship.DAUGHTER, Relationship.SON):
            child = participant_map[rule.participant_2]
            parent = participant_map[rule.participant_1]

            if not parent:
                conflicts.append(f'{rule.participant_1} does not exist')
                continue

            if parent.sex == 1:
                # male
                if child.paternal_id and child.paternal_id != parent.individual_id:
                    conflicts.append(
                        f'{rule.participant_1}\'s {rule.relationship.value} is NOT {rule.participant_2}'
                    )
                elif not child.paternal_id:
                    print(
                        f'Correcting {rule.relationship.value}, setting {child.individual_id}.paternal_id to {parent.individual_id}'
                    )
                    child.paternal_id = rule.participant_1
            elif parent.sex == 2:
                if child.maternal_id and child.maternal_id != parent.individual_id:
                    conflicts.append(
                        f'{rule.participant_1}\'s {rule.relationship.value} is NOT {rule.participant_2}'
                    )
                elif not child.maternal_id:
                    print(
                        f'Correcting {rule.relationship.value}, setting {child.individual_id}.maternal_id to {parent.individual_id}'
                    )
                    child.maternal_id = rule.participant_1

    return conflicts


def check_siblings(
    relations: List[Relation], participant_map: Dict[Any, PedRowRules]
) -> List[str]:
    warnings = []
    for rule in relations:

        if rule.relationship not in (Relationship.BROTHER, Relationship.SISTER):
            continue

        # we won't check sex, just that the two people share both parents
        p1 = participant_map[rule.participant_1]
        p2 = participant_map[rule.participant_2]

        mothers_match = p1.maternal_id == p2.maternal_id
        fathers_match = p1.paternal_id == p2.paternal_id
        have_mothers = p1.maternal_id and p2.maternal_id
        have_fathers = p1.paternal_id and p2.paternal_id

        if not (mothers_match and fathers_match and have_mothers and have_fathers):
            message = f'Expected {rule.participant_1} and {rule.participant_2} parents to match'
            if not mothers_match:
                message += (
                    f', mothers do not match ({p1.maternal_id} != {p2.maternal_id})'
                )
            elif not have_mothers:
                message += f', do not have mothers'
            if not fathers_match:
                message += (
                    f', fathers do not match ({p1.paternal_id} != {p2.paternal_id})'
                )
            elif not have_fathers:
                message += f', do not have fathers'
            warnings.append(message)

    return warnings


def check_grandparents(
    relations: List[Relation], participant_map: Dict[Any, PedRowRules]
) -> List[str]:
    warnings = []
    confirmed = 0

    for rule in relations:

        if rule.relationship == Relationship.GRANDMOTHER:
            self = participant_map[rule.participant_1]
            # maternal / paternal grandmother
            parents = [self.maternal_id, self.paternal_id]
            grandmothers = [
                participant_map[p].maternal_id
                for p in parents
                if p and participant_map[p].maternal_id
            ]
            if rule.participant_2 not in grandmothers:
                warnings.append(
                    f'{rule.participant_2} is NOT the {rule.relationship.value} of {rule.participant_1}'
                )
            else:
                confirmed += 1
        if rule.relationship == Relationship.GRANDFATHER:
            self = participant_map[rule.participant_1]
            # maternal / paternal grandfather
            parents = [self.maternal_id, self.paternal_id]
            grandfathers = [
                participant_map[p].paternal_id
                for p in parents
                if p and participant_map[p].paternal_id
            ]
            if rule.participant_2 not in grandfathers:
                warnings.append(
                    f'{rule.participant_2} is NOT the {rule.relationship.value} of {rule.participant_1}'
                )
            else:
                confirmed += 1

    if confirmed:
        print(f'Confirmed {confirmed} grand relationships')

    return warnings


def filter_falsey(*args: List[Any]):
    return [i for i in args if i]


def mutating_synthesize_family_ids(
    ordered_individuals: List[PedRowRules],
    individuals_map: Dict[Any, PedRowRules],
    prefix: str = 'FAM',
):
    individual_to_family_map: Dict[Any, int] = {}
    family_id_to_individuals = defaultdict(list)
    fam_counter = 1

    for individual in ordered_individuals[::-1]:
        # start backwards because ordered individuals have more parents,
        # this will reduce the need for consolidation
        related_ids = filter_falsey(
            individual.individual_id, individual.maternal_id, individual.paternal_id
        )
        family_ids = {
            individual_to_family_map[iid]
            for iid in related_ids
            if iid in individual_to_family_map
        }
        if len(family_ids) == 0:
            # synthesize!
            for iid in related_ids:
                individual_to_family_map[iid] = fam_counter
                family_id_to_individuals[fam_counter].append(iid)
            fam_counter += 1
        elif len(family_ids) == 1:
            family_id = next(iter(family_ids))
            for iid in related_ids:
                individual_to_family_map[iid] = family_id
                family_id_to_individuals[family_id].append(iid)
        else:
            # consolidate to first
            family_id, *family_ids_to_consolidate = list(family_ids)
            for family_id_to_consolidate in family_ids_to_consolidate:
                for iid in family_id_to_individuals[family_id_to_consolidate]:
                    individual_to_family_map[iid] = family_id
                    family_id_to_individuals[family_id].append(iid)

                family_id_to_individuals.pop(family_id_to_consolidate)

    n_family_ids = len(set(individual_to_family_map.values()))
    n_digits = max(3, round(math.log10(n_family_ids)))

    # the current family_ids will skip due to the consolidation, so let's
    # just take the index as the new_family_id
    for new_family_id, iids in enumerate(family_id_to_individuals.values(), start=1):
        for iid in iids:
            str_fam_id = prefix + str(new_family_id).zfill(n_digits)
            individuals_map[iid].family_id = str_fam_id


if __name__ == '__main__':
    main(
        '/Users/michael.franklin/source/sample-metadata/scripts/prophecy-interelated-data.tsv'
    )
