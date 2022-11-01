import logging
from pydantic import BaseModel


class Family(BaseModel):
    """Family model"""

    id: int
    external_id: str
    project: int
    description: str | None = None
    coded_phenotype: str | None = None

    @staticmethod
    def from_db(d):
        """From DB fields"""
        return Family(**d)


class PedigreeRow(BaseModel):
    """
    Formed pedigree row
    """

    family_id: int | str | None
    individual_id: int | str
    paternal_id: int | str | None
    maternal_id: int | str | None
    sex: int | None
    affected: int | None = None
    notes: str | None = None

    @staticmethod
    def order(rows: list['PedigreeRow']) -> list['PedigreeRow']:
        """
        Order a list of PedRows, but also validates:
        - There are no circular dependencies
        - All maternal / paternal IDs are found in the pedigree
        """
        rows_to_order: list[PedigreeRow] = [*rows]
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
                raise Exception(
                    "There was an issue in the pedigree, either a parent wasn't "
                    'found in the pedigree, or a circular dependency detected '
                    "(eg: someone's child is an ancestor's parent). "
                    f"Can't resolve participants with parental IDs: {participant_ids}"
                )

        return ordered

    @staticmethod
    def validate_sexes(rows: list['PedigreeRow'], throws=True) -> bool:
        """
        Validate that individuals listed as mothers and fathers
        have either unknown sex, male if paternal, and female if maternal.

        Future note: The pedigree has a simplified view of sex, especially
        how it relates families together. This function might not handle
        more complex cases around intersex disorders within families. The
        best advice is either to skip this check, or provide sex as 0 (unknown)

        :param throws: If True is provided (default), raise a ValueError, else just return False
        """
        keyed: dict[str | int, PedigreeRow] = {r.individual_id: r for r in rows}
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
