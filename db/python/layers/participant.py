from collections import defaultdict
from enum import Enum
from typing import Dict, List, Tuple, Optional

from db.python.layers.base import BaseLayer
from db.python.tables.participant import ParticipantTable
from db.python.tables.participant_phenotype import ParticipantPhenotypeTable
from db.python.tables.sample import SampleTable


class SeqrMetadataKeys(Enum):
    """Describes keys for Seqr individual metadata template"""

    FAMILY_ID = 'Family ID'
    INDIVIDUAL_ID = 'Individual ID'
    HPO_TERMS_PRESENT = 'HPO Terms (present)'
    HPO_TERMS_ABSENT = 'HPO Terms (absent)'
    BIRTH_YEAR = 'Birth Year'
    DEATH_YEAR = 'Death Year'
    AGE_OF_ONSET = 'Age of Onset'
    INDIVIDUAL_NOTES = 'Individual Notes'
    CONSANGUINITY = 'Consanguinity'
    OTHER_AFFECTED_RELATIVES = 'Other Affected Relatives'
    EXPECTED_MODE_OF_INHERITANCE = 'Expected Mode of Inheritance'
    FERTILITY_MEDICATIONS = 'Fertility medications'
    INTRAUTERINE_INSEMINATION = 'Intrauterine insemination'
    IN_VITRO_FERTILIZATION = 'In vitro fertilization'
    INTRA_CYTOPLASMIC_SPERM_INJECTION = 'Intra-cytoplasmic sperm injection'
    GESTATIONAL_SURROGACY = 'Gestational surrogacy'
    DONOR_EGG = 'Donor egg'
    DONOR_SPERM = 'Donor sperm'
    MATERNAL_ANCESTRY = 'Maternal Ancestry'
    PATERNAL_ANCESTRY = 'Paternal Ancestry'
    PRE_DISCOVERY_OMIM_DISORDERS = 'Pre-discovery OMIM disorders'
    PREVIOUSLY_TESTED_GENES = 'Previously Tested Genes'
    CANDIDATE_GENES = 'Candidate Genes'

    @staticmethod
    def get_ordered_headers():
        """Get ordered headers for OUTPUT"""
        return [
            SeqrMetadataKeys.FAMILY_ID,
            SeqrMetadataKeys.INDIVIDUAL_ID,
            SeqrMetadataKeys.HPO_TERMS_PRESENT,
            SeqrMetadataKeys.HPO_TERMS_ABSENT,
            SeqrMetadataKeys.BIRTH_YEAR,
            SeqrMetadataKeys.DEATH_YEAR,
            SeqrMetadataKeys.AGE_OF_ONSET,
            SeqrMetadataKeys.INDIVIDUAL_NOTES,
            SeqrMetadataKeys.CONSANGUINITY,
            SeqrMetadataKeys.OTHER_AFFECTED_RELATIVES,
            SeqrMetadataKeys.EXPECTED_MODE_OF_INHERITANCE,
            SeqrMetadataKeys.FERTILITY_MEDICATIONS,
            SeqrMetadataKeys.INTRAUTERINE_INSEMINATION,
            SeqrMetadataKeys.IN_VITRO_FERTILIZATION,
            SeqrMetadataKeys.INTRA_CYTOPLASMIC_SPERM_INJECTION,
            SeqrMetadataKeys.GESTATIONAL_SURROGACY,
            SeqrMetadataKeys.DONOR_EGG,
            SeqrMetadataKeys.DONOR_SPERM,
            SeqrMetadataKeys.MATERNAL_ANCESTRY,
            SeqrMetadataKeys.PATERNAL_ANCESTRY,
            SeqrMetadataKeys.PRE_DISCOVERY_OMIM_DISORDERS,
            SeqrMetadataKeys.PREVIOUSLY_TESTED_GENES,
            SeqrMetadataKeys.CANDIDATE_GENES,
        ]

    @staticmethod
    def get_storeable_keys():
        """
        Get list of keys that we'll store in participant phenotype db
        """
        return [
            SeqrMetadataKeys.HPO_TERMS_PRESENT,
            SeqrMetadataKeys.HPO_TERMS_ABSENT,
            SeqrMetadataKeys.BIRTH_YEAR,
            SeqrMetadataKeys.DEATH_YEAR,
            SeqrMetadataKeys.AGE_OF_ONSET,
            SeqrMetadataKeys.INDIVIDUAL_NOTES,
            SeqrMetadataKeys.CONSANGUINITY,
            SeqrMetadataKeys.OTHER_AFFECTED_RELATIVES,
            SeqrMetadataKeys.EXPECTED_MODE_OF_INHERITANCE,
            SeqrMetadataKeys.FERTILITY_MEDICATIONS,
            SeqrMetadataKeys.INTRAUTERINE_INSEMINATION,
            SeqrMetadataKeys.IN_VITRO_FERTILIZATION,
            SeqrMetadataKeys.INTRA_CYTOPLASMIC_SPERM_INJECTION,
            SeqrMetadataKeys.GESTATIONAL_SURROGACY,
            SeqrMetadataKeys.DONOR_EGG,
            SeqrMetadataKeys.DONOR_SPERM,
            SeqrMetadataKeys.MATERNAL_ANCESTRY,
            SeqrMetadataKeys.PATERNAL_ANCESTRY,
            SeqrMetadataKeys.PRE_DISCOVERY_OMIM_DISORDERS,
            SeqrMetadataKeys.PREVIOUSLY_TESTED_GENES,
            SeqrMetadataKeys.CANDIDATE_GENES,
        ]


class ParticipantLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection):
        super().__init__(connection)
        self.ptable = ParticipantTable(connection=connection)

    async def fill_in_missing_participants(self):
        """Update the sequencing status from the internal sample id"""
        sample_table = SampleTable(connection=self.connection)

        samples_with_no_participant_id: Dict[str, int] = dict(
            await sample_table.samples_with_missing_participants()
        )
        ext_sample_id_to_pid = {}

        async with self.connection.connection.transaction():
            sample_ids_to_update = {}
            for external_id, sample_id in samples_with_no_participant_id.items():
                participant_id = await self.ptable.create_participant(
                    external_id=external_id
                )
                ext_sample_id_to_pid[external_id] = participant_id
                sample_ids_to_update[sample_id] = participant_id

            await sample_table.update_many_participant_ids(
                list(sample_ids_to_update.keys()), list(sample_ids_to_update.values())
            )

        return f'Updated {len(sample_ids_to_update)} records'

    async def generic_individual_metadata_importer(
        self, headers: List[str], rows: List[List[str]]
    ):
        """
        Import individual level metadata,
        currently only imports seqr metadata fields.
        """
        # pylint: disable=too-many-locals
        # currently only does the seqr metadata template

        pptable = ParticipantPhenotypeTable(self.connection)

        lheader_set = set(h.lower() for h in headers)
        mandatory_keys = [SeqrMetadataKeys.INDIVIDUAL_ID.value]
        missing_keys = [h for h in mandatory_keys if h.lower() not in lheader_set]
        if len(missing_keys) > 0:
            missing_keys_str = ', '.join(missing_keys)
            raise ValueError(
                f'Import did not include mandatory keys: {missing_keys_str}'
            )

        recognised_keys = set(k.value.lower() for k in SeqrMetadataKeys)
        unrecognised_keys = [h for h in headers if h.lower() not in recognised_keys]
        if len(unrecognised_keys) > 0:
            unrecognised_keys_str = ', '.join(unrecognised_keys)
            raise ValueError(
                f'Import did not recognise the keys {unrecognised_keys_str}'
            )

        headers_to_idx_map = {h.lower(): idx for idx, h in enumerate(headers)}

        participant_id_field_indx = headers_to_idx_map[
            SeqrMetadataKeys.INDIVIDUAL_ID.value.lower()
        ]

        # validate persons
        external_id_to_row_number = defaultdict(list)
        for idx, row in enumerate(rows):
            external_id_to_row_number[row[participant_id_field_indx]].append(idx + 1)

        pids_with_duplicates = {
            eid: rownumbers
            for eid, rownumbers in external_id_to_row_number.items()
            if len(rownumbers) > 1
        }

        if len(pids_with_duplicates) > 0:
            raise ValueError(
                f'There were duplicate participants for {{external_id: row_numbers}}: {pids_with_duplicates}'
            )

        if '' in pids_with_duplicates or None in pids_with_duplicates:
            rows_with_empty_pids = sorted(
                row_numbers
                for k in ('', None)
                for row_numbers in pids_with_duplicates[k]
                if k in pids_with_duplicates
            )
            raise ValueError(
                f'Empty values found for participants in rows {rows_with_empty_pids}'
            )

        external_participant_ids = set(external_id_to_row_number.keys())

        # will throw if missing external ids
        # TODO: determine better way to add persons if they're not here, if we add them here
        #       we risk when the samples are added, we might not link them correctly.
        pid_map = await self.ptable.get_id_map_by_external_ids(
            list(external_participant_ids), allow_missing=False
        )

        # do all the matching in lowercase space, but store in regular case space
        # pylint: disable=invalid-name
        storeable_header_col_number_tuples: List[Tuple[str, int]] = [
            (k.value, headers_to_idx_map[k.value.lower()])
            for k in SeqrMetadataKeys.get_storeable_keys()
            if k.value.lower() in lheader_set
        ]

        # List of (PersonId, Key, value) to insert into the participant_phenotype table
        insertable_rows: List[Tuple[int, str, any]] = []

        for row in rows:
            external_participant_id = row[participant_id_field_indx]
            participant_id = pid_map[external_participant_id]

            for header_key, col_number in storeable_header_col_number_tuples:
                value = row[col_number]
                if value:
                    insertable_rows.append((participant_id, header_key, value))

        await pptable.add_key_value_rows(insertable_rows)
        return True

    async def get_seqr_individual_template(
        self,
        project: int,
        external_participant_ids: Optional[List[str]] = None,
        # pylint: disable=invalid-name
        replace_with_participant_external_ids=True,
    ) -> List[List[str]]:
        """Get seqr individual level metadata template as List[List[str]]"""
        pptable = ParticipantPhenotypeTable(self.connection)
        internal_to_external_pid_map = {}
        if external_participant_ids:
            pids = await self.ptable.get_id_map_by_external_ids(
                external_participant_ids, allow_missing=False
            )
            pid_to_features = await pptable.get_key_value_rows_for_participant_ids(
                participant_ids=list(pids.values())
            )
            if replace_with_participant_external_ids:
                internal_to_external_pid_map = {v: k for k, v in pids.items()}
        else:
            pid_to_features = await pptable.get_key_value_rows_for_all_participants(
                project=project
            )
            if replace_with_participant_external_ids:
                internal_to_external_pid_map = (
                    await self.ptable.get_id_map_by_internal_ids(
                        list(pid_to_features.keys())
                    )
                )

        headers = [k.value for k in SeqrMetadataKeys.get_ordered_headers()]
        lheaders = [h.lower() for h in headers]
        rows: List[List[str]] = [headers]
        for pid, d in pid_to_features.items():
            d[SeqrMetadataKeys.INDIVIDUAL_ID.value] = internal_to_external_pid_map.get(
                pid, str(pid)
            )
            ld = {k.lower(): v for k, v in d.items()}
            rows.append([ld.get(h, '') for h in lheaders])

        return rows
