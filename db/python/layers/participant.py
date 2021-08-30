from collections import defaultdict
from enum import Enum
from typing import Dict, List, Tuple, Optional

from db.python.layers.base import BaseLayer
from db.python.layers.family import FamilyLayer
from db.python.tables.family import FamilyTable
from db.python.tables.family_participant import FamilyParticipantTable
from db.python.tables.participant import ParticipantTable
from db.python.tables.participant_phenotype import ParticipantPhenotypeTable
from db.python.tables.sample import SampleTable


class ExtraParticipantImporterHandler(Enum):
    """How to handle extra participants during metadata import"""

    FAIL = 'fail'
    IGNORE = 'ignore'
    ADD = 'add'


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

        # { external_id: internal_id }
        samples_with_no_participant_id: Dict[str, int] = dict(
            await sample_table.samples_with_missing_participants()
        )
        ext_sample_id_to_pid = {}

        unlinked_participants = await self.ptable.get_id_map_by_external_ids(
            list(samples_with_no_participant_id.keys()), allow_missing=True
        )
        external_participant_ids_to_add = set(
            samples_with_no_participant_id.keys()
        ) - set(unlinked_participants.keys())

        async with self.connection.connection.transaction():
            sample_ids_to_update = {
                samples_with_no_participant_id[external_id]: pid
                for external_id, pid in unlinked_participants.items()
            }

            for external_id in external_participant_ids_to_add:
                sample_id = samples_with_no_participant_id[external_id]
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
        self,
        headers: List[str],
        rows: List[List[str]],
        extra_participants_method: ExtraParticipantImporterHandler = ExtraParticipantImporterHandler.FAIL,
    ):
        """
        Import individual level metadata,
        currently only imports seqr metadata fields.
        """
        # pylint: disable=too-many-locals
        # currently only does the seqr metadata template

        # filter to non-comment rows
        async with self.connection.connection.transaction():
            pptable = ParticipantPhenotypeTable(self.connection)

            self._validate_individual_metadata_headers(headers)

            lheaders_to_idx_map = {h.lower(): idx for idx, h in enumerate(headers)}

            participant_id_field_idx = lheaders_to_idx_map[
                SeqrMetadataKeys.INDIVIDUAL_ID.value.lower()
            ]
            family_id_field_indx = lheaders_to_idx_map[
                SeqrMetadataKeys.FAMILY_ID.value.lower()
            ]
            self._validate_individual_metadata_participant_ids(
                rows=rows, participant_id_field_indx=participant_id_field_idx
            )

            external_participant_ids = {row[participant_id_field_idx] for row in rows}
            # TODO: determine better way to add persons if they're not here, if we add them here
            #       we risk when the samples are added, we might not link them correctly.
            # will throw if missing external ids

            # we'll allow missing (from the db) participants if we're going to add them
            allow_missing_participants = (
                extra_participants_method != ExtraParticipantImporterHandler.FAIL
            )
            external_pid_map = await self.ptable.get_id_map_by_external_ids(
                list(external_participant_ids), allow_missing=allow_missing_participants
            )
            if extra_participants_method == ExtraParticipantImporterHandler.ADD:
                missing_participant_eids = external_participant_ids - set(
                    external_pid_map.keys()
                )
                for ex_pid in missing_participant_eids:
                    external_pid_map[ex_pid] = await self.ptable.create_participant(
                        external_id=ex_pid, project=self.connection.project
                    )
            elif extra_participants_method == ExtraParticipantImporterHandler.IGNORE:
                rows = [
                    row
                    for row in rows
                    if row[participant_id_field_idx] in external_pid_map
                ]

            internal_to_external_pid_map = {v: k for k, v in external_pid_map.items()}
            pids = list(external_pid_map.values())

            # we're going to verify the family identifier if specified (and known) is correct
            # then we'll insert a row in the FamilyParticipant if unknown (by SM) and specified

            ftable = FamilyTable(self.connection)
            fptable = FamilyParticipantTable(self.connection)

            provided_pid_to_external_family = {
                external_pid_map[row[participant_id_field_idx]]: row[
                    family_id_field_indx
                ]
                for row in rows
                if row[family_id_field_indx]
            }

            external_family_ids = set(provided_pid_to_external_family.values())
            # check that all the family ids actually line up
            _, pid_to_internal_family = await fptable.get_participant_family_map(pids)
            fids = set(pid_to_internal_family.values())
            fmap_by_internal = await ftable.get_id_map_by_internal_ids(list(fids))
            fmap_from_external = await ftable.get_id_map_by_external_ids(
                list(external_family_ids), allow_missing=True
            )
            fmap_by_external = {
                **fmap_from_external,
                **{v: k for k, v in fmap_by_internal.items()},
            }
            missing_family_ids = external_family_ids - set(fmap_by_external.keys())

            family_persons_to_insert: List[Tuple[str, int]] = []
            incompatible_familes: List[str] = []
            for pid, external_family_id in provided_pid_to_external_family.items():
                if pid in pid_to_internal_family:
                    # we know the family
                    family_id = pid_to_internal_family[pid]
                    known_external_fid = fmap_by_internal[family_id]
                    if known_external_fid != external_family_id:
                        external_pid = internal_to_external_pid_map.get(pid, pid)
                        incompatible_familes.append(
                            f'{external_pid} (expected: {known_external_fid}, received: {external_family_id})'
                        )
                    # else: we're all gravy
                else:
                    # we can insert the family
                    # we'd have to INSERT the families, we'll use the
                    family_persons_to_insert.append((external_family_id, pid))

            if len(incompatible_familes) > 0:
                raise ValueError(
                    f'Specified family IDs for participants did not match what SM already knows, '
                    f'please update these in the SM database before proceeding: {", ".join(incompatible_familes)}'
                )

            if len(missing_family_ids) > 0:
                # they might not be missing
                for external_family_id in missing_family_ids:
                    new_pid = await ftable.create_family(
                        external_id=external_family_id,
                        description=None,
                        coded_phenotype=None,
                    )
                    fmap_by_internal[new_pid] = external_family_id
                    fmap_by_external[external_family_id] = new_pid

            if len(family_persons_to_insert) > 0:
                formed_rows = [
                    {
                        'family_id': fmap_by_external[external_family_id],
                        'participant_id': pid,
                        'sex': 0,
                        'affected': 0,
                    }
                    for external_family_id, pid in family_persons_to_insert
                ]
                await fptable.create_rows(formed_rows)

            storeable_keys = [k.value for k in SeqrMetadataKeys.get_storeable_keys()]
            insertable_rows = self._prepare_individual_metadata_insertable_rows(
                storeable_keys=storeable_keys,
                lheaders_to_idx_map=lheaders_to_idx_map,
                participant_id_field_idx=participant_id_field_idx,
                pid_map=external_pid_map,
                rows=rows,
            )

            await pptable.add_key_value_rows(insertable_rows)
            return True

    async def get_seqr_individual_template(
        self,
        project: int,
        external_participant_ids: Optional[List[str]] = None,
        # pylint: disable=invalid-name
        replace_with_participant_external_ids=True,
        replace_with_family_external_ids=True,
    ) -> List[List[str]]:
        """Get seqr individual level metadata template as List[List[str]]"""
        pptable = ParticipantPhenotypeTable(self.connection)
        internal_to_external_pid_map = {}
        internal_to_external_fid_map = {}

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

        flayer = FamilyLayer(self.connection)
        pid_to_fid = await flayer.get_participant_family_map(
            list(internal_to_external_pid_map.keys())
        )
        if replace_with_family_external_ids:
            ftable = FamilyTable(self.connection)
            internal_to_external_fid_map = await ftable.get_id_map_by_internal_ids(
                list(set(pid_to_fid.values()))
            )

        headers = [k.value for k in SeqrMetadataKeys.get_ordered_headers()]
        lheaders = [h.lower() for h in headers]
        rows: List[List[str]] = [headers]
        for pid, d in pid_to_features.items():
            d[SeqrMetadataKeys.INDIVIDUAL_ID.value] = internal_to_external_pid_map.get(
                pid, str(pid)
            )
            fid = pid_to_fid.get(pid, '<unknown>')
            d[SeqrMetadataKeys.FAMILY_ID.value] = internal_to_external_fid_map.get(
                fid, fid
            )
            ld = {k.lower(): v for k, v in d.items()}
            rows.append([ld.get(h, '') for h in lheaders])

        return rows

    @staticmethod
    def _validate_individual_metadata_headers(headers):

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

    @staticmethod
    def _validate_individual_metadata_participant_ids(rows, participant_id_field_indx):
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

    @staticmethod
    def _prepare_individual_metadata_insertable_rows(
        storeable_keys: List[str],
        lheaders_to_idx_map: Dict[str, int],
        participant_id_field_idx: int,
        pid_map: Dict[str, int],
        rows: List[List[str]],
    ):
        # do all the matching in lowercase space, but store in regular case space
        # pylint: disable=invalid-name
        storeable_header_col_number_tuples: List[Tuple[str, int]] = [
            (k, lheaders_to_idx_map[k.lower()])
            for k in storeable_keys
            if k.lower() in lheaders_to_idx_map
        ]

        # List of (PersonId, Key, value) to insert into the participant_phenotype table
        insertable_rows: List[Tuple[int, str, any]] = []

        for row in rows:
            external_participant_id = row[participant_id_field_idx]
            participant_id = pid_map[external_participant_id]

            for header_key, col_number in storeable_header_col_number_tuples:
                value = row[col_number]
                if value:
                    insertable_rows.append((participant_id, header_key, value))

        return insertable_rows
