# pylint: disable=invalid-name
from typing import Dict, List, Tuple, Optional, Any

import re
from collections import defaultdict
from enum import Enum

from pydantic import BaseModel

from db.python.connect import NotFoundError
from db.python.layers.base import BaseLayer
from db.python.layers.sample import (
    SampleBatchUpsert,
    SampleBatchUpsertBody,
    SampleLayer,
)
from db.python.tables.family import FamilyTable
from db.python.tables.family_participant import FamilyParticipantTable
from db.python.tables.participant import ParticipantTable
from db.python.tables.participant_phenotype import ParticipantPhenotypeTable
from db.python.tables.sample import SampleTable
from db.python.utils import ProjectId, split_generic_terms
from models.models.participant import Participant

HPO_REGEX_MATCHER = re.compile(r'HP\:\d+$')


class ParticipantUpdateModel(BaseModel):
    """Update participant model"""

    external_id: Optional[str] = None
    reported_sex: Optional[int] = None
    reported_gender: Optional[str] = None
    karyotype: Optional[str] = None
    meta: Optional[Dict] = None


class ParticipantUpsert(ParticipantUpdateModel):
    """Update model for sample with sequences list"""

    id: Optional[int]
    samples: List[SampleBatchUpsert]


class ParticipantUpsertBody(BaseModel):
    """Upsert model for batch Participants"""

    participants: List[ParticipantUpsert]


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
    def get_key_parsers():
        """Get specific parsers for individual fields"""
        return {
            SeqrMetadataKeys.AGE_OF_ONSET: SeqrMetadataKeys.parse_age_of_onset,
            SeqrMetadataKeys.HPO_TERMS_ABSENT: lambda q: ','.join(
                SeqrMetadataKeys.parse_hpo_terms(q)
            ),
            # this is handled manually
            # SeqrMetadataKeys.HPO_TERMS_PRESENT: SeqrMetadataKeys.parse_hpo_terms,
        }

    @staticmethod
    def get_hpo_keys():
        """Get list of columns where HPO present terms might be listed"""
        return [
            SeqrMetadataKeys.HPO_TERMS_PRESENT.value,
            *[f'HPO Term {i}' for i in range(1, 21)],
        ]

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

    @staticmethod
    def get_age_of_onset_allowed_keys():
        """
        SEQR age of onset must be one of these values
        """
        return {
            'Congenital onset',
            'Embryonal onset',
            'Fetal onset',
            'Neonatal onset',
            'Infantile onset',
            'Childhood onset',
            'Juvenile onset',
            'Adult onset',
            'Young adult onset',
            'Middle age onset',
            'Late onset',
        }

    @staticmethod
    def parse_age_of_onset(age_of_onset: str):
        """
        Age of onset in seqr must be a value defined
        in `get_age_of_onset_allowed_keys`, validate that it's either
        true or we can can simply guess which one they meant.

        >>> SeqrMetadataKeys.parse_age_of_onset('congenital')
        'Congenital onset'

        >>> SeqrMetadataKeys.parse_age_of_onset(' iNFaNtIlE OnsET ')
        'Infantile onset'
        """
        if not age_of_onset:
            return None
        keys = SeqrMetadataKeys.get_age_of_onset_allowed_keys()
        if age_of_onset in keys:
            return age_of_onset
        lkeys_without_onset = {k.lower().replace('onset', '').strip(): k for k in keys}
        stripped_aoo = age_of_onset.lower().replace('onset', '').strip()
        if stripped_aoo in lkeys_without_onset:
            return lkeys_without_onset[stripped_aoo]

        raise ValueError(
            f"Didn't recognise age of set key {age_of_onset}, "
            f"expected one of: {', '.join(keys)}"
        )

    @staticmethod
    def parse_hpo_terms(hpo_terms: str) -> List[str]:
        """
        Validate that comma-separated HPO terms must start with 'HP:'

        >>> SeqrMetadataKeys.parse_hpo_terms('')
        []
        >>> SeqrMetadataKeys.parse_hpo_terms(',')
        []
        >>> SeqrMetadataKeys.parse_hpo_terms(' ,')
        []
        >>> SeqrMetadataKeys.parse_hpo_terms(' ')
        []
        >>> SeqrMetadataKeys.parse_hpo_terms('HP:0000504')
        ['HP:0000504']
        >>> SeqrMetadataKeys.parse_hpo_terms('HP:0003015 |Flared metaphysis|http://purl.obolibrary.org/obo/hp.fhir')
        ['HP:0003015']
        >>> SeqrMetadataKeys.parse_hpo_terms(' HP:12,  HP:34 ')
        ['HP:12', 'HP:34']
        >>> SeqrMetadataKeys.parse_hpo_terms('Clinical,Failure')
        Traceback (most recent call last):
        ValueError: HPO terms must follow the format "HP\\:\\d+$": Clinical, Failure
        """
        if not hpo_terms or not hpo_terms.strip():
            return []
        terms = split_generic_terms(hpo_terms)
        if not terms:
            return []

        def process_hpo_term(term):
            if '|' in term:
                return term.split('|', maxsplit=1)[0].strip()
            return term

        # mfranklin (2021-09-06): There were no IDs that didn't start with HP
        # https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/master/hp.obo
        terms = list(map(process_hpo_term, terms))
        terms = [t for t in terms if t]
        failing_terms = [term for term in terms if not HPO_REGEX_MATCHER.match(term)]
        if failing_terms:
            raise ValueError(
                f'HPO terms must follow the format "{HPO_REGEX_MATCHER.pattern}": '
                + ', '.join(failing_terms)
            )

        # do this, because sometimes collaborators use ', ' instead of ','
        return terms


class ParticipantLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection):
        super().__init__(connection)
        self.pttable = ParticipantTable(connection=connection)

    async def get_participants_by_ids(self, pids: list[int]) -> list[Participant]:
        project, participants = await self.pttable.get_participants_by_ids(pids)

        return participants

    async def get_participants(
        self,
        project: int,
        external_participant_ids: List[str] = None,
        internal_participant_ids: List[int] = None,
    ):
        """
        Get participants for a project
        """
        internal_ids = set(internal_participant_ids or [])
        if external_participant_ids:
            id_map = await self.get_id_map_by_external_ids(
                external_participant_ids, project, allow_missing=False
            )
            internal_ids.update(set(id_map.values()))

        ps = await self.pttable.get_participants(
            project=project, internal_participant_ids=list(internal_ids)
        )
        return [Participant(**p) for p in ps]

    async def fill_in_missing_participants(self):
        """Update the sequencing status from the internal sample id"""
        sample_table = SampleTable(connection=self.connection)

        # { external_id: internal_id }
        internal_sample_map_with_no_pid = dict(
            await sample_table.get_sample_with_missing_participants_by_internal_id(
                project=self.connection.project
            )
        )
        external_sample_map_with_no_pid = {
            v: k for k, v in internal_sample_map_with_no_pid.items()
        }
        ext_sample_id_to_pid = {}

        unlinked_participants = await self.get_id_map_by_external_ids(
            list(external_sample_map_with_no_pid.keys()),
            project=self.connection.project,
            allow_missing=True,
        )

        external_participant_ids_to_add = set(
            external_sample_map_with_no_pid.keys()
        ) - set(unlinked_participants.keys())

        if not external_participant_ids_to_add:
            # if there are no participants to add, skip the next step
            return '0 participants updated'

        async with self.connection.connection.transaction():
            sample_ids_to_update = {
                external_sample_map_with_no_pid[external_id]: pid
                for external_id, pid in unlinked_participants.items()
            }

            for external_id in external_participant_ids_to_add:
                sample_id = external_sample_map_with_no_pid[external_id]
                participant_id = await self.pttable.create_participant(
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
            ppttable = ParticipantPhenotypeTable(self.connection)

            self._validate_individual_metadata_headers(headers)

            lheaders_to_idx_map = {h.lower(): idx for idx, h in enumerate(headers)}

            participant_id_field_idx = lheaders_to_idx_map[
                SeqrMetadataKeys.INDIVIDUAL_ID.value.lower()
            ]
            family_id_field_indx = lheaders_to_idx_map.get(
                SeqrMetadataKeys.FAMILY_ID.value.lower()
            )
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
            assert self.connection.project
            external_pid_map = await self.get_id_map_by_external_ids(
                list(external_participant_ids),
                project=self.connection.project,
                allow_missing=allow_missing_participants,
            )
            if extra_participants_method == ExtraParticipantImporterHandler.ADD:
                missing_participant_eids = external_participant_ids - set(
                    external_pid_map.keys()
                )
                for ex_pid in missing_participant_eids:
                    external_pid_map[ex_pid] = await self.pttable.create_participant(
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
            fpttable = FamilyParticipantTable(self.connection)

            provided_pid_to_external_family = {
                external_pid_map[row[participant_id_field_idx]]: row[
                    family_id_field_indx
                ]
                for row in rows
                if family_id_field_indx and row[family_id_field_indx]
            }

            external_family_ids = set(provided_pid_to_external_family.values())
            # check that all the family ids actually line up
            _, pid_to_internal_family = await fpttable.get_participant_family_map(pids)
            fids = set(pid_to_internal_family.values())
            fmap_by_internal = await ftable.get_id_map_by_internal_ids(list(fids))
            fmap_from_external = await ftable.get_id_map_by_external_ids(
                list(external_family_ids),
                project=self.connection.project,
                allow_missing=True,
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
                await fpttable.create_rows(formed_rows)

            storeable_keys = [k.value for k in SeqrMetadataKeys.get_storeable_keys()]
            insertable_rows = self._prepare_individual_metadata_insertable_rows(
                storeable_keys=storeable_keys,
                lheaders_to_idx_map=lheaders_to_idx_map,
                participant_id_field_idx=participant_id_field_idx,
                pid_map=external_pid_map,
                rows=rows,
            )

            await ppttable.add_key_value_rows(insertable_rows)
            return True

    async def get_seqr_individual_template(
        self,
        project: int,
        external_participant_ids: Optional[List[str]] = None,
        # pylint: disable=invalid-name
        replace_with_participant_external_ids=True,
        replace_with_family_external_ids=True,
    ) -> dict[str, Any]:
        """Get seqr individual level metadata template as List[List[str]]"""

        # avoid circular imports
        # pylint: disable=import-outside-toplevel,cyclic-import,too-many-locals
        from db.python.layers.family import FamilyLayer

        ppttable = ParticipantPhenotypeTable(self.connection)
        internal_to_external_pid_map = {}
        internal_to_external_fid_map = {}

        if external_participant_ids:
            assert self.connection.project
            pids = await self.get_id_map_by_external_ids(
                external_participant_ids,
                project=self.connection.project,
                allow_missing=False,
            )
            pid_to_features = await ppttable.get_key_value_rows_for_participant_ids(
                participant_ids=list(pids.values())
            )
            if replace_with_participant_external_ids:
                internal_to_external_pid_map = {v: k for k, v in pids.items()}
        else:
            pid_to_features = await ppttable.get_key_value_rows_for_all_participants(
                project=project
            )
            if replace_with_participant_external_ids:
                internal_to_external_pid_map = (
                    await self.pttable.get_id_map_by_internal_ids(
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
        json_headers = [
            h.replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')
            for h in lheaders
        ]
        json_header_map = dict(zip(json_headers, headers))
        lheader_to_json = dict(zip(lheaders, json_headers))
        rows: List[Dict[str, str]] = []
        for pid, d in pid_to_features.items():
            d[SeqrMetadataKeys.INDIVIDUAL_ID.value] = internal_to_external_pid_map.get(
                pid, str(pid)
            )
            fid = pid_to_fid.get(pid, '<unknown>')
            d[SeqrMetadataKeys.FAMILY_ID.value] = internal_to_external_fid_map.get(
                fid, fid
            )
            ld = {k.lower(): v for k, v in d.items()}
            rows.append({lheader_to_json[h]: ld.get(h) for h in lheaders if ld.get(h)})

        set_headers = set()
        for row in rows:
            set_headers.update(set(row.keys()))

        rows = [{h: r.get(h) for h in set_headers if h in r} for r in rows]

        return {
            'rows': rows,
            'headers': list(set_headers),
            'header_map': json_header_map,
        }

    async def get_id_map_by_external_ids(
        self,
        external_ids: List[str],
        project: Optional[ProjectId],
        allow_missing=False,
    ) -> Dict[str, int]:
        """Get participant ID map by external_ids"""
        id_map = await self.pttable.get_id_map_by_external_ids(
            external_ids, project=project
        )
        if not allow_missing and len(id_map) != len(external_ids):
            provided_external_ids = set(external_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_external_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_participant_ids = provided_external_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find participants with IDS: {', '.join(missing_participant_ids)}"
                )

        return id_map

    async def update_many_participant_external_ids(
        self, internal_to_external_id: Dict[int, str], check_project_ids=True
    ):
        """Update many participant external ids"""
        if check_project_ids:

            projects = await self.pttable.get_project_ids_for_participant_ids(
                list(internal_to_external_id.keys())
            )
            await self.ptable.check_access_to_project_ids(
                user=self.author, project_ids=projects, readonly=False
            )

        return await self.pttable.update_many_participant_external_ids(
            internal_to_external_id
        )

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
        recognised_keys |= set(k.lower() for k in SeqrMetadataKeys.get_hpo_keys())
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
        insertable_rows: List[Tuple[int, str, Any]] = []
        parsers = {k.value: v for k, v in SeqrMetadataKeys.get_key_parsers().items()}

        hpo_col_indices = [
            lheaders_to_idx_map.get(h.lower())
            for h in SeqrMetadataKeys.get_hpo_keys()
            if h.lower() in lheaders_to_idx_map
        ]

        for row in rows:
            external_participant_id = row[participant_id_field_idx]
            participant_id = pid_map[external_participant_id]

            for header_key, col_number in storeable_header_col_number_tuples:
                if header_key == SeqrMetadataKeys.HPO_TERMS_PRESENT.value:
                    continue
                if col_number >= len(row):
                    continue
                value = row[col_number]
                if header_key in parsers:
                    # use custom parse declared in SeqrMetadataKeys.get_key_parsers
                    value = parsers[header_key](value)

                if value:
                    insertable_rows.append((participant_id, header_key, value))

            hpo_terms = []
            for idx in hpo_col_indices:
                hpo_terms.extend(SeqrMetadataKeys.parse_hpo_terms(row[idx]))

            if hpo_terms:
                insertable_rows.append(
                    (
                        participant_id,
                        SeqrMetadataKeys.HPO_TERMS_PRESENT.value,
                        ','.join(hpo_terms),
                    )
                )

        return insertable_rows

    async def get_external_participant_id_to_internal_sample_id_map(
        self, project: int
    ) -> List[Tuple[str, int]]:
        """
        Get a map of {external_participant_id} -> {internal_sample_id}
        useful to matching joint-called samples in the matrix table to the participant

        Return a list not dictionary, because dict could lose
        participants with multiple samples.
        """
        return await self.pttable.get_external_participant_id_to_internal_sample_id_map(
            project=project
        )

    async def create_participant(
        self,
        external_id: str,
        reported_sex: int = None,
        reported_gender: str = None,
        karyotype: str = None,
        meta: Dict = None,
        author: str = None,
        project: ProjectId = None,
    ) -> int:
        """Create a single participant"""
        # pylint: disable=unused-argument
        d = {k: v for k, v in locals().items() if k != 'self'}
        return await self.pttable.create_participant(**d)

    async def update_participants(
        self,
        participant_ids: List[int],
        reported_sexes: List[int] = None,
        reported_genders: List[str] = None,
        karyotypes: List[str] = None,
        metas: List[Dict] = None,
        author=None,
    ):
        """Update many participants"""
        # pylint: disable=unused-argument
        d = {k: v for k, v in locals().items() if k != 'self'}
        return await self.pttable.update_participants(**d)

    async def update_single_participant(
        self,
        participant_id: int,
        external_id: str = None,
        reported_sex: int = None,
        reported_gender: str = None,
        karyotype: str = None,
        meta: Dict = None,
        author=None,
        check_project_ids=True,
    ):
        """Update single participants"""
        # pylint: disable=unused-argument
        d = {
            k: v for k, v in locals().items() if k not in ('self', 'check_project_ids')
        }

        if check_project_ids:

            projects = await self.pttable.get_project_ids_for_participant_ids(
                [participant_id]
            )
            await self.ptable.check_access_to_project_ids(
                user=self.author, project_ids=projects, readonly=False
            )

        _ = await self.pttable.update_participant(**d)
        return participant_id

    async def upsert_participant(self, participant: ParticipantUpsert):
        """Upsert a participant"""
        if not participant.id:
            internal_id = await self.create_participant(
                external_id=participant.external_id,
                reported_sex=participant.reported_sex,
                reported_gender=participant.reported_gender,
                karyotype=participant.karyotype,
                meta=participant.meta,
                author=self.connection.author,
                project=self.connection.project,
            )
            return int(internal_id)

        # Otherwise update
        internal_id = await self.update_single_participant(
            participant_id=participant.id,
            external_id=participant.external_id,
            reported_sex=participant.reported_sex,
            reported_gender=participant.reported_gender,
            karyotype=participant.karyotype,
            meta=participant.meta,
            author=self.connection.author,
            check_project_ids=False,
        )
        return int(internal_id)

    async def batch_upsert_participants(self, participants: ParticipantUpsertBody):
        """Batch upsert a list of participants with sequences"""
        all_participants = participants.participants

        sampt: SampleLayer = SampleLayer(self.connection)

        # Create or update participants
        pids = [await self.upsert_participant(p) for p in all_participants]

        # Pull a list of external pids
        epids = [str(participant.external_id) for participant in all_participants]

        # Get map of external pids to internal pids
        epid_ipid_map = await self.get_id_map_by_external_ids(
            epids,
            project=self.connection.project,
            allow_missing=False,
        )

        for participant in all_participants:
            epid = participant.external_id
            ipid = epid_ipid_map[epid]

            for sample in participant.samples:
                sample.participant_id = ipid

        # Upsert all samples with sequences for each participant
        samples = [SampleBatchUpsertBody(samples=p.samples) for p in all_participants]
        results = [await sampt.batch_upsert_samples(s) for s in samples]

        # Format and return response
        return dict(zip(pids, results))
