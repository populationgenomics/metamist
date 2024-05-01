# pylint: disable=used-before-assignment

from api.utils import group_by
from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.layers.participant import ParticipantLayer
from db.python.tables.family import FamilyFilter, FamilyTable
from db.python.tables.family_participant import (
    FamilyParticipantFilter,
    FamilyParticipantTable,
)
from db.python.tables.participant import ParticipantTable
from db.python.tables.sample import SampleTable
from db.python.utils import GenericFilter, NotFoundError
from models.models.family import FamilyInternal, PedRow, PedRowInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.project import ProjectId


class FamilyLayer(BaseLayer):
    """Layer for import logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.stable = SampleTable(connection)
        self.ftable = FamilyTable(connection)
        self.fptable = FamilyParticipantTable(self.connection)

    async def create_family(
        self, external_id: str, description: str = None, coded_phenotype: str = None
    ):
        """Create a family"""
        return await self.ftable.create_family(
            external_id=external_id,
            description=description,
            coded_phenotype=coded_phenotype,
        )

    async def get_family_by_internal_id(
        self, family_id: int, check_project_id: bool = True
    ) -> FamilyInternal:
        """Get family by internal ID"""
        projects, families = await self.ftable.query(
            FamilyFilter(id=GenericFilter(eq=family_id))
        )
        if not families:
            raise NotFoundError(f'Family with ID {family_id} not found')
        family = families[0]
        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return family

    async def get_family_by_external_id(
        self, external_id: str, project: ProjectId | None = None
    ):
        """Get family by external ID, requires project scope"""
        families = await self.ftable.query(
            FamilyFilter(
                external_id=GenericFilter(eq=external_id),
                project=GenericFilter(eq=project or self.connection.project),
            )
        )
        if not families:
            raise NotFoundError(f'Family with external ID {external_id} not found')

        return families[0]

    async def query(
        self,
        filter_: FamilyFilter,
        check_project_ids: bool = True,
    ) -> list[FamilyInternal]:
        """Get all families for a project"""

        # don't need a project check, as we're being provided an explicit filter

        projects, families = await self.ftable.query(filter_)

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.connection.author, projects, readonly=True
            )

        return families

    async def get_families_by_ids(
        self,
        family_ids: list[int],
        check_missing: bool = True,
        check_project_ids: bool = True,
    ) -> list[FamilyInternal]:
        """Get families by internal IDs"""
        projects, families = await self.ftable.query(
            FamilyFilter(id=GenericFilter(in_=family_ids))
        )
        if not families:
            return []

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.connection.author, projects, readonly=True
            )

        if check_missing and len(family_ids) != len(families):
            missing_ids = set(family_ids) - set(f.id for f in families)
            raise ValueError(f'Missing family IDs: {missing_ids}')

        return families

    async def get_families_by_participants(
        self, participant_ids: list[int], check_project_ids: bool = True
    ) -> dict[int, list[FamilyInternal]]:
        """
        Get families keyed by participant_ids, this will duplicate families
        """
        projects, participant_map = await self.ftable.get_families_by_participants(
            participant_ids=participant_ids
        )
        if not participant_map:
            return {}

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.connection.author, projects, readonly=True
            )

        return participant_map

    async def update_family(
        self,
        id_: int,
        external_id: str = None,
        description: str = None,
        coded_phenotype: str = None,
        check_project_ids: bool = True,
    ) -> bool:
        """Update fields on some family"""
        if check_project_ids:
            project_ids = await self.ftable.get_projects_by_family_ids([id_])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.ftable.update_family(
            id_=id_,
            external_id=external_id,
            description=description,
            coded_phenotype=coded_phenotype,
        )

    async def get_pedigree(
        self,
        project: ProjectId,
        family_ids: list[int] | None = None,
        # pylint: disable=invalid-name
        replace_with_participant_external_ids: bool = False,
        # pylint: disable=invalid-name
        replace_with_family_external_ids: bool = False,
        empty_participant_value: str | None = None,
        include_participants_not_in_families: bool = False,
    ) -> list[dict[str, str | int | None]]:
        """
        Generate pedigree file for ALL families in project
        (unless internal_family_ids is specified).

        Use internal IDs unless specific options are specified.
        """

        _, rows = await self.fptable.query(
            FamilyParticipantFilter(
                project=GenericFilter(eq=project),
                family_id=GenericFilter(in_=family_ids) if family_ids else None,
            ),
            include_participants_not_in_families=include_participants_not_in_families,
        )
        # participant_id to external_id
        pmap: dict[int, str] = {}
        # family_id to external_id
        fmap: dict[int, str] = {}
        if replace_with_participant_external_ids:
            participant_ids = set(
                s
                for r in rows
                for s in (r.individual_id, r.maternal_id, r.paternal_id)
                if s is not None
            )
            ptable = ParticipantTable(connection=self.connection)
            pmap = await ptable.get_id_map_by_internal_ids(list(participant_ids))

        if replace_with_family_external_ids:
            family_ids = list(set(r.family_id for r in rows if r.family_id is not None))
            fmap = await self.ftable.get_id_map_by_internal_ids(list(family_ids))

        mapped_rows: list[dict[str, str | int | None]] = []
        for r in rows:
            mapped_rows.append(
                {
                    'family_id': fmap.get(r.family_id, str(r.family_id)),
                    'individual_id': pmap.get(r.individual_id, r.individual_id)
                    or empty_participant_value,
                    'paternal_id': pmap.get(r.paternal_id, r.paternal_id)
                    or empty_participant_value,
                    'maternal_id': pmap.get(r.maternal_id, r.maternal_id)
                    or empty_participant_value,
                    'sex': r.sex,
                    'affected': r.affected,
                    'notes': r.notes,
                }
            )

        return mapped_rows

    async def get_participant_family_map(
        self, participant_ids: list[int], check_project_ids=False
    ):
        """Get participant family map"""

        fptable = FamilyParticipantTable(self.connection)
        projects, family_map = await fptable.get_participant_family_map(
            participant_ids=participant_ids
        )

        if check_project_ids:
            raise NotImplementedError(f'Must check specified projects: {projects}')

        return family_map

    async def import_pedigree(
        self,
        header: list[str] | None,
        rows: list[list[str]],
        create_missing_participants=False,
        perform_sex_check=True,
    ):
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

        pedrows: list[PedRow] = [
            PedRow(**{_header[i]: r[i] for i in range(len(_header))}) for r in rows
        ]
        # this validates a lot of the pedigree too
        pedrows = PedRow.order(pedrows)
        if perform_sex_check:
            PedRow.validate_sexes(pedrows, throws=True)

        external_family_ids = set(r.family_id for r in pedrows)
        # get set of all individual, paternal, maternal participant ids
        external_participant_ids = set(
            pid
            for r in pedrows
            for pid in [r.individual_id, r.paternal_id, r.maternal_id]
            if pid
        )

        participant_table = ParticipantLayer(self.connection)

        external_family_id_map = await self.ftable.get_id_map_by_external_ids(
            list(external_family_ids),
            project=self.connection.project,
            allow_missing=True,
        )
        missing_external_family_ids = [
            f for f in external_family_ids if f not in external_family_id_map
        ]
        external_participant_ids_map = await participant_table.get_id_map_by_external_ids(
            list(external_participant_ids),
            project=self.connection.project,
            # Allow missing participants if we're creating them
            allow_missing=create_missing_participants,
        )

        async with self.connection.connection.transaction():
            if create_missing_participants:
                missing_participant_ids = set(external_participant_ids) - set(
                    external_participant_ids_map
                )
                for row in pedrows:
                    if row.individual_id not in missing_participant_ids:
                        continue
                    upserted_participant = await participant_table.upsert_participant(
                        ParticipantUpsertInternal(
                            external_id=row.individual_id,
                            reported_sex=row.sex,
                        )
                    )
                    pid = upserted_participant.id
                    external_participant_ids_map[row.individual_id] = pid

            for external_family_id in missing_external_family_ids:
                internal_family_id = await self.ftable.create_family(
                    external_id=external_family_id,
                    description=None,
                    coded_phenotype=None,
                )
                external_family_id_map[external_family_id] = internal_family_id

            # now let's map participants back

            insertable_rows = [
                PedRowInternal(
                    family_id=external_family_id_map[row.family_id],
                    individual_id=external_participant_ids_map[row.individual_id],
                    paternal_id=external_participant_ids_map.get(row.paternal_id),
                    maternal_id=external_participant_ids_map.get(row.maternal_id),
                    affected=row.affected,
                    notes=row.notes,
                    sex=row.sex,
                )
                for row in pedrows
            ]

            await participant_table.upsert_participants(
                [
                    ParticipantUpsertInternal(
                        id=external_participant_ids_map[row.individual_id],
                        reported_sex=row.sex,
                    )
                    for row in pedrows
                ]
            )
            await self.fptable.create_rows(insertable_rows)

        return True

    async def update_family_members(self, rows: list[PedRowInternal]):
        """Update family members"""
        await self.fptable.create_rows(rows)

    async def import_families(self, headers: list[str] | None, rows: list[list[str]]):
        """Import a family table"""
        ordered_headers = [
            'Family ID',
            'Display Name',
            'Description',
            'Coded Phenotype',
        ]
        _headers = headers or ordered_headers[: len(rows[0])]
        lheaders = [k.lower() for k in _headers]
        key_map = {
            'externalId': {'family_id', 'family id', 'familyid'},
            'displayName': {'display name', 'displayname', 'display_name'},
            'description': {'description'},
            'phenotype': {
                'coded phenotype',
                'phenotype',
                'codedphenotype',
                'coded_phenotype',
            },
        }

        def get_idx_for_header(header) -> int | None:
            return next(
                iter(idx for idx, key in enumerate(lheaders) if key in key_map[header]),
                None,
            )

        external_identifier_idx = get_idx_for_header('externalId')
        display_name_idx = get_idx_for_header('displayName')
        description_idx = get_idx_for_header('description')
        phenotype_idx = get_idx_for_header('phenotype')

        # replace empty strings with None
        def replace_empty_string_with_none(val):
            """Don't set as empty string, prefer to set as null"""
            return None if val == '' else val

        _fixed_rows = [[replace_empty_string_with_none(el) for el in r] for r in rows]

        empty: list[str | None] = [None] * len(_fixed_rows)

        def select_columns(
            col1: int | None, col2: int | None = None
        ) -> list[str | None]:
            """
            - If col1 and col2 is None, return [None] * len(rows)
            - if either col1 or col2 is not None, return that column
            - else, return a mixture of column col1 | col2 if set
            """
            if col1 is None and col2 is None:
                # if col1 AND col2 is NONE
                return empty
            if col1 is not None and col2 is None:
                # if only col1 is set
                return [r[col1] for r in _fixed_rows]
            if col2 is not None and col1 is None:
                # if only col2 is set
                return [r[col2] for r in _fixed_rows]
            # if col1 AND col2 are not None
            assert col1 is not None and col2 is not None
            return [r[col1] if r[col1] is not None else r[col2] for r in _fixed_rows]

        await self.ftable.insert_or_update_multiple_families(
            external_ids=select_columns(external_identifier_idx, display_name_idx),
            descriptions=select_columns(description_idx),
            coded_phenotypes=select_columns(phenotype_idx),
        )
        return True

    async def get_family_participants_by_family_ids(
        self, family_ids: list[int], check_project_ids: bool = True
    ) -> dict[int, list[PedRowInternal]]:
        """Get family participants for family IDs"""
        projects, fps = await self.fptable.query(
            FamilyParticipantFilter(family_id=GenericFilter(in_=family_ids))
        )

        if not fps:
            return {}

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.connection.author, projects, readonly=True
            )

        return group_by(fps, lambda r: r.family_id)

    async def get_family_participants_for_participants(
        self, participant_ids: list[int], check_project_ids: bool = True
    ) -> list[PedRowInternal]:
        """Get family participants for participant IDs"""
        projects, fps = await self.fptable.query(
            FamilyParticipantFilter(participant_id=GenericFilter(in_=participant_ids))
        )

        if not fps:
            return []

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.connection.author, projects, readonly=True
            )

        return fps
