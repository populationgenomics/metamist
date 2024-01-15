from db.python.tables.base import DbBase


class FileTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'file'

    COMMON_GET_KEYS = [
        'f.path',
        'f.basename',
        'f.dirname',
        'f.nameroot',
        'f.nameext',
        'f.checksum',
        'f.size',
        'f.secondaryFiles',
    ]


    async def query(
        self, filter_: AssayFilter
    ) -> tuple[set[ProjectId], list[AssayInternal]]:
        """Query assays"""
        sql_overides = {
            'sample_id': 'a.sample_id',
            'id': 'a.id',
            'external_id': 'aeid.external_id',
            'meta': 'a.meta',
            'sample_meta': 's.meta',
            'project': 's.project',
            'type': 'a.type',
        }
        if filter_.external_id is not None and filter_.project is None:
            raise ValueError('Must provide a project if filtering by external_id')

        conditions, values = filter_.to_sql(sql_overides)
        keys = ', '.join(self.COMMON_GET_KEYS)
        _query = f"""
            SELECT {keys}
            FROM assay a
            LEFT JOIN sample s ON s.id = a.sample_id
            LEFT JOIN assay_external_id aeid ON aeid.assay_id = a.id
            WHERE {conditions}
        """

        assay_rows = await self.connection.fetch_all(_query, values)

        # this will unique on the id, which we want due to joining on 1:many eid table
        assay_ids = [a['id'] for a in assay_rows]
        seq_eids = await self._get_assays_eids(assay_ids)
        assays = []

        project_ids: set[ProjectId] = set()
        for row in assay_rows:
            drow = dict(row)
            project_ids.add(drow.pop('project'))
            assay = AssayInternal.from_db(drow)
            assay.external_ids = seq_eids.get(assay.id, {}) if assay.id else {}
            assays.append(assay)

        return project_ids, assays