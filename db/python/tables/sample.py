from typing import Dict

from psycopg2.extras import Json

from models.models.sample import Sample
from models.enums import SampleType, SampleUpdateType

from db.python.connect import DbBase, NotFoundError


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    def _log_update(
        self,
        sample_id: int,
        type_: SampleUpdateType,
        update: Dict[str, any],
        author: str = None,
        commit=True,
        cursor=None,
    ):
        _query = """\
INSERT INTO sample_update
    (sample_id, type, update, author)
VALUES (%s, %s, %s, %s)
        """

        def execute_with(cursor):
            cursor.execute(
                _query, (sample_id, type_.value, Json(update), author or self.author)
            )

        if cursor:
            execute_with(cursor)
        else:
            with self.get_cursor() as crs:
                execute_with(crs)

        if commit:
            self.commit()

    def insert_sample(
        self,
        external_id,
        sample_type: SampleType,
        active,
        meta=None,
        participant_id=None,
        commit=True,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        _query = """\
INSERT INTO sample
    (external_id, participant_id, meta, type, active)
VALUES (%s, %s, %s, %s, %s) RETURNING id;"""

        with self.get_cursor() as cursor:

            cursor.execute(
                _query,
                (
                    external_id,
                    participant_id,
                    Json(meta),
                    sample_type.value,
                    active,
                ),
            )
            id_of_new_sample = cursor.fetchone()[0]
            self._log_update(
                sample_id=id_of_new_sample,
                type_=SampleUpdateType.created,
                update={
                    'external_id': external_id,
                    'participant_id': participant_id,
                    'meta': meta,
                    'type': sample_type.value,
                    'active': active,
                },
            )

            if commit:
                self.commit()

        return id_of_new_sample

    def get_single_by_external_id(self, external_id, check_active=True) -> Sample:
        """Get a Sample by its external_id"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
        ]
        if check_active:
            _query = f"""\
SELECT {", ".join(keys)} from sample
    where external_id = %s AND active
    LIMIT 1;"""
        else:
            _query = (
                f'SELECT {", ".join(keys)} from sample where external_id = %s LIMIT 1;'
            )
        with self.get_cursor() as cursor:
            cursor.execute(_query, (external_id,))
            sample_row = cursor.fetchone()

        if sample_row is None:
            raise NotFoundError(
                f'Couldn\'t find active sample with external id {external_id}'
            )

        kwargs = {keys[i]: sample_row[i] for i in range(len(keys))}
        sample = Sample.from_db(**kwargs)
        return sample
