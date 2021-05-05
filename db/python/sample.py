from psycopg2.extras import Json

from models.models.sample import Sample
from models.enums import SampleType

from .connect import DbBase, NotFoundError


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    def new(
        self,
        external_id,
        sample_type: SampleType,
        active,
        sample_meta=None,
        participant_id=None,
        commit=True,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        _query = """\
INSERT INTO sample
    (external_id, participant_id, sample_meta, sample_type, active)
VALUES (%s, %s, %s, %s, %s) RETURNING id;"""

        with self.get_cursor() as cursor:

            cursor.execute(
                _query,
                (
                    external_id,
                    participant_id,
                    Json(sample_meta),
                    sample_type.value,
                    active,
                ),
            )
            id_of_new_sample = cursor.fetchone()[0]

            if commit:
                self.commit()

        return id_of_new_sample

    def get_single_by_external_id(self, external_id, check_active=True) -> Sample:
        """Get a Sample by its external_id"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'sample_meta',
            'active',
            'sample_type',
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
