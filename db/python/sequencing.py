from typing import Dict

from psycopg2.extras import Json

# from models.models.sequence import SampleSequencing
from models.enums import SequencingType

from .connect import DbBase


class SampleSequencingTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    def insert_sequencing(
        self,
        sample_id,
        sequence_type: SequencingType,
        sequence_meta: Dict[str, any] = None,
        commit=True,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """

        _query = """\
INSERT INTO sample_sequencing
    (sample_id, type, meta)
VALUES (%s, %s, %s) RETURNING id;"""

        with self.get_cursor() as cursor:

            cursor.execute(
                _query, (sample_id, sequence_type.value, Json(sequence_meta))
            )
            id_of_new_sample = cursor.fetchone()[0]

            if commit:
                self.commit()

        return id_of_new_sample
