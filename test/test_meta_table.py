import tempfile
from io import BytesIO
from typing import Any

import duckdb
import pytest

from db.python.connect import Connection
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from models.models import PRIMARY_EXTERNAL_ORG
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal


def query_parquet(tables: dict[str, BytesIO], query: str) -> list[dict[str, Any]]:
    """
    Handle getting data out of a parquet file, this writes the file to a temporary
    folder and then reads it back in to duckdb to get the data. This probably isn't
    the most efficient, but the files are small so it shouldn't be too slow.
    """
    duck = duckdb.connect()

    with tempfile.TemporaryDirectory() as td:
        for table_name, table_bytes in tables.items():
            filename = f'{td}/{table_name}.parquet'
            with open(filename, 'wb') as f:  # noqa: PTH123
                table_bytes.seek(0)
                file_val = table_bytes.getvalue()
                f.write(file_val)
                f.close()

            duck.register(table_name, duck.read_parquet(filename))

        return duck.query(query).fetch_arrow_table().to_pylist()


@pytest.mark.asyncio
@pytest.mark.skip(
    reason="Tests won't work until we finish migrating participants and samples"
)
class TestMetaTable:
    """Test meta table operations"""

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_export_participants(
        self, connection_with_project: Connection
    ) -> None:
        """Test getting a participant table from the export layer"""
        pl = ParticipantLayer(connection_with_project)

        await pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01', 'other': 'OTHER1'},
                    reported_sex=2,
                    karyotype='XX',
                    meta={'field': 1},
                ),
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX02'},
                    reported_sex=1,
                    karyotype='XY',
                    meta={'field': 2},
                ),
            ]
        )
        assert connection_with_project.project_id
        pts = await pl.export_participant_table(connection_with_project.project_id)
        assert pts is not None
        result = query_parquet(
            {'participants': pts},
            'SELECT * FROM participants order by participant_id',
        )

        assert len(result) == 2
        assert result[0]['meta_field'] == 1
        assert result[1]['meta_field'] == 2
        assert result[0]['external_id'] == 'EX01'
        assert result[1]['external_id'] == 'EX02'
        assert result[0]['external_id_other'] == 'OTHER1'

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_export_empty_table(
        self, connection_with_project: Connection
    ) -> None:
        """Test that exporting an empty table returns None"""
        pl = ParticipantLayer(connection_with_project)
        assert connection_with_project.project_id
        pts = await pl.export_participant_table(connection_with_project.project_id)
        assert pts is None

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_export_samples(self, connection_with_project: Connection) -> None:
        """Test getting a sample table from the export layer"""
        sl = SampleLayer(connection_with_project)

        await sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'field_1': 'field_1_value'},
            )
        )

        await sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                type='blood',
                active=True,
                meta={'field_2': 'field_2_value'},
            )
        )

        assert connection_with_project.project_id
        samples = await sl.export_sample_table(connection_with_project.project_id)
        assert samples is not None
        result = query_parquet(
            {'samples': samples},
            'SELECT * FROM samples order by sample_id',
        )

        assert len(result) == 2
        assert result[0]['meta_field_1'] == 'field_1_value'
        assert result[1]['meta_field_2'] == 'field_2_value'
        assert result[0]['external_id'] == 'Test01'
        assert result[1]['external_id'] == 'Test02'
