import pytest

from db.python.connect import Connection
from db.python.enum_tables import AssayTypeTable


class TestEnumTable:
    """Base class for testing enum tables"""

    @pytest.mark.asyncio
    async def test_insert_new_enum(self, connection: Connection):
        """Test insert new enum and retrieving existing enums"""

        assay_type_table = AssayTypeTable(connection)
        assay_type = 'gvcf'
        await assay_type_table.insert(assay_type)

        get_vals: list[str] = await assay_type_table.get()
        assert assay_type in get_vals

    @pytest.mark.asyncio
    async def test_update_existing_enum(self, connection: Connection):
        assay_type_table = AssayTypeTable(connection)
        table_name = assay_type_table.get_table_name()

        assay_type = 'gvcf'
        await assay_type_table.insert(assay_type)

        new_assay_type = 'gvcf_1'
        await connection.pg_connection.execute(
            t'UPDATE {table_name:i} set name = {new_assay_type} where id = {assay_type}'
        )

        await assay_type_table.insert(assay_type)
        get_vals: list[str] = await assay_type_table.get()
        assert new_assay_type not in get_vals
