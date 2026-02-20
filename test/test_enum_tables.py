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
        """Test insert new enum and retrieving existing enums"""

        assay_type_table = AssayTypeTable(connection)
        table_name = assay_type_table.get_table_name()

        assay_type_id = 'gvcf'
        assay_type_name = 'gvcf_1'
        audit_type_id = await connection.audit_log_id()

        # inject an enum entry id != name
        await connection.pg_connection.execute(
            t'INSERT INTO {table_name:i} (id, name, audit_log_id) VALUES ({assay_type_id}, {assay_type_name}, {audit_type_id})'
        )

        # reset entry name
        await assay_type_table.insert(assay_type_id)
        get_vals: list[str] = await assay_type_table.get()
        assert assay_type_name not in get_vals
