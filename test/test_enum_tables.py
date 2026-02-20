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
        """Test re-inserting an existing enum do nothing"""

        assay_type_table = AssayTypeTable(connection)
        assay_type = 'gvcf'

        await assay_type_table.insert(assay_type)
        get_vals: list[str] = await assay_type_table.get()
        assert assay_type in get_vals

        await assay_type_table.insert(assay_type)
        get_vals: list[str] = await assay_type_table.get()
        assert assay_type in get_vals
