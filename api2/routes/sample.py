from fastapi import APIRouter, Header, HTTPException, Depends

from db.python.tables.sample import SampleTable, Sample

from api2.utils.db_dependency import get_connection, Connection


router = APIRouter(prefix="/sample", tags=["user"])
# sample_api = JsonBlueprint('sample_api', __name__)
# project_prefix = prefix + '<project>/sample'


@router.get('/{id_}', response_model=Sample)
async def get_sample_by_external_id(
    id_: str, connection: Connection = Depends(get_connection)
):
    st = SampleTable(connection)
    result = await st.get_single_by_external_id(id_)
    return result
