from fastapi import APIRouter

from api.utils.db import get_db_connection, Connection
from db.python.layers.participant import ParticipantLayer

router = APIRouter(prefix='/participant', tags=['participant'])


@router.post('/fill-in-missing-participants', operation_id='fillInMissingParticipants')
async def fill_in_missing_participants(connection: Connection = get_db_connection):
    """Get sample by external ID"""
    participant_layer = ParticipantLayer(connection)

    return {'success': await participant_layer.fill_in_missing_participants()}
