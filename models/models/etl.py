import datetime
import json
from models.base import SMBase
from models.enums import EtlStatus    

    
class EtlRecord(SMBase):
    """Return class for the ETL record"""

    request_id: str
    last_run_at: datetime.datetime | None
    status: EtlStatus
    source_type: str
    
    submitting_user: str
    parser_result: str
    
    class Config:
        """Config for EtlRecord Response"""
        orm_mode = True
        
    @staticmethod
    def from_json(record):
        """Create EtlRecord from json"""
        
        record['details'] = json.loads(record['details'])
        record['sample_record'] = json.loads(record['sample_record'])
        
        return EtlRecord(
            request_id=record['request_id'],
            last_run_at=record['last_run_at'], 
            status=EtlStatus[str(record['status']).upper()], 
            # remove leading backslash fromt the source_type
            source_type=record['details']['source_type'].strip('/'),
            submitting_user=record['details']['submitting_user'],
            parser_result=record['details']['result'],
        )
