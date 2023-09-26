import datetime
from models.base import SMBase
    

class EtlSummary(SMBase):
    """Return class for the ETL summary endpoint"""

    request_id: str
    last_run_at: datetime.datetime | None
    status: str
    source_type: str
    
    submitting_user: str
    parser_result: str
    
    class Config:
        """Config for EtlSummary Response"""
        orm_mode = True
    