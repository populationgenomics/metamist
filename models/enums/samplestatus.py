from enum import Enum

class SampleStatus(Enum):
    collected = 1
    sequenced = 2
    passed_qc = 3
    failed_qc = 4
    uploaded = 5
