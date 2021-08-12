from enum import Enum


class SampleType(str, Enum):
    """Enum describing types of physical samples"""

    BLOOD = 'blood'
    SALIVA = 'saliva'
