from enum import Enum


class MetaSearchEntityPrefix(Enum):
    """Links prefixes to tables"""

    PARTICIPANT = 'p'
    SAMPLE = 's'
    ASSAY = 'a'
    FAMILY = 'f'
    SEQUENCING_GROUP = 'sg'


class SeqrDatasetType(Enum):
    """Type of dataset (es-index) that can be POSTed to Seqr"""

    SNV_INDEL = 'SNV_INDEL'  # Haplotypecaller in seqr UI
    SV = 'SV'                # SV Caller in seqr UI (WGS projects)
    GCNV = 'SV_WES'          # SV Caller in seqr UI (WES projects)
    MITO = 'MITO'            # Mitochondria Caller in seqr UI
