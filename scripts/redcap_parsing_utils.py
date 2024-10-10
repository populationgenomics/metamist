# pylint: disable=unused-variable
import sys
from enum import Enum
from collections import defaultdict
from cloudpathlib import CloudPath

PEDFILE_HEADERS = [
    'Family ID',
    'Individual ID',
    'Paternal ID',
    'Maternal ID',
    'Sex',
    'Affected Status',
    'Notes',
]
INDIVIDUAL_METADATA_HEADERS = [
    'Family ID',
    'Individual ID',
    'HPO Terms (present)',
    'HPO Terms (absent)',
    'Age of Onset',
    'Individual Notes',
    'Consanguinity',
]
FAMILY_METADATA_HEADERS = [
    'Family ID',
    'Display Name',
    'Description',
    'Coded Phenotype',
]
FILEMAP_HEADERS = ['Individual ID', 'Sample ID', 'Filenames', 'Type']


class Facility(str, Enum):
    """Sequencing facilities"""

    GARVAN = 'Garvan'
    VCGS = 'VCGS'
    NSWPath = 'NSWPath'
    QPath = 'QPath'


class SeqType(Enum):
    """Sequence types"""

    GENOME = 'genome'
    EXOME = 'exome'
    TRNA = 'totalRNA'
    MRNA = 'mRNA'


# VCGS library types that appear in fastq names mapped to metamist
# sequence types.
VCGS_SEQTYPE_BY_LIBRARY_TYPE = {
    'ILMNDNAPCRFREE': SeqType.GENOME,
    'TruSeqDNAPCR-Free': SeqType.GENOME,
    'NexteraDNAFLEX': SeqType.GENOME,
    'NEXTERAFLEXWGS': SeqType.GENOME,
    'SSQXT-CRE': SeqType.EXOME,
    'SSQXTCRE': SeqType.EXOME,
    'SSXTLICREV2': SeqType.EXOME,
    'SSQXTCREV2': SeqType.EXOME,
    'TwistWES1VCGS1': SeqType.EXOME,
    'TwistWES2VCGS2': SeqType.EXOME,
    'TwistWES2VCGS2H': SeqType.EXOME,
    'TSStrmRNA': SeqType.MRNA,
    'TSStrtRNA': SeqType.TRNA,
    'TruSeq-Stranded-mRNA': SeqType.MRNA,
    'ILLStrtRNA': SeqType.TRNA,
}


class FacilityFastq:  # pylint: disable=too-many-instance-attributes
    """
    Object for parsing filenames of fastqs prepared by known sequence providers
    """

    def __init__(self, path, facility: Facility) -> None:
        self.path = path
        self.facility = facility

        # Parse fastq file name
        if facility == Facility.VCGS:
            self.parse_vcgs_fastq_name()
        elif facility == Facility.GARVAN:
            self.parse_garvan_fastq_name()
        elif facility == Facility.NSWPath:
            self.parse_nswpath_fastq_name()
        elif facility == Facility.QPath:
            self.parse_qpath_fastq_name()
        else:
            raise AssertionError(f'Facility {facility} not supported')

    def parse_vcgs_fastq_name(self):
        """
        Parse a standard VCGS fastq file name of format:
            220723_A00692_0301_ML222037_22W001579_MAN-20220723_NEXTERAFLEXWGS_L002_R2.fastq.gz

        Sets the following properties:
            sample_id: the sample ID used by the sequencing facility (WARNING: may be different to sample ID given to us)
            read_pair_prefix: basename of fastq excluding the R1/R2 suffix.
            library_type: Kit used to construct library using VCGS nomenclature
            library_id: Unique ID for the specific library that was sequenced


        Notes:
            - VCGS has used this file name format since ~2018, previous format will fail sanity check and is currently not supported.
            - Fastqs can be grouped by read_pair_prefix to find all read pairs
            - Fastqs can be grouped by library_id to find all part-fastqs from the same sequence (eg split across lanes)
        """
        # Sanity checks
        assert self.path.match('*.fastq.gz')

        base = self.path.name.rsplit('.', 2)[0]
        if len(self.path.name.split('_')) == 8:
            # pre ~2018 format
            (
                a,
                b,
                library_id,
                sample_id,
                library_prep_id,
                library_type,
                lane,
                read,
            ) = base.split('_')
        elif len(self.path.name.split('_')) == 9:
            # modern VCGS format
            (
                date,
                a,
                b,
                library_id,
                sample_id,
                library_prep_id,
                library_type,
                lane,
                read,
            ) = base.split('_')
        elif len(self.path.name.split('_')) == 10:
            # modern VCGS format with extra fields
            (
                extra,
                date,
                a,
                b,
                library_id,
                sample_id,
                library_prep_id,
                library_type,
                lane,
                read,
            ) = base.split('_')
        else:
            raise AssertionError(
                f'Unexpected number of fields in filename {self.path.name}'
            )

        self.sample_id = sample_id.rsplit('-', 1)[0]
        self.read_pair_prefix = base.rsplit('_', 1)[0]
        self.library_type = library_type
        self.library_id = library_id

        self.seq_type = VCGS_SEQTYPE_BY_LIBRARY_TYPE[self.library_type]

    def parse_garvan_fastq_name(self):
        """
        Parse a standard Garvan sequencing facility fastq file name of format:
            HTHW7DSX3_4_220822_FS28686902_Homo-sapiens_GAGAATGGTT-TTGCTGCCGA_R_220713_TINLY_DNA_M001_R2.fastq.gz

        Sets the following properties:
            sample_id: FluidX tube barcode
            read_pair_prefix: basename of fastq excluding the R1/R2 suffix.

        Notes:
            - I dont have an explicit understanding of what the other name parts of the filename represent. Would be handy to get.
            - Fastqs can be grouped by read_pair_prefix to find all read pairs
        """

        # Sanity checks
        assert self.path.match('*.fastq.gz')
        assert len(self.path.name.split('_')) == 12

        self.sample_id = self.path.name.split('_')[3]
        self.read_pair_prefix = self.path.name.rsplit('_', 1)[0]
        self.sample_type = self.path.name.split('_')[9]

        self.seq_type = SeqType.EXOME

        if (self.sample_type == 'DNA') or (self.sample_type == 'CARDIOGEN') or (self.sample_type == 'OHMR5DNA'):
            # logic: Garvan do not provide exome seq service
            self.seq_type = SeqType.GENOME
        else:
            raise AssertionError(
                f'Sample type {self.sample_type} not supported for GARVAN.'
            )

    def parse_nswpath_fastq_name(self):
        """
        Parse a standard NSWpath fastq file name of format:
            21R2140258-20210326-A00712_S70_L001_R1_001.fastq.gz
        Where:
            - 21R2140258 is a unique individual/biological sample ID
            - 21R2140258-20210326-A00712 is a unique sequence ID

        Sets the following properties:
            sample_id: set to individualID (eg 21R2140258)
            read_pair_prefix: set to sequence ID (eg 21R2140258-20210326-A00712).

        Notes:
            - NSWpath currently only accredited for exomes, so we will assume all data is exome for now
            - Fastqs can be grouped by read_pair_prefix to find all read pairs
        """

        # Sanity checks
        assert self.path.match('*.fastq.gz')
        assert len(self.path.name.split('_')) == 5

        self.read_pair_prefix = self.path.name.rsplit('_', 1)[0]
        self.sample_id = self.path.name.split('-')[0]

        self.sample_type = 'DNA'
        self.seq_type = SeqType.EXOME

    def parse_qpath_fastq_name(self):
        """
        Parse a standard QPath fastq file name of format:
            01-G-JS-2134576908_S1_L001_R1_001.fastq.gz
        Where:
            - 21345-76908 is a unique individual/biological sample ID
            - 01-G-JS-2134576908 is a unique sequence ID

        Sets the following properties:
            sample_id: set to individualID (eg 21345-76908)
            read_pair_prefix: set to sequence ID (eg 01-G-JS-2134576908).

        Notes:
            - NSWpath currently only sharing genomes, so we will assume all data is WGS for now
            - Fastqs can be grouped by read_pair_prefix to find all read pairs
        """

        # Sanity checks
        assert self.path.match('*.fastq.gz')
        assert len(self.path.name.split('_')) == 5

        self.read_pair_prefix = self.path.name.split('_', 1)[0]
        sample_id_full = self.read_pair_prefix.rsplit('-', 1)[1]
        self.sample_id = sample_id_full[:5] + '-' + sample_id_full[5:]

        self.sample_type = 'DNA'
        self.seq_type = SeqType.GENOME


def find_fastq_pairs(
    search_path: str,
    facility: Facility,
    recursive: bool = False,
):
    """
    Find all fastq file pairs in a given bucket path.

    Returns a dict with a list of fastq object pairs per sample ID.
    """

    # Find all fastq pairs
    search_dir = CloudPath(search_path)
    assert search_dir.is_dir()
    read_pairs = defaultdict(list)

    if recursive:
        raise AssertionError('rglob not supported by CloudPathLib?')
        # glob/rglob not supported by CloudPathLib?
        # AttributeError: type object '_CloudPathSelectable' has no attribute '_scandir'. Did you mean: 'scandir'?
        # falling back to non-recursive implementation using iterdir

    #     fastq_iter = search_dir.rglob('*.fastq.gz')
    # else:
    #     fastq_iter = search_dir.glob('*.fastq.gz')

    # for f in fastq_iter:
    for f in search_dir.iterdir():
        # skip non-fastq
        if not f.is_file() or not f.match('*.fastq.gz'):
            continue
        fq = FacilityFastq(f, facility)
        read_pairs[fq.read_pair_prefix].append(fq)

    # Group by sample
    read_pairs_by_sample_id = defaultdict(list)
    for pair in read_pairs.values():
        if len(pair) != 2:
            print(
                f"Warning: skipping incomplete pair: {[fq.path for fq in pair]}",
                file=sys.stderr,
            )
            continue
        read_pairs_by_sample_id[pair[0].sample_id].append(pair)

    return read_pairs_by_sample_id
