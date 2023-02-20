
from enum import Enum
from collections import defaultdict
from cloudpathlib import CloudPath

PEDFILE_HEADERS = ['Family ID', 'Individual ID', 'Paternal ID', 'Maternal ID', 'Sex', 'Affected Status', 'Notes']
INDIVIDUAL_METADATA_HEADERS = ['Family ID', 'Individual ID', 'HPO Terms (present)', 'HPO Terms (absent)', 'Age of Onset', 'Individual Notes', 'Consanguinity']
FAMILY_METADATA_HEADERS = ['Family ID', 'Display Name', 'Description', 'Coded Phenotype']
FILEMAP_HEADERS = ["Individual ID", "Sample ID", "Filenames", "Type"]


class Facility(Enum):
    GARVAN = "Garvan"
    VCGS = "VCGS"


class SeqType(Enum):
    """ Sequnce types """
    GENOME = "genome"
    EXOME = "exome"
    TRNA = "totalRNA"
    MRNA = "mRNA"


# VCGS library types that appear in fastq names mapped to metamist
# sequence types.
VCGS_SEQTYPE_BY_LIBRARY_TYPE = {
    'ILMNDNAPCRFREE' : SeqType.GENOME,
    'TruSeqDNAPCR-Free' : SeqType.GENOME,
    'NexteraDNAFLEX' : SeqType.GENOME,
    'NEXTERAFLEXWGS' : SeqType.GENOME,
    'SSQXT-CRE' : SeqType.EXOME,
    'SSQXTCRE' : SeqType.EXOME,
    'SSXTLICREV2' : SeqType.EXOME,
    'SSQXTCREV2' : SeqType.EXOME,
    'TwistWES1VCGS1' : SeqType.EXOME,
    'TSStrmRNA' : SeqType.MRNA,
    'TSStrtRNA' : SeqType.TRNA,
    'TruSeq-Stranded-mRNA' : SeqType.MRNA,
    'ILLStrtRNA' : SeqType.TRNA,
}


class FacilityFastq:
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
        else:
            assert False, f"Facility {facility} not supported"

        # Determine sequence type
        self.seq_type = self.get_seq_type()

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
        assert len(self.path.name.split('_')) == 9

        base = self.path.name.rsplit('.', 2)[0]
        date, a, b, library_id, sample_id, library_prep_id, library_type, lane, read = base.split('_')

        self.sample_id = sample_id.rsplit('-', 1)[0]
        self.read_pair_prefix = base.rsplit('_', 1)[0]
        self.library_type = library_type
        self.library_id = library_id

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

    def get_seq_type(self):
        "Divine the correct seq type based on what we know"

        if self.facility == Facility.GARVAN:
            if self.sample_type == 'DNA':
                # logic: Garvan do not provide exome seq service
                return SeqType.GENOME
            else:
                assert False, f"Sample type {self.sample_type} not supported."
        elif self.facility == Facility.VCGS:
            return VCGS_SEQTYPE_BY_LIBRARY_TYPE[self.library_type]
        else:
            assert False, f"Facility {self.facility} not supported"


def find_fastq_pairs(search_path: str, facility: Facility, recursive: bool = False, ):
    """
        Find all fastq file pairs in a given bucket path.

        Returns a dict with a list of fastq object pairs per sample ID.
    """

    # Find all fastq pairs
    search_dir = CloudPath(search_path)
    assert search_dir.is_dir()
    read_pairs = defaultdict(list)

    if recursive:
        assert False, "rglob not supported by CloudPathLib?"
        # glob/rglob not supported by CloudPathLib?
        # AttributeError: type object '_CloudPathSelectable' has no attribute '_scandir'. Did you mean: 'scandir'?
        # falling back to non-recursive implementation using iterdir

    #     fastq_iter = search_dir.rglob("*.fastq.gz")
    # else:
    #     fastq_iter = search_dir.glob("*.fastq.gz")

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
        read_pairs_by_sample_id[pair[0].sample_id].append(pair)

    return read_pairs_by_sample_id
