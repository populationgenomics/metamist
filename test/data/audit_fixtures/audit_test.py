"""
Contains fixtured data for the audit unit tests
"""

from metamist.audit.models import (
    SequencingGroup,
    Analysis,
    Assay,
    Sample,
    Participant,
    ExternalIds,
    FileMetadata,
)

from cpg_utils import to_path

ENUMS_QUERY_RESULT = {
    'enum': {
        'analysisType': ['cram', 'vcf'],
        'sampleType': ['blood', 'saliva'],
        'sequencingType': ['genome', 'exome'],
        'sequencingPlatform': ['illumina', 'pacbio', 'ont'],
        'sequencingTechnology': ['short-read', 'long-read'],
    }
}

PARTICIPANTS = {
    1: Participant(
        id=1,
        external_ids=ExternalIds(ids={'': 'P001'}),
    ),
    2: Participant(
        id=2,
        external_ids=ExternalIds(ids={'': 'P002'}),
    ),
    3: Participant(
        id=3,
        external_ids=ExternalIds(ids={'': 'P003'}),
    ),
    4: Participant(
        id=4,
        external_ids=ExternalIds(ids={'': 'P004'}),
    ),
    5: Participant(
        id=5,
        external_ids=ExternalIds(ids={'': 'P005'}),
    ),
}

SAMPLES = {
    'S01': Sample(
        id='S01',
        external_ids=ExternalIds(ids={'': 'EXT001'}),
        participant=Participant(id=1, external_ids=ExternalIds(ids={'': 'P001'})),
    ),
    'S02': Sample(
        id='S02',
        external_ids=ExternalIds(ids={'': 'EXT002'}),
        participant=Participant(id=2, external_ids=ExternalIds(ids={'': 'P002'})),
    ),
    'S03': Sample(
        id='S03',
        external_ids=ExternalIds(ids={'': 'EXT003'}),
        participant=Participant(id=3, external_ids=ExternalIds(ids={'': 'P003'})),
    ),
    'S04': Sample(
        id='S04',
        external_ids=ExternalIds(ids={'': 'EXT004'}),
        participant=Participant(id=4, external_ids=ExternalIds(ids={'': 'P004'})),
    ),
    'S05': Sample(
        id='S05',
        external_ids=ExternalIds(ids={'': 'EXT005'}),
        participant=Participant(id=4, external_ids=ExternalIds(ids={'': 'P004'})),
    ),
    'S06': Sample(
        id='S06',
        external_ids=ExternalIds(ids={'': 'EXT006'}),
        participant=Participant(id=5, external_ids=ExternalIds(ids={'': 'P005'})),
    ),
    'S07': Sample(
        id='S07',
        external_ids=ExternalIds(ids={'': 'EXT007'}),
        participant=Participant(id=5, external_ids=ExternalIds(ids={'': 'P005'})),
    ),
}

ASSAYS = {
    # One read file (BAM) (in bucket)
    1: Assay(
        id=1,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-01-01/bams/EXT001.bam'
                ),
                filesize=1234,
                checksum='abc123',
            )
        ],
    ),
    # One read file (BAM) (exome)
    2: Assay(
        id=2,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-01-01/bams/exome/EXT001.bam'
                ),
                filesize=321,
                checksum='cba321',
            )
        ],
    ),
    # One read file (CRAM) (in bucket)
    3: Assay(
        id=3,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-01-01/crams/EXT002.cram'
                ),
                filesize=1953,
                checksum='asd456',
            )
        ],
    ),
    # Two read files (fastq) (exome) (in bucket)
    4: Assay(
        id=4,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-02-01/exome/fastqs/EXT003_R1.fq'
                ),
                filesize=3456,
                checksum='cde345',
            ),
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-02-01/exome/fastqs/EXT003_R2.fq'
                ),
                filesize=4567,
                checksum='def456',
            ),
        ],
    ),
    # Two read files (fastq)
    5: Assay(
        id=5,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-02-01/fastqs/EXT004_L001_R1.fq'
                ),
                filesize=5678,
                checksum='efg567',
            ),
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-02-01/fastqs/EXT004_L001_R2.fq'
                ),
                filesize=6789,
                checksum='fgh678',
            ),
        ],
    ),
    # Two read files (fastq)
    6: Assay(
        id=6,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-03-01/fastqs/EXT004_L002_R1.fq'
                ),
                filesize=7890,
                checksum='ghi789',
            ),
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-03-01/fastqs/EXT004_L002_R2.fq'
                ),
                filesize=8901,
                checksum='hij890',
            ),
        ],
    ),
    # Two read files (fastq)
    7: Assay(
        id=7,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-03-01/fastqs/EXT004_L003_R1.fq'
                ),
                filesize=9012,
                checksum='ijk123',
            ),
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-03-01/fastqs/EXT004_L003_R2.fq'
                ),
                filesize=123,
                checksum='jkl234',
            ),
        ],
    ),
    # Two read files (fastq)
    8: Assay(
        id=8,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-04-01/fastqs/EXT005_L001_R1.fq'
                ),
                filesize=2345,
                checksum='mno345',
            ),
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-04-01/fastqs/EXT005_L001_R2.fq'
                ),
                filesize=3456,
                checksum='pqr456',
            ),
        ],
    ),
    # Two read files (fastq)
    9: Assay(
        id=9,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-04-01/fastqs/EXT005_L002_R1.fq'
                ),
                filesize=4567,
                checksum='stu567',
            ),
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-04-01/fastqs/EXT005_L002_R2.fq'
                ),
                filesize=5678,
                checksum='vwx678',
            ),
        ],
    ),
    # Two read files (fastq)
    10: Assay(
        id=10,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-04-01/fastqs/EXT005_L003_R1.fq'
                ),
                filesize=6789,
                checksum='yza890',
            ),
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-04-01/fastqs/EXT005_L003_R2.fq'
                ),
                filesize=7890,
                checksum='bcd901',
            ),
        ],
    ),
    # One read file (bam) (long-read)
    11: Assay(
        id=11,
        read_files=[
            FileMetadata(
                filepath=to_path(
                    'gs://cpg-dataset-main-upload/2025-05-01/long_read/bams/EXT006.bam'
                ),
                filesize=12345,
                checksum='abcde12345',
            )
        ],
    ),
}

UPLOAD_BUCKET_FILES = [
    # Matches assay 1
    FileMetadata(
        filepath=to_path('gs://cpg-dataset-main-upload/2025-01-01/bams/EXT001.bam'),
        filesize=1234,
        checksum='abc123',
    ),
    # Matches assay 3
    FileMetadata(
        filepath=to_path('gs://cpg-dataset-main-upload/2025-01-01/crams/EXT002.cram'),
        filesize=2345,
        checksum='bcd234',
    ),
    # Matches assay 4
    FileMetadata(
        filepath=to_path(
            'gs://cpg-dataset-main-upload/2025-02-01/exome/fastqs/EXT003_R1.fq'
        ),
        filesize=3456,
        checksum='cde345',
    ),
    FileMetadata(
        filepath=to_path(
            'gs://cpg-dataset-main-upload/2025-02-01/exome/fastqs/EXT003_R2.fq'
        ),
        filesize=4567,
        checksum='def456',
    ),
    # Matches a registered vcf analysis
    FileMetadata(
        filepath=to_path('gs://cpg-dataset-main-upload/vcf/EXT006.vcf'),
        filesize=123456,
        checksum='abcd1234',
    ),
    # Does not match any assay, but contains an external ID in filename
    FileMetadata(
        filepath=to_path(
            'gs://cpg-dataset-main-upload/2025-02-01/exome/fastqs/EXT003_topup_R1.fq.gz'
        ),
        filesize=5679,
        checksum='efg5679',
    ),
    FileMetadata(
        filepath=to_path(
            'gs://cpg-dataset-main-upload/2025-02-01/exome/fastqs/EXT003_topup_R2.fq.gz'
        ),
        filesize=5670,
        checksum='efg5670',
    ),
    # Does not match any assay, and does not contain any external IDs in filename
    # But matches an ingested file based on checksum and size
    FileMetadata(
        filepath=to_path('gs://cpg-dataset-main-upload/unknown_files/unknown_R1.fq'),
        filesize=6789,
        checksum='yza890',
    ),
    FileMetadata(
        filepath=to_path('gs://cpg-dataset-main-upload/unknown_files/unknown_R2.fq'),
        filesize=7890,
        checksum='bcd901',
    ),
    # Does not match any assay, contains no external IDs in filename, does not match
    # any known files
    FileMetadata(
        filepath=to_path('gs://cpg-dataset-main-upload/unknown_files/unknown_2_R1.fq'),
        filesize=8543,
        checksum='sdf123',
    ),
    FileMetadata(
        filepath=to_path('gs://cpg-dataset-main-upload/unknown_files/unknown_2_R2.fq'),
        filesize=7415,
        checksum='szx758',
    ),
]


MAIN_BUCKET_FILES = [
    FileMetadata(to_path('gs://cpg-dataset-main/cram/SG01_1.cram')),
    FileMetadata(to_path('gs://cpg-dataset-main/exome/cram/SG01_2.cram')),
    FileMetadata(to_path('gs://cpg-dataset-main/cram/SG02.cram')),
    FileMetadata(to_path('gs://cpg-dataset-main/exome/cram/SG03.cram')),
    FileMetadata(to_path('gs://cpg-dataset-main/cram/SG04.cram')),
    FileMetadata(to_path('gs://cpg-dataset-main/cram/SG06.cram')),
    FileMetadata(
        to_path('gs://cpg-dataset-main/vcf/SG06.vcf'),
        filesize=123456,
        checksum='abcd1234',
    ),
]

ANALYSES = {
    1: Analysis(
        id=1,
        type='cram',
        output_file=FileMetadata(to_path('gs://cpg-dataset-main/cram/SG01_1.cram')),
        sequencing_group_id='SG01_1',
        timestamp_completed='2025-06-15T12:00:00Z',
    ),
    2: Analysis(
        id=2,
        type='cram',
        output_file=FileMetadata(
            to_path('gs://cpg-dataset-main/exome/cram/SG01_2.cram')
        ),
        sequencing_group_id='SG01_2',
        timestamp_completed='2025-06-16T12:00:00Z',
    ),
    3: Analysis(
        id=3,
        type='cram',
        output_file=FileMetadata(to_path('gs://cpg-dataset-main/cram/SG02.cram')),
        sequencing_group_id='SG02',
        timestamp_completed='2025-06-17T12:00:00Z',
    ),
    4: Analysis(
        id=4,
        type='cram',
        output_file=FileMetadata(to_path('gs://cpg-dataset-main/exome/cram/SG03.cram')),
        sequencing_group_id='SG03',
        timestamp_completed='2025-06-18T12:00:00Z',
    ),
    5: Analysis(
        id=5,
        type='cram',
        output_file=FileMetadata(to_path('gs://cpg-dataset-main/cram/SG05.cram')),
        sequencing_group_id='SG05',
        timestamp_completed='2025-06-19T12:00:00Z',
    ),
    6: Analysis(
        id=6,
        type='cram',
        output_file=FileMetadata(to_path('gs://cpg-dataset-main/cram/SG06.cram')),
        sequencing_group_id='SG06',
        timestamp_completed='2025-06-20T12:00:00Z',
    ),
    7: Analysis(
        id=7,
        type='vcf',
        output_file=FileMetadata(
            to_path('gs://cpg-dataset-main/vcf/SG06.vcf'),
            filesize=123456,
            checksum='abcd1234',
        ),
        sequencing_group_id='SG06',
        timestamp_completed='2025-06-21T12:00:00Z',
    ),
}

SEQUENCING_GROUPS = {
    'SG01_1': SequencingGroup(
        id='SG01_1',
        type='genome',
        technology='short-read',
        platform='illumina',
        sample=SAMPLES['S01'],
        assays=[ASSAYS[1]],
    ),
    'SG01_2': SequencingGroup(  # Our "short-read | illumina | exome" SG
        id='SG01_2',
        type='exome',
        technology='short-read',
        platform='illumina',
        sample=SAMPLES['S01'],
        assays=[ASSAYS[2]],
    ),
    'SG02': SequencingGroup(
        id='SG02',
        type='genome',
        technology='short-read',
        platform='illumina',
        sample=SAMPLES['S02'],
        assays=[ASSAYS[3]],
    ),
    'SG03': SequencingGroup(
        id='SG03',
        type='exome',
        technology='short-read',
        platform='ont',
        sample=SAMPLES['S03'],
        assays=[ASSAYS[4]],
    ),
    'SG04': SequencingGroup(
        id='SG04',
        type='genome',
        technology='short-read',
        platform='illumina',
        sample=SAMPLES['S04'],
        assays=[ASSAYS[5], ASSAYS[6], ASSAYS[7]],
    ),
    'SG05': SequencingGroup(  # This one will not have a cram analysis
        id='SG05',
        type='genome',
        technology='short-read',
        platform='pacbio',
        sample=SAMPLES['S05'],
        assays=[ASSAYS[8], ASSAYS[9], ASSAYS[10]],
        cram_analysis=ANALYSES[5],
    ),
    'SG06': SequencingGroup(  # This will have a cram and a vcf
        id='SG06',
        type='genome',
        technology='long-read',
        platform='pacbio',
        sample=SAMPLES['S06'],
        assays=[ASSAYS[11]],
    ),
}

EXOME_SGS = [sg for sg in SEQUENCING_GROUPS.values() if sg.type == 'exome']
GENOME_SGS = [sg for sg in SEQUENCING_GROUPS.values() if sg.type == 'genome']
ILLUMINA_SGS = [sg for sg in SEQUENCING_GROUPS.values() if sg.platform == 'illumina']
PACBIO_SGS = [sg for sg in SEQUENCING_GROUPS.values() if sg.platform == 'pacbio']
SHORT_READ_SGS = [
    sg for sg in SEQUENCING_GROUPS.values() if sg.technology == 'short-read'
]
LONG_READ_SGS = [
    sg for sg in SEQUENCING_GROUPS.values() if sg.technology == 'long-read'
]

SR_ILLUMINA_EXOME_SGS = [
    sg for sg in ILLUMINA_SGS if sg in EXOME_SGS and sg in SHORT_READ_SGS
]

ALL_SEQUENCING_GROUP_ASSAYS_RESPONSE = {
    'project': {
        'sequencingGroups': [sg.to_gql_dict() for sg in SEQUENCING_GROUPS.values()],
    }
}

SR_ILLUMINA_EXOME_SGS_ASSAYS_RESPONSE = {
    'project': {'sequencingGroups': [sg.to_gql_dict() for sg in SR_ILLUMINA_EXOME_SGS]}
}

ALL_SEQUENCING_GROUP_ANALYSES_RESPONSE = {
    'project': {
        'sequencingGroups': [
            {
                'id': sg.id,
                'analyses': [
                    analysis.to_gql_dict()
                    for analysis in ANALYSES.values()
                    if analysis.sequencing_group_id == sg.id
                ],
            }
            for sg in SEQUENCING_GROUPS.values()
        ],
    }
}

SR_ILLUMINA_EXOME_SGS_CRAM_RESPONSE = {
    'project': {
        'sequencingGroups': [
            {
                'id': sg.id,
                'analyses': [
                    analysis.to_gql_dict()
                    for analysis in ANALYSES.values()
                    if analysis.sequencing_group_id == sg.id and analysis.is_cram
                ],
            }
            for sg in SR_ILLUMINA_EXOME_SGS
        ],
    }
}
