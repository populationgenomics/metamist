from metamist.audit.models import (
    Assay,
    FileMetadata,
)

from cpg_utils import to_path

READ_FILES_BY_ASSAY_ID = {
    # One read file (BAM) (in bucket)
    1: [
        FileMetadata(
            filepath=to_path('gs://cpg-dataset-main-upload/2025-01-01/bams/EXT001.bam'),
            filesize=1234,
            checksum='abc123',
        ),
    ],
    # One read file (BAM) (exome)
    2: [
        FileMetadata(
            filepath=to_path(
                'gs://cpg-dataset-main-upload/2025-01-01/bams/exome/EXT001.bam'
            ),
            filesize=321,
            checksum='cba321',
        ),
    ],
    # One read file (CRAM)
    3: [
        FileMetadata(
            filepath=to_path(
                'gs://cpg-dataset-main-upload/2025-01-01/crams/EXT002.cram'
            ),
            filesize=1953,
            checksum='asd456',
        ),
    ],
    # Two read files (fastq) (exome) (in bucket)
    4: [
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
    # Two read files (fastq)
    5: [
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
    # Two read files (fastq)
    6: [
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
    # Two read files (fastq)
    7: [
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
    # Two read files (fastq)
    8: [
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
    # Two read files (fastq)
    9: [
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
    # Two read files (fastq)
    10: [
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
    # One read file (bam) (long-read)
    11: [
        FileMetadata(
            filepath=to_path(
                'gs://cpg-dataset-main-upload/2025-05-01/long_read/bams/EXT006.bam'
            ),
            filesize=12345,
            checksum='abcde12345',
        )
    ],
}

ASSAYS = {
    assay_id: Assay(id=assay_id, read_files=read_files)
    for assay_id, read_files in READ_FILES_BY_ASSAY_ID.items()
}

ASSAY_WITH_SECONDARY_FILE_GQL_DICT = {
    'id': 101,
    'meta': {
        'reads_type': 'cram',
        'reads': {
            'location': 'gs://cpg-dataset-main-upload/2023-05-01/EXT101.cram',
            'basename': 'EXT101.cram',
            'size': 29217574937,
            'secondaryFiles': [
                {
                    'location': 'gs://cpg-dataset-main-upload/2023-05-01/EXT101.cram.crai',
                    'basename': 'EXT101.cram.crai',
                    'size': 1529566,
                },
                {
                    # missing 'location' field for testing
                    # 'location': '',
                    'basename': 'EXT101.cram.crai.x',
                    'size': 15295,
                },
            ],
        },
    },
}
