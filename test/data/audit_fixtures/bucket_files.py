"""
Simulated bucket files for testing the audit module.
"""

from metamist.audit.models import (
    FileMetadata,
)

from cpg_utils import to_path


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
    FileMetadata(to_path('gs://cpg-dataset-main/archive/files.tar.gz')),
]


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
