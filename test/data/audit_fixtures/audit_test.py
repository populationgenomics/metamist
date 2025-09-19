"""
Contains fixtured data for the audit unit tests
"""

from metamist.audit.models import (
    SequencingGroup,
    Analysis,
    Sample,
    Participant,
    ExternalIds,
    FileMetadata,
)

from test.data.audit_fixtures.read_files import (
    ASSAYS,
    ASSAY_WITH_SECONDARY_FILE_GQL_DICT,
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

ALL_SG_ASSAYS_RESPONSE = {
    'project': {
        'sequencingGroups': [sg.to_gql_dict() for sg in SEQUENCING_GROUPS.values()],
    }
}

SG_ASSAYS_RESPONSE_WITH_SECONDARY_FILES = {
    'project': {
        'sequencingGroups': [
            {
                'id': 'SG101',
                'type': 'exome',
                'technology': 'short-read',
                'platform': 'illumina',
                'sample': SAMPLES['S01'].to_gql_dict(),
                'assays': [ASSAY_WITH_SECONDARY_FILE_GQL_DICT],
            }
        ]
    }
}

SR_ILLUMINA_EXOME_SGS_ASSAYS_RESPONSE = {
    'project': {'sequencingGroups': [sg.to_gql_dict() for sg in SR_ILLUMINA_EXOME_SGS]}
}

ALL_SG_ANALYSES_RESPONSE = {
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

ANALYSIS_WITH_OUTPUTS_GQL_RESPONSE = {
    'project': {
        'sequencingGroups': [
            {
                'id': 'SG101',
                'analyses': [
                    {
                        'id': 101,
                        'type': 'cram',
                        'status': 'completed',
                        'meta': {},
                        'output': 'gs://cpg-dataset-main-upload/2023-05-01/EXT101.cram',
                        'outputs': {
                            'id': 123,
                            'path': 'gs://cpg-dataset-main-upload/2023-05-01/EXT101.cram',
                            'basename': 'EXT101.cram',
                            'dirname': 'gs://cpg-dataset-main-upload/2023-05-01',
                            'nameroot': 'EXT101',
                            'nameext': '.cram',
                            'file_checksum': 'pyVQxQ==',
                            'size': 29330411025,
                        },
                        'timestampCompleted': '2023-05-01T00:00:00Z',
                    }
                ],
            }
        ],
    }
}


ANALYSES_WITH_MALFORMED_TIMESTAMPS_GQL_RESPONSE = {
    'project': {
        'sequencingGroups': [
            {
                'id': 'SG101',
                'analyses': [
                    {
                        'id': 101,
                        'type': 'cram',
                        'status': 'completed',
                        'meta': {},
                        'output': 'gs://cpg-dataset-main-upload/2023-05-01/EXT101.cram',
                        'timestampCompleted': 'abcde',
                    },
                    {
                        'id': 102,
                        'type': 'cram',
                        'status': 'completed',
                        'meta': {},
                        'output': 'gs://cpg-dataset-main-upload/2023-05-01/EXT102.cram',
                        # Removed for testing
                        # 'timestampCompleted': '',
                    },
                ],
            }
        ],
    }
}
