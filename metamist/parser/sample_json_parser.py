#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order,unused-argument
import logging

from metamist.parser.generic_metadata_parser import (
    GenericMetadataParser
)

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

__DOC = """
Sample JSON parser - TODO add more details

JSON data example:
{"identifier": "AB0002", "name": "j smith", "age": 50, "dob": "5/05/1950", "measurement": "98.7", "observation": "B++", "receipt_date": "1/02/2023"}

"""


class SampleJsonColumns:
    """Column keys for JSON Sample Data"""

    IDENTIFIER = 'identifier'
    NAME = 'name'
    AGE = 'age'
    DOB = 'dob'
    MEASUREMENT = 'measurement'
    OBSERVATION = 'observation'
    RECEIPT_DATE = 'receipt_date'

    @staticmethod
    def participant_meta_map():
        """Participant meta map"""
        return {}

    @staticmethod
    def sequence_meta_map():
        """Columns that will be put into sequence.meta"""
        return {}

    @staticmethod
    def sample_meta_map():
        """Columns that will be put into sample.meta"""
        return {}


class SampleJsonParser(GenericMetadataParser):
    """Parser for Sample JSON Records"""

    def __init__(
        self,
        project: str
    ):
        super().__init__(
            project=project,
            search_locations=[],
            participant_meta_map=SampleJsonColumns.participant_meta_map(),
            sample_meta_map=SampleJsonColumns.sample_meta_map(),
            assay_meta_map=SampleJsonColumns.sequence_meta_map(),
            sample_name_column=SampleJsonColumns.IDENTIFIER,
            qc_meta_map={}
        )

    async def parse(
        self, record: str, confirm=False, dry_run=False
    ):
        """Parse passed record """
        raise NotImplementedError()
