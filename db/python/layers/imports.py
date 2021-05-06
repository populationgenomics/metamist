from typing import List, Dict, Iterable

from db.python.sample import SampleTable
from db.python.sequencing import SampleSequencingTable
from db.python.sequencingstatus import SampleSequencingStatusTable

from models.enums import SampleType, SequencingType, SequencingStatus


class ImportLayer:
    """Layer for import logic"""

    def __init__(self, connection):
        self.connection = connection

    def import_airtable_manifest(self, rows: Iterable[Dict[str, any]]):
        """
        Import airtable manifest from formed row objects,
        where the key is the header. Imports to a combination
        of sample, sequencing and sequencing_status tables,
        where additional details are placed in the sequencing.meta
        """

        sample_meta_keys = set(
            [
                'Specimen Type',
                'Concentration provided by Linda (ng/ul)*',
                'Concentration (ng/ul)*',
                'Volume (ul)*',
                'Notes',
                'OneK1K Data?',
            ]
        )
        with self.connection:
            # open a transaction
            sample_table = SampleTable(connection=self.connection)
            seq_table = SampleSequencingTable(connection=self.connection)
            seq_status_table = SampleSequencingStatusTable(connection=self.connection)

            for obj in rows:
                external_sample_id = obj.pop('Sample ID')
                sample_type = ImportLayer.parse_specimen_type_to_sample_type(
                    obj.get('Specimen Type', obj.get('Specimen Type*'))
                )
                sequence_status = ImportLayer.parse_row_status(obj.get('Status'))

                sequence_meta_keys = [
                    k for k in obj.keys() if k not in sample_meta_keys
                ]
                sample_meta = {k: obj[k] for k in sample_meta_keys if obj.get(k)}
                sequence_meta = {k: obj[k] for k in sequence_meta_keys if obj.get(k)}

                internal_sample_id = sample_table.insert_sample(
                    external_id=external_sample_id,
                    active=True,
                    sample_meta=sample_meta,
                    sample_type=sample_type,
                    commit=False,
                )

                sequence_id = seq_table.insert_sequencing(
                    sample_id=internal_sample_id,
                    sequence_type=SequencingType.wgs,
                    sequence_meta=sequence_meta,
                    commit=False,
                )

                seq_status_table.insert_sequencing_status(
                    sequence_id=sequence_id, status=sequence_status, commit=False
                )
                print(f'Inserting sequencing with internal ID: {internal_sample_id}')

            self.connection.commit()

    def import_airtable_manifest_csv(
        self, headers: List[str], rows: Iterable[List[str]]
    ):
        """
        Call self.import_airtable_manifest, by converting rows to
        well formed objects, keyed by the corresponding header.
        """

        class RowToDictIterator:
            """Build object from headers and rows, but as an iterator"""

            def __init__(self, headers, rows):
                self.headers = headers
                self.rows = rows

            def __iter__(self):
                return self

            def __next__(self):
                # I originaly wrote this as a list comprehension
                #   { k: v for k,v in zip(...) }
                # but the pylint error 'unnecessary-comprehension'
                # suggests this as the better way:
                return dict(zip(self.headers, next(rows)))

        return self.import_airtable_manifest(RowToDictIterator(headers, rows))

    @staticmethod
    def parse_specimen_type_to_sample_type(specimen_type: str) -> SampleType:
        """Take the airtable 'Specimen Type' and return a SampleType"""
        if 'blood' in specimen_type.lower():
            return SampleType.blood

        if 'saliva' in specimen_type.lower():
            return SampleType.saliva

        raise Exception(f"Couldn't determine sample type from '{specimen_type}'")

    @staticmethod
    def parse_row_status(row_status: str) -> SequencingStatus:
        """Take the airtable 'Status' and return a SequencingStatus"""
        row_status_lower = row_status.lower()
        if row_status_lower == 'sequencing complete':
            return SequencingStatus.completed_sequencing
        if row_status_lower == 'qc complete':
            return SequencingStatus.completed_qc
        if row_status_lower == 'upload successful':
            return SequencingStatus.uploaded
        if row_status_lower == 'failed':
            return SequencingStatus.failed_qc

        raise ValueError(f"Couldn't parse sequencing status '{row_status}'")
