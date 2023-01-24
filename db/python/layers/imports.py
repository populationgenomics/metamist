from typing import List, Dict, Iterable, Any, Optional

from models.enums import SampleType, SequenceType, SequenceStatus
from models.models.sequence import SampleSequencing

from db.python.tables.sample import SampleTable
from db.python.tables.sequence import SampleSequencingTable

from db.python.layers.base import BaseLayer


class ImportLayer(BaseLayer):
    """Layer for import logic"""

    async def import_airtable_manifest(self, rows: Iterable[Dict[str, Any]]):
        """
        Import airtable manifest from formed row objects,
        where the key is the header. Imports to a combination
        of sample, sequencing and sequencing_status tables,
        where additional details are placed in the sequencing.meta
        """

        sample_meta_keys = {
            'Specimen Type',
            'Concentration provided by Linda (ng/ul)*',
            'Concentration (ng/ul)*',
            'Volume (ul)*',
            'Notes',
            'OneK1K Data?',
        }
        inserted_sample_ids = []

        async with self.connection.connection.transaction():
            # open a transaction
            sample_table = SampleTable(self.connection)
            seq_table = SampleSequencingTable(self.connection)

            sequences: List[SampleSequencing] = []

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

                internal_sample_id = await sample_table.insert_sample(
                    external_id=external_sample_id,
                    active=True,
                    meta=sample_meta,
                    sample_type=sample_type,
                )
                inserted_sample_ids.append(internal_sample_id)
                sequence = SampleSequencing(
                    id_=None,
                    sample_id=internal_sample_id,
                    type=SequenceType.GENOME,
                    meta=sequence_meta,
                    status=sequence_status,
                )
                sequences.append(sequence)

            await seq_table.insert_many_sequencing(sequences)

        return inserted_sample_ids

    async def import_airtable_manifest_csv(
        self, headers: List[str], rows: Iterable[List[str]]
    ):
        """
        Call self.import_airtable_manifest, by converting rows to
        well formed objects, keyed by the corresponding header.
        """

        drows = [dict(zip(headers, r)) for r in rows]
        return await self.import_airtable_manifest(drows)

    @staticmethod
    def parse_specimen_type_to_sample_type(specimen_type: Optional[str]) -> SampleType:
        """Take the airtable 'Specimen Type' and return a SampleType"""
        if not specimen_type:
            raise Exception(f"Couldn't determine sample type from {specimen_type!r}")

        if 'blood' in specimen_type.lower():
            return SampleType.BLOOD

        if 'saliva' in specimen_type.lower():
            return SampleType.SALIVA

        raise Exception(f"Couldn't determine sample type from {specimen_type!r}")

    @staticmethod
    def parse_row_status(row_status: Optional[str]) -> SequenceStatus:
        """Take the airtable 'Status' and return a SequencingStatus"""
        if not row_status:
            return SequenceStatus.UNKNOWN
        row_status_lower = row_status.lower()
        if row_status_lower == 'sequencing complete':
            return SequenceStatus.COMPLETED_SEQUENCING
        if row_status_lower == 'qc complete':
            return SequenceStatus.COMPLETED_QC
        if row_status_lower == 'upload successful':
            return SequenceStatus.UPLOADED
        if row_status_lower == 'failed':
            return SequenceStatus.FAILED_QC
        if row_status_lower == '':
            return SequenceStatus.UNKNOWN

        raise ValueError(f"Couldn't parse sequencing status {row_status!r}")


# if __name__ == '__main__':

# async def import_csv():

#     import csv
#     from db.python.connect import SMConnections

#     author = 'michael.franklin@populationgenomics.org.au'
#     con = SMConnections.get_connection_for_project('dev', author)
#     csv_path = '<manifest-file>.csv'
#     with open(csv_path, encoding='utf-8-sig') as csvfile:
#         csvreader = csv.reader(csvfile)
#         headers = next(csvreader)

#         await ImportLayer(con).import_airtable_manifest_csv(headers, csvreader)

# import asyncio
# asyncio.run(import_csv())
