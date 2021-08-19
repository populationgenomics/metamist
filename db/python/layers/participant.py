from typing import Dict

from db.python.tables.participant import ParticipantTable
from db.python.tables.sample import SampleTable

from db.python.layers.base import BaseLayer


class ParticipantLayer(BaseLayer):
    """Layer for more complex sample logic"""

    async def fill_in_missing_participants(self):
        """Update the sequencing status from the internal sample id"""
        sample_table = SampleTable(connection=self.connection)
        participant_table = ParticipantTable(connection=self.connection)

        samples_with_no_pids_by_internal_id: Dict[
            int, str
        ] = await sample_table.get_sample_with_missing_participants_by_internal_id(
            self.connection.project
        )
        samples_with_no_pid_by_external_id: Dict[str, int] = {
            v: k for k, v in samples_with_no_pids_by_internal_id.items()
        }
        ext_sample_id_to_pid = {}

        async with self.connection.connection.transaction():
            sample_ids_to_update = {}
            for external_id, sample_id in samples_with_no_pid_by_external_id.items():
                participant_id = await participant_table.create_participant(
                    external_id=external_id
                )
                ext_sample_id_to_pid[external_id] = participant_id
                sample_ids_to_update[sample_id] = participant_id

            await sample_table.update_many_participant_ids(
                list(sample_ids_to_update.keys()), list(sample_ids_to_update.values())
            )

        return f'Updated {len(sample_ids_to_update)} records'
