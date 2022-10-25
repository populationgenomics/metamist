"""
We need to apply this migration in three steps:

- Add schema for files
- migrate all the data to new schema
- Remove old columns
    + MariaDB will probably complain about deleting columns in archived data

This is also the time we should consider archiving analysis with duplicate output
paths, and also
"""
from collections import defaultdict

from models.models.file import File

connection = None


def main():
    pass


async def migrate_analysis():
    # collect all analysis-entries that are gs:// files
    get_query = 'SELECT * FROM analysis WHERE output LIKE "gs://%"'
    rows = await connection.fetch_all(get_query)
    mapped_files = defaultdict(list)
    for r in rows:
        \mapped_files[r['output']].append(r['id'])

    inserted_files = insert_files(list(mapped_files.keys()))

    for f in inserted_files:
        File(path=)


