# Dockerfile for Cloud Run migration job using dbmate
FROM python:3.12-slim

COPY --from=ghcr.io/amacneil/dbmate:2 /usr/local/bin/dbmate /usr/local/bin/dbmate

COPY --from=ghcr.io/astral-sh/uv:0.10.6 /uv /usr/local/bin/uv

# Create migrations directory
RUN mkdir -p /db/migrations

# Copy the migrate script and its lockfile
COPY deploy/migrations/migrate.py /migrate.py
COPY deploy/migrations/migrate.py.lock /migrate.py.lock

# Sync script dependencies (uses the lockfile)
RUN uv sync --script /migrate.py

# Default environment variables
ENV DBMATE_NO_DUMP_SCHEMA=true
ENV DBMATE_MIGRATIONS_DIR=/db/migrations

# Run script offline (dependencies already synced)
ENTRYPOINT ["uv", "run", "--offline", "/migrate.py"]
