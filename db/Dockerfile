FROM postgres:18

# Copy dbmate binary from official image
COPY --from=ghcr.io/amacneil/dbmate:2 /usr/local/bin/dbmate /usr/local/bin/dbmate

RUN apt-get update \
    # Install PostgreSQL development headers and PGXN client
    && apt-get install -y \
        postgresql-server-dev-18 \
        pgxnclient \
        make \
        gcc \
    # Install the 'temporal_tables' extension using PGXN
    && pgxn install temporal_tables \
    # Clear apt cache to reduce image size
    && rm -rf /var/lib/apt/lists/*
