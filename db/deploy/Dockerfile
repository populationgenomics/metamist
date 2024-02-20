# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install JDK 17 (Temurin distribution)
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget ca-certificates gnupg software-properties-common && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - && \
    add-apt-repository --yes https://packages.adoptium.net/artifactory/deb/ && \
    apt-get update && \
    apt-get install -y temurin-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the Liquibase project file
COPY project.xml .
# Copy the modified Python script for fetching credentials and executing Liquibase
COPY deploy/liquibase_schema_update.py .

# Install Google Cloud SDK and Python Secret Manager library
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk && \
    pip install --no-cache-dir google-cloud-secret-manager && \
    rm -rf /var/lib/apt/lists/*

# Download Liquibase
RUN wget https://github.com/liquibase/liquibase/releases/download/v4.26.0/liquibase-4.26.0.zip -O liquibase.zip && \
    unzip liquibase.zip -d liquibase && \
    rm liquibase.zip

# Download MariaDB JDBC Driver
RUN wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.0.3/mariadb-java-client-3.0.3.jar -O mariadb-java-client-3.0.3.jar

# Ensure the script is executable
RUN chmod +x fetch_credentials.py

# Set the ENTRYPOINT to run the Liquibase command
ENTRYPOINT ["./fetch_credentials.py"]
