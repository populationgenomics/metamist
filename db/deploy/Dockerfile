# Use the official lightweight Python image.
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV LIQUIBASE_VERSION=4.26.0
ENV MARIADB_JDBC_VERSION=3.0.3

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends gcc git ssh default-jdk wget unzip

# Download and install Liquibase
RUN wget https://github.com/liquibase/liquibase/releases/download/v${LIQUIBASE_VERSION}/liquibase-${LIQUIBASE_VERSION}.zip \
    && unzip liquibase-${LIQUIBASE_VERSION}.zip -d /opt/liquibase \
    && chmod +x /opt/liquibase/liquibase \
    # Clean up to reduce layer size
    && rm liquibase-${LIQUIBASE_VERSION}.zip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download the MariaDB JDBC driver
RUN wget https://downloads.mariadb.com/Connectors/java/connector-java-${MARIADB_JDBC_VERSION}/mariadb-java-client-${MARIADB_JDBC_VERSION}.jar
RUN mv mariadb-java-client-${MARIADB_JDBC_VERSION}.jar /opt/

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install Python dependencies
RUN python3 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

# Run the FastAPI app on container startup
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
