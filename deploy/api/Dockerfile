FROM python:3.11

ARG SM_ENVIRONMENT

ENV PORT 8000
ENV SM_ENVIRONMENT ${SM_ENVIRONMENT}
# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

# Create a non-root user and group
RUN groupadd -r appuser && useradd -r -g appuser -d /home/appuser -m -s /bin/bash appuser

# Set the working directory
WORKDIR /app/sample_metadata/

# Copy requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --no-deps -r requirements.txt

# Copy the application code
COPY api api

# Change ownership of the application directory to the non-root user
RUN chown -R appuser:appuser /app/sample_metadata/

# Switch to the non-root user
USER appuser

EXPOSE $PORT

# Command to run the FastAPI app
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "$PORT"]
