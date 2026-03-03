FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:0.10.6 /uv /uvx /bin/

ARG SM_ENVIRONMENT

ENV PORT=8000
ENV SM_ENVIRONMENT=${SM_ENVIRONMENT}
# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED=1

ENV UV_COMPILE_BYTECODE=1

ENV UV_LINK_MODE=copy

# Create a non-root user and group
RUN groupadd -r appuser && useradd -r -g appuser -d /home/appuser -m -s /bin/bash appuser

# Set the working directory
WORKDIR /app/metamist/

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project

# Copy the application code
COPY api api
COPY db db/
COPY models models/


# Change ownership of the application directory to the non-root user
RUN chown -R appuser:appuser /app/metamist/

# Switch to the non-root user
USER appuser

ENV PATH="/app/metamist/.venv/bin:$PATH"
