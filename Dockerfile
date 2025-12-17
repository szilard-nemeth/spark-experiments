# Use the official Apache Spark image
FROM apache/spark:3.5.0

USER root

# Official images are slim; we need to install python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip3 install poetry --break-system-packages

WORKDIR /app

# Ensure event log directory exists (Apache Spark uses /opt/spark by default)
RUN mkdir -p /opt/spark/events && chmod 777 /opt/spark/events

# Copy dependency files
COPY pyproject.toml poetry.lock* /app/

# Install dependencies into system python
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy your script
COPY . /app/

# Switch back to the default spark user (UID 185)
USER 185