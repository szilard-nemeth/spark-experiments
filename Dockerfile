# Use the official Apache Spark image
FROM apache/spark:3.5.0

USER root

# Install Python and basic build tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install poetry
# Using the ENV variable is more compatible than the command line flag
ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN pip3 install --upgrade pip && pip3 install poetry

WORKDIR /app

# Ensure event log directory exists
RUN mkdir -p /opt/spark/events && chmod 777 /opt/spark/events

# Copy dependency files
COPY pyproject.toml poetry.lock* /app/

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy your script
COPY . /app/

USER 185