FROM bitnami/spark:3.5

USER root

# Install Poetry
RUN pip install poetry

# Set working directory
WORKDIR /app

# Copy poetry files
COPY pyproject.toml poetry.lock* /app/

# Install dependencies
# We use --without-hashes for speed and --no-root because we just want the libs
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy the rest of your app
COPY . /app/

USER 1001