FROM python:3.13-slim

# Set environment variables
ENV POETRY_VERSION=1.8.2
ENV LIVE=true
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl build-essential

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Copy project files
COPY . /app
COPY src /app/src/

# Install dependencies
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Run unit tests after installing packages
RUN PYTHONPATH=src poetry run pytest tests/ --maxfail=1 --disable-warnings

# Create required directories
RUN mkdir -p /app/data/bronze /app/data/silver /app/data/db

# Expose ports (FastAPI + Streamlit)
EXPOSE 8000 8501

# Entrypoint script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

CMD ["/app/start.sh"]