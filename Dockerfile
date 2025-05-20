FROM python:3.9-slim

WORKDIR /app

# Install system dependencies that might be needed for psycopg
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Environment variables will be injected by Docker Compose
# CMD ["python", "main.py"]
# Instead of CMD, we'll use a command in docker-compose.yml to loop the script execution

