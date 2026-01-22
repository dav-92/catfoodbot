FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py .

# Create volume for persistent data
VOLUME ["/app/data"]

# Environment variables
ENV DATABASE_URL=sqlite:///data/catfood.db
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "main.py"]
