FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY api/ ./
COPY config/ ./config/

# Create models directory (will be populated by volume mount)
RUN mkdir -p ./models

# Expose port
EXPOSE 8000

# Run the application
CMD ["python", "main.py"]
