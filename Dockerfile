# Use the official Python image
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code
COPY processor.py .

# Expose the port used by the app
EXPOSE 8000

# Run the Shiny app using the correct Python module
CMD ["python", "-m", "shiny", "run", "processor.py", "--host", "0.0.0.0", "--port", "8000"]
