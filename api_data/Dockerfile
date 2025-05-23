# Use official Python image
FROM python:3.12-slim

# Set work directory
WORKDIR /app

# Copy all files from api_data to /app
COPY . /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose FastAPI port
EXPOSE 8000

# Run the FastAPI app
CMD ["python", "mock_api.py"]
