# Use the official Python image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Run the FastAPI application with uvicorn
CMD ["uvicorn", "sensorAPI:app", "--host", "0.0.0.0", "--port", "85"]