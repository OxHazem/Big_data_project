# Use a robust base image that already has Java, Spark, and Python installed
FROM jupyter/pyspark-notebook:spark-3.5.0

# Switch to root to install system dependencies if needed
USER root

# Set the working directory inside the container
WORKDIR /app

# Copy your requirements file into the container
COPY requirements.txt .

# Install your specific Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the standard user for security
USER jovyan