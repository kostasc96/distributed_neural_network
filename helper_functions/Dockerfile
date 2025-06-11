# Use official Python slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file and fix any format issues
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the Python scripts into the image
COPY compute_accuracy.py initial_script.py ./

# Default to an interactive shell so you can exec in and run scripts manually
CMD ["bash"]