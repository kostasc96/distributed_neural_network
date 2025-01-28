# Use a Python base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies, including Jupyter
RUN pip install --no-cache-dir -r requirements.txt && \
    pip show jupyter

# Copy the pcomp_utils folder and setup.py into the container
COPY pcomp_utils/ ./pcomp_utils/
COPY setup.py .

# Install the build module
RUN pip install --no-cache-dir build

# Build the wheel from setup.py
RUN python -m build --wheel

# Install the built wheel package
RUN pip install --no-cache-dir dist/*.whl

# Command to verify the package installation (can be replaced with actual run command)
CMD ["python", "-c", "import pcomp; print('Package installed successfully!')"]

# Expose the port for Jupyter Notebook
EXPOSE 8888

# Set a default command for the container
CMD ["bash"]
