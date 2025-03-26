# Use a Python base image
FROM python:3.9-slim

# Install system build essentials and Eigen
RUN apt-get update && apt-get install -y build-essential libeigen3-dev

# Install system build essentials (recommended for compiling)
RUN apt-get update && apt-get install -y build-essential

# Install pybind11 and build tooling BEFORE build
RUN pip install --upgrade pip
RUN pip install pybind11 wheel setuptools

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

# ✅ Directly build the wheel without isolated environment (so it sees pybind11)
RUN python setup.py bdist_wheel

# Install the built wheel package
RUN pip install --no-cache-dir dist/*.whl

# Command to verify the package installation (can be replaced with actual run command)
CMD ["python", "-c", "import pcomp; print('Package installed successfully!')"]

# Expose the port for Jupyter Notebook
EXPOSE 8888

# Set a default command for the container
CMD ["bash"]
