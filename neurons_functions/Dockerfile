##############################
# 1) Builder stage: build wheels
##############################
FROM python:3.9-slim AS builder
WORKDIR /build

# Install build tools
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

# Copy only your package + its requirements
COPY setup.py requirements.txt ./
COPY pcomp_utils/ ./pcomp_utils/

# Build wheels for everything (pcomp_utils, intel-numpy, etc.)
RUN pip install --upgrade pip wheel setuptools \
 && pip wheel --wheel-dir=/build/wheels --no-cache-dir -r requirements.txt . \
 && apt-get purge -y build-essential \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

#####################################
# 2) Runtime stage: lean final image
#####################################
FROM python:3.9-slim AS runtime
WORKDIR /app

# Copy in ONLY the wheels from the builder
COPY --from=builder /build/wheels /wheels

# Install them (no network) then delete the wheel cache
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*.whl \
 && rm -rf /wheels

# Copy your Knative wrapper
COPY neuron_function.py .

# Make sure this matches the port your Flask app listens on!
EXPOSE 5000

# Tag the runtime stage as 'latest'
CMD ["python", "neuron_function.py"]
