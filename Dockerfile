# Dockerfile

# Use a specific, stable version of the official Airflow image as base
FROM apache/airflow:2.8.1-python3.11

# Set the AIRFLOW_HOME environment variable inside the container
ENV AIRFLOW_HOME=/opt/airflow

# Copy your project's requirements.txt into the container
COPY requirements.txt /tmp/requirements.txt

# Install your custom Python dependencies without Airflow constraints
# Remove any strict versions that conflict with Airflow (e.g., aiohttp)
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy your entire project directory into the container's AIRFLOW_HOME
COPY . ${AIRFLOW_HOME}

# Set the working directory
WORKDIR ${AIRFLOW_HOME}

# Install additional Python packages
RUN pip install --no-cache-dir plotly pandas duckdb