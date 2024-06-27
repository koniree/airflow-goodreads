# Use the official Apache Airflow image
FROM apache/airflow:2.9.1

# Switch to the root user to install system packages
USER root

# Install any necessary system packages
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy the requirements file and install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the custom goodreadsfaker module
COPY dags/goodreadsfaker /opt/airflow/dags/goodreadsfaker

# Set the PYTHONPATH environment variable to include your custom module
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags/goodreadsfaker"

# Copy the DAGs folder
COPY dags /opt/airflow/dags
