FROM apache/airflow:2.9.1-python3.11

# Install additional Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dags and plugins
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

# Copy .env to container
COPY .env /opt/airflow/.env

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
