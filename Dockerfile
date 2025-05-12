FROM apache/airflow:2.10.5

USER airflow
#Copy requirements.txt file
COPY requirements.txt /tmp/requirements.txt

#Install python packages as root
RUN pip install --no-cache-dir -r /tmp/requirements.txt

#SWitch back to airflow user for safety
USER airflow