FROM apache/airflow:3.0.2


COPY requirements.txt /


USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir kafka-python
RUN pip install --no-cache-dir apache-airflow-providers-apache-kafka==1.4.0

