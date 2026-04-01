FROM apache/airflow:2.9.0

USER root

# Instalar Java + herramientas de compilación (CLAVE)
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk \
        ant \
        procps \
        wget \
        build-essential \
        gcc \
        g++ \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Configurar JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/

USER airflow

# Copiar el archivo requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Instalar paquetes desde el archivo requirements.txt
RUN pip install --upgrade pip && \
    pip install --default-timeout=1000 --no-cache-dir \
    -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt" 

RUN pip install --no-cache-dir --force-reinstall --upgrade pyspark==3.5.5

