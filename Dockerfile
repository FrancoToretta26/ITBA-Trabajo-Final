# Dockerfile

# Usa una imagen base de Python
FROM python:3.8-slim

# Instala dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instala Rust y Cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copia los archivos de tu proyecto
COPY . /app
WORKDIR /app

FROM apache/airflow:2.2.3

# Instala las dependencias de Python
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Instalar psycopg2 para Redshift
RUN pip install psycopg2-binary

# Comando para ejecutar Airflow
CMD ["airflow", "webserver"]