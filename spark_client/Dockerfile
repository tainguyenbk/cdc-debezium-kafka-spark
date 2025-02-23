FROM bitnami/spark:3.4.1

# Install necessary packages
USER root
RUN apt-get update && apt-get install -y curl

# Install MinIO Client
RUN curl -o mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc && mv mc /usr/local/bin/

# Set MinIO alias
RUN mc alias set dataops_minio http://minio:9000 dataops Ankara06 --api S3v4

# Copy requirements.txt and install dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt