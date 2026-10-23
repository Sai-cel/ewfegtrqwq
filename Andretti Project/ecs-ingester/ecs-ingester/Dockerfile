# Use Ubuntu 22.04 (Jammy) as the base

FROM ubuntu:22.04
 
# Set DEBIAN_FRONTEND to noninteractive to avoid prompts during apt-get

ENV DEBIAN_FRONTEND=noninteractive
 
# Install FFmpeg and Python dependencies

RUN apt-get update && apt-get install -y \

    ffmpeg \

    python3 \

    python3-pip \

    awscli \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*
 
# Set working directory

WORKDIR /app
 
# Copy requirements first (better build caching) and install ALL Python deps

# requirements.txt includes boto3, amazon-transcribe, aiohttp

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r requirements.txt
 
# Copy all application files

COPY ingester.py /app/ingester.py

COPY streaming-ingester.py /app/streaming-ingester.py
 
# Run the batch ingester: chunking + parallel full-audio recording.

# Prod logs confirm production runs this batch ingester (log group ecs/v2_req_ingester,

# segment-by-segment chunking, no streaming activity).

CMD ["python3", "ingester.py"]
 