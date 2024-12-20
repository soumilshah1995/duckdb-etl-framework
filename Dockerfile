# Build stage
FROM python:3.9-slim as builder

WORKDIR /app

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.9-slim

WORKDIR /app

# Copy installed dependencies from builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

# Copy your application
COPY template/template.py .

# Install AWS CLI
RUN apt-get update && \
    apt-get install -y awscli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set up environment variables for AWS credentials
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_DEFAULT_REGION="us-east-1"


