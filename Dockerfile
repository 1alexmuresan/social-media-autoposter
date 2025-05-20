FROM python:3.9-slim

# Install ffmpeg and other system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsm6 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /tmp/autoposter/temp /tmp/autoposter/output /tmp/autoposter/download

# Set environment variables
ENV ASSETS_BUCKET=marketing-automation-static
ENV LONG_VIDEOS_BUCKET=longs-clips
ENV SHORTS_REELS_BUCKET=shorts-clips
ENV CONFIG_BUCKET=marketing-automation-static

# Expose port
EXPOSE 8080

# Run with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]
