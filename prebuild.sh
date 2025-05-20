#!/bin/bash
# Prebuild script for App Runner to install system dependencies

# Exit on error
set -e

echo "Installing system dependencies..."
apt-get update -y
apt-get install -y ffmpeg libsm6 libxext6

echo "System dependencies installed successfully"
echo "FFmpeg version:"
ffmpeg -version | head -n 1

# Create necessary directories
echo "Creating application directories..."
mkdir -p /tmp/autoposter/temp
mkdir -p /tmp/autoposter/output
mkdir -p /tmp/autoposter/download
echo "Directories created successfully"

# Success
echo "Prebuild completed successfully"