"""Utility functions for the social media autoposter"""
import os
import logging
import boto3
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('social_media_autoposter_utils')

def init_s3_client():
    """Initialize and return an S3 client"""
    return boto3.client('s3')

def create_temp_directories(temp_dir, output_dir, download_dir):
    """Create necessary directories for processing"""
    for directory in [temp_dir, output_dir, download_dir]:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")

def cleanup_directories(temp_dir, download_dir):
    """Clean up temporary files and directories"""
    try:
        for dir_path in [temp_dir, download_dir]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
        logger.info("Cleaned up temporary files and directories")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")