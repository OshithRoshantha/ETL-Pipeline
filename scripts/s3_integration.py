import boto3
from botocore.exceptions import ClientError
import os
import sys
import logging
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import AWS_CONFIG

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.propagate = False
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)


class S3Integration:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_CONFIG['access_key'],
            aws_secret_access_key=AWS_CONFIG['secret_key'],
            region_name=AWS_CONFIG['region']
        )
        self.bucket_name = AWS_CONFIG['bucket']
    
    def upload_file(self, local_file, s3_key):
        try:
            logger.info(f"Uploading {local_file} to S3 bucket {self.bucket_name}")
            self.s3_client.upload_file(local_file, self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded to s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload file: {e}")
            return False
        except FileNotFoundError:
            logger.error(f"File not found: {local_file}")
            return False
    
    def download_file(self, s3_key, local_file):
        try:
            logger.info(f"Downloading s3://{self.bucket_name}/{s3_key} to {local_file}")
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            self.s3_client.download_file(self.bucket_name, s3_key, local_file)
            logger.info(f"Successfully downloaded to {local_file}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download file: {e}")
            return False
    
    def list_files(self, prefix=''):
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
                logger.info(f"Found {len(files)} files with prefix '{prefix}'")
                return files
            else:
                logger.info(f"No files found with prefix '{prefix}'")
                return []
        except ClientError as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    def backup_to_s3(self, local_dir, s3_prefix):
        logger.info(f"Starting backup of {local_dir} to S3...")
        success_count = 0
        fail_count = 0
        
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace('\\', '/')
                
                if self.upload_file(local_path, s3_key):
                    success_count += 1
                else:
                    fail_count += 1
        
        logger.info(f"Backup completed: {success_count} succeeded, {fail_count} failed")
        return success_count, fail_count


def main():
    s3 = S3Integration()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print("Downloading raw data from S3...")
    os.makedirs('data/raw', exist_ok=True)
    s3.download_file('raw/hotel_bookings_raw.csv', 'data/raw/hotel_bookings_raw.csv')
    
    cleaned_file = 'data/cleaned/hotel_bookings_cleaned.csv'
    if os.path.exists(cleaned_file):
        print(f"Backing up cleaned data to S3...")
        s3.upload_file(cleaned_file, f'cleaned/{timestamp}/hotel_bookings_cleaned.csv')
    
    log_dir = 'logs'
    if os.path.exists(log_dir):
        print(f"Backing up logs to S3...")
        s3.backup_to_s3(log_dir, f'logs/{timestamp}')
    
    logger.info("Files in S3 bucket:")
    all_files = s3.list_files()
    for file in all_files:
        logger.info(f"  - {file}")


if __name__ == "__main__":
    main()
