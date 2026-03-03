import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

from scripts.etl_pipeline import ETLPipeline
from scripts.s3_integration import S3Integration


def main():
    print("\n[STEP 1] Downloading raw data from S3...")
    try:
        s3 = S3Integration()
        os.makedirs('data/raw', exist_ok=True)
        s3.download_file('raw/hotel_bookings_raw.csv', 'data/raw/hotel_bookings_raw.csv')
    except Exception as e:
        print(f"ERROR: Failed to download raw data from S3: {e}")
        print("Make sure raw data is manually uploaded to S3 first!")
        return
    
    print("\n[STEP 2] Running ETL Pipeline...")
    try:
        pipeline = ETLPipeline(
            raw_data_path='data/raw/hotel_bookings_raw.csv',
            cleaned_data_path='data/cleaned/hotel_bookings_cleaned.csv'
        )
        pipeline.run()
    except Exception as e:
        print(f"ERROR: ETL pipeline failed: {e}")
        return
    
    print("\n[STEP 3] Backing up cleaned data to S3...")
    try:
        s3.upload_file('data/cleaned/hotel_bookings_cleaned.csv', 
                      'cleaned/hotel_bookings_cleaned.csv')
        s3.backup_to_s3('logs', 'logs')
    except Exception as e:
        print(f"WARNING: S3 backup skipped or failed: {e}")
    
    print("\n" + "="*60)
    print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("="*60)


if __name__ == "__main__":
    main()
