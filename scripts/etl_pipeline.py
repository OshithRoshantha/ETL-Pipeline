import pandas as pd
import psycopg2
from datetime import datetime
import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import DB_CONFIG

log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.propagate = False

file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)


class ETLPipeline:
    def __init__(self, raw_data_path, cleaned_data_path):
        self.raw_data_path = raw_data_path
        self.cleaned_data_path = cleaned_data_path
        self.rejected_records = []
        self.stats = {
            'total_records': 0,
            'duplicates_removed': 0,
            'missing_values_handled': 0,
            'invalid_records': 0,
            'final_records': 0
        }
    
    def extract(self):
        logger.info(f"Extracting data from {self.raw_data_path}")
        try:
            df = pd.read_csv(self.raw_data_path)
            self.stats['total_records'] = len(df)
            logger.info(f"Extracted {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise
    
    def transform(self, df):
        logger.info("Starting data transformation...")
        
        initial_count = len(df)
        df = df.drop_duplicates(subset=['booking_id'], keep='first')
        duplicates = initial_count - len(df)
        self.stats['duplicates_removed'] = duplicates
        logger.info(f"Removed {duplicates} duplicate records")
        
        df['category'] = df['category'].str.title()
        df['hotel_name'] = df['hotel_name'].str.strip()
        df['customer_email'] = df['customer_email'].str.lower()
        df['country'] = df['country'].str.upper()
        logger.info("Standardized text formats")
        
        df['created_date'] = df['created_date'].apply(self.parse_date)
        invalid_dates = df['created_date'].isna().sum()
        if invalid_dates > 0:
            logger.warning(f"Found {invalid_dates} records with invalid dates")
            self.stats['invalid_records'] += invalid_dates
        
        price_missing = df['price'].isna().sum()
        if price_missing > 0:
            logger.info(f"Filling {price_missing} missing price values with category median")
            df['price'] = df.groupby('category')['price'].transform(
                lambda x: x.fillna(x.median())
            )
            self.stats['missing_values_handled'] += price_missing
        
        rating_missing = df['rating'].isna().sum()
        if rating_missing > 0:
            logger.info(f"Filling {rating_missing} missing ratings with overall median")
            median_rating = df['rating'].median()
            df['rating'] = df['rating'].fillna(median_rating)
            self.stats['missing_values_handled'] += rating_missing
        
        rooms_missing = df['rooms_booked'].isna().sum()
        if rooms_missing > 0:
            logger.info(f"Filling {rooms_missing} missing rooms_booked with 1")
            df['rooms_booked'] = df['rooms_booked'].fillna(1).astype(int)
            self.stats['missing_values_handled'] += rooms_missing
        
        df = self.validate_data(df)
        
        df['revenue'] = df['price'] * df['rooms_booked']
        
        logger.info("Data transformation completed")
        return df
    
    def parse_date(self, date_str):
        if pd.isna(date_str):
            return None
        
        formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y.%m.%d', '%m/%d/%Y']
        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except:
                continue
        
        self.rejected_records.append({
            'value': date_str,
            'reason': 'Invalid date format',
            'timestamp': datetime.now()
        })
        return None
    
    def validate_data(self, df):
        initial_count = len(df)
        
        invalid_price = (df['price'] <= 0) | (df['price'] > 10000)
        if invalid_price.sum() > 0:
            logger.warning(f"Removing {invalid_price.sum()} records with invalid prices")
            rejected = df[invalid_price]
            for _, row in rejected.iterrows():
                self.rejected_records.append({
                    'booking_id': row['booking_id'],
                    'reason': f'Invalid price: {row["price"]}',
                    'timestamp': datetime.now()
                })
            df = df[~invalid_price]
        
        invalid_rating = (df['rating'] < 0) | (df['rating'] > 5)
        if invalid_rating.sum() > 0:
            logger.warning(f"Removing {invalid_rating.sum()} records with invalid ratings")
            df = df[~invalid_rating]
        
        df = df.dropna(subset=['created_date'])
        
        removed = initial_count - len(df)
        self.stats['invalid_records'] += removed
        
        return df
    
    def load_to_csv(self, df):
        logger.info(f"Saving cleaned data to {self.cleaned_data_path}")
        os.makedirs(os.path.dirname(self.cleaned_data_path), exist_ok=True)
        df.to_csv(self.cleaned_data_path, index=False)
        logger.info(f"Saved {len(df)} cleaned records")
        self.stats['final_records'] = len(df)
    
    def load_to_postgres(self, df):
        logger.info("Loading data to PostgreSQL...")
        
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            insert_count = 0
            for _, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO hotel_bookings 
                        (booking_id, hotel_name, category, price, rating, country, 
                         created_date, rooms_booked, customer_email, revenue)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (booking_id) DO NOTHING
                    """, (
                        int(row['booking_id']),
                        row['hotel_name'],
                        row['category'],
                        float(row['price']),
                        float(row['rating']),
                        row['country'],
                        row['created_date'],
                        int(row['rooms_booked']),
                        row['customer_email'],
                        float(row['revenue'])
                    ))
                    insert_count += 1
                except Exception as e:
                    logger.error(f"Error inserting record {row['booking_id']}: {e}")
                    self.rejected_records.append({
                        'booking_id': row['booking_id'],
                        'reason': f'DB insertion error: {str(e)}',
                        'timestamp': datetime.now()
                    })
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Successfully loaded {insert_count} records to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def save_rejected_records(self):
        if self.rejected_records:
            rejected_df = pd.DataFrame(self.rejected_records)
            rejected_path = f'logs/rejected_records_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            rejected_df.to_csv(rejected_path, index=False)
            logger.info(f"Saved {len(self.rejected_records)} rejected records to {rejected_path}")
    
    def print_stats(self):
        logger.info("="*50)
        logger.info("ETL Pipeline Execution Summary")
        logger.info("="*50)
        logger.info(f"Total records extracted: {self.stats['total_records']}")
        logger.info(f"Duplicates removed: {self.stats['duplicates_removed']}")
        logger.info(f"Missing values handled: {self.stats['missing_values_handled']}")
        logger.info(f"Invalid records rejected: {self.stats['invalid_records']}")
        logger.info(f"Rejected records logged: {len(self.rejected_records)}")
        logger.info(f"Final records loaded: {self.stats['final_records']}")
        logger.info("="*50)
    
    def run(self):
        logger.info("Starting ETL Pipeline...")
        start_time = datetime.now()
        
        try:
            df = self.extract()
            
            df = self.transform(df)
            
            self.load_to_csv(df)
            
            self.load_to_postgres(df)
            
            self.save_rejected_records()
            
            self.print_stats()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"ETL Pipeline completed in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    raw_path = 'data/raw/hotel_bookings_raw.csv'
    cleaned_path = 'data/cleaned/hotel_bookings_cleaned.csv'
    
    pipeline = ETLPipeline(raw_path, cleaned_path)
    pipeline.run()
