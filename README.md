# Hotel Booking ETL Pipeline

ETL pipeline built with Python, PostgreSQL and AWS S3 for processing hotel booking data. Handles messy real-world data with duplicates, missing values, and format inconsistencies.

## What it does

Downloads raw data from S3, cleans it up, loads into postgres database, backs everything up to S3. The dataset has about 12k records with intentional quality issues (missing values, weird date formats, duplicates etc) to demonstrate data cleaning.

## Quick Start

Install requirements:
```bash
pip install -r requirements.txt
```

Setup your .env file with database and AWS credentials:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db
DB_USER=your_user
DB_PASSWORD=your_password

AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
S3_BUCKET_NAME=your_bucket
```

Upload raw CSV to S3 bucket under /raw folder first. Then run:
```bash
python run_pipeline.py
```

It'll download from S3, clean the data, load to postgres, and backup cleaned files to S3.

## Pipeline Workflow

**Extract**: Gets CSV from S3, loads into pandas dataframe

**Transform**: 
- Removes duplicates based on booking_id
- Fills missing prices using category median (different categories have different price ranges)
- Fills missing ratings with overall median
- Standardizes text formatting (proper case for categories, lowercase emails)
- Handles 3 different date formats I found in the data
- Validates price and rating ranges
- Calculates revenue column

**Load**: Saves cleaned CSV locally and inserts into postgres. Uses ON CONFLICT DO NOTHING so you can rerun without duplicates.

## Database

Built the schema with proper constraints and indexes. Check sql/schema.sql for details.

Indexes I added:
- idx_country, idx_category, idx_rating, idx_created_date for common filters
- idx_country_category and idx_date_category for combined queries

The indexes make a huge difference - tested with EXPLAIN ANALYZE and queries went from 150ms (seq scan) to 8ms (index scan). About 95% faster.

## S3 Setup

Created IAM user and attached AmazonS3FullAccess policy for S3 access.

Bucket structure:
```
/raw - source data uploaded here manually
/cleaned - transformed data backed up here
/logs - execution logs backed up here
```

## Queries

Check out sql/queries.sql for sample queries. Includes stuff like:
- Top categories by revenue
- Monthly trends
- Country analysis with ratings
- Window functions for ranking
- High value bookings

Run them with: `psql -d your_db -f sql/queries.sql`

## Scaling thoughts

For handling way more data (1M+ records):

Database side - would add partitioning by date (monthly partitions probably). Also materialized views for common aggregations. And pgBouncer for connection pooling.

ETL side - process in chunks instead of loading everything at once. Could parallelize with multiprocessing for independent transforms. Incremental loads where we only process new data.

For scheduling, simple cron job works fine for now:
```bash
0 2 * * * python /path/to/run_pipeline.py >> /logs/cron.log 2>&1
```

Could move to Airflow later if we need retry logic, monitoring, complex dependencies between tasks etc.

Would also add retry logic with exponential backoff, circuit breaker pattern if DB is down, data quality checks (assert record counts, check nulls, validate ranges).

For really massive scale might switch to full data lake setup - keep everything in S3 as parquet files, use Athena to query directly, only load aggregated results to postgres.

## Performance

Running on about 12k records:
- ETL takes 5-8 seconds
- DB load is 3-4 seconds  
- S3 upload depends on network but usually 2-3 seconds

## Files

- run_pipeline.py - main script that orchestrates everything
- scripts/etl_pipeline.py - core ETL logic
- scripts/s3_integration.py - S3 upload/download
- scripts/setup_database.py - initializes DB schema
- scripts/performance_demo.py - shows index performance
- data_quality_analysis.ipynb - jupyter notebook analyzing data issues
- sql/schema.sql - table definition with indexes
- sql/queries.sql - analytical queries
- sql/performance_analysis.sql - detailed performance tests

## Troubleshooting

If postgres connection fails check DB_PORT in .env (mine's on 5433 not 5432 because Docker).

If S3 upload fails verify AWS credentials with `aws s3 ls` and check IAM permissions.

If modules missing just `pip install -r requirements.txt` again.
