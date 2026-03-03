import psycopg2
from psycopg2 import sql
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import DB_CONFIG

def setup_database():
    print("Setting up PostgreSQL database...")
    
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DB_CONFIG['database'],))
        exists = cursor.fetchone()
        
        if not exists:
            print(f"Creating database {DB_CONFIG['database']}...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(DB_CONFIG['database'])
            ))
            print("Database created successfully!")
        else:
            print(f"Database {DB_CONFIG['database']} already exists.")
        
        cursor.close()
        conn.close()
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Creating schema...")
        
        schema_file = 'sql/schema.sql'
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        cursor.execute(schema_sql)
        conn.commit()
        
        print("Schema created successfully!")
        
        cursor.close()
        conn.close()
        
        print("\nDatabase setup complete!")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"Schema file not found: sql/schema.sql")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_database()
