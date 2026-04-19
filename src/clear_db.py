import sqlite3
import os

from pathlib import Path

db_path = Path("/Users/konstantinos/Projects/job-tracker/results/jobs_seen.sqlite3")

if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # Drop all tables
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table[0]};")
    
    conn.commit()
    conn.close()
    print("Database cleared successfully")
else:
    print(f"Database file not found at {db_path}")