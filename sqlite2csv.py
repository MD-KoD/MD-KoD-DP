import sqlite3
from typing import Tuple
import pandas as pd
import os

class DatabaseConnection:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def __enter__(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        return self.conn, self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close() 

def get_table_df(table_name: str, db_name: str = 'jeju.db'):
    with DatabaseConnection(db_name) as (conn, cursor):
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df

def save_table_to_csv(table_name: str, db_name: str = 'jeju.db', output_dir: str = './jeju_csv'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    df = get_table_df(table_name, db_name)
    
    csv_filename = f"{table_name.lower()}.csv"
    csv_path = os.path.join(output_dir, csv_filename)
    
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    print(f"{table_name} table -> {csv_path}")
    print(f"len(df): {len(df)}")
    print(f"len(df.columns): {len(df.columns)}")
    print(f"df.columns: {list(df.columns)}")
    print()
    
    return df

if __name__ == "__main__":
    print(__name__)
    
    save_table_to_csv('Information')
    save_table_to_csv('Review')
    
    print("good")