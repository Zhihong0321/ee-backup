import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def extract_schema():
    conn = get_db_connection()
    cur = conn.cursor()

    # Get all tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = [row[0] for row in cur.fetchall()]

    schema_data = {}

    for table in tables:
        # Get columns
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
        """, (table,))
        columns = []
        for col in cur.fetchall():
            col_name = col[0]
            data_type = col[1]
            nullable = col[2] == 'YES'
            
            # Auto-description
            desc = ""
            if col_name == 'id':
                desc = "Primary unique identifier."
            elif 'created_at' in col_name:
                desc = "Timestamp when the record was created."
            elif 'updated_at' in col_name:
                desc = "Timestamp when the record was last updated."
            elif col_name.endswith('_id'):
                desc = f"Foreign key reference to {col_name[:-3]}."
            
            columns.append({
                "name": col_name,
                "type": data_type,
                "nullable": nullable,
                "description": desc
            })

        # Get Foreign Keys
        cur.execute("""
            SELECT
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.key_column_usage AS kcu
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = kcu.constraint_name
                JOIN information_schema.table_constraints AS tc
                ON tc.constraint_name = kcu.constraint_name
            WHERE kcu.table_name = %s AND tc.constraint_type = 'FOREIGN KEY'
        """, (table,))
        
        fks = []
        for fk in cur.fetchall():
            fks.append({
                "column": fk[0],
                "references_table": fk[1],
                "references_column": fk[2]
            })

        schema_data[table] = {
            "description": "",  # Placeholder for table description
            "columns": columns,
            "foreign_keys": fks
        }

    conn.close()
    
    with open("schema_metadata.json", "w") as f:
        json.dump(schema_data, f, indent=4)
    
    print("Schema extracted to schema_metadata.json")

if __name__ == "__main__":
    extract_schema()
