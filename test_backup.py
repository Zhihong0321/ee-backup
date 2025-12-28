import os
from dotenv import load_dotenv
from app.backup import perform_backup, init_db

# Load env vars
load_dotenv()

def test():
    print("--- Starting Connection Test ---")
    print(f"DATABASE_URL: {os.getenv('DATABASE_URL')}")
    # Ensure table exists
    init_db()
    
    print("Attempting to dump and upload to R2...")
    success, message = perform_backup()
    
    if success:
        print(f"SUCCESS: {message}")
    else:
        print(f"FAILED: {message}")

if __name__ == "__main__":
    test()
