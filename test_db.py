"""
test_db.py - Test PostgreSQL connection
"""
from sqlalchemy import create_engine, text

# Your connection string
DATABASE_URL = 'postgresql://user:yourpassword@localhost:****/airbnb_investment'

print("Testing PostgreSQL connection...")
print(f"Connection string: postgresql://user:****@localhost:****/airbnb_investment")

try:
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        version = result.fetchone()
        print("✓ SUCCESS! Connected to PostgreSQL")
        print(f"  Version: {version[0][:60]}...")
        
        # Check if database exists
        result = conn.execute(text("SELECT current_database()"))
        db_name = result.fetchone()
        print(f"  Database: {db_name[0]}")
        
except Exception as e:
    print(f"✗ Failed: {e}")
    print("\nPossible issues:")
    print("  - PostgreSQL not running")
    print("  - Wrong password")
    print("  - Database doesn't exist")
    print("  - Port **** is blocked")
