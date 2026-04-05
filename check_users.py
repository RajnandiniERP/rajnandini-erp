from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    users = conn.execute(text("SELECT id, username, name, role, is_active FROM users")).fetchall()
    print("=== USERS IN DB ===")
    for u in users:
        print(f"  ID:{u[0]} | username:{u[1]} | name:{u[2]} | role:{u[3]} | active:{u[4]}")
    
    if not users:
        print("  NO USERS FOUND!")
