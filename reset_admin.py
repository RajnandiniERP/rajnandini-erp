from database import engine, SessionLocal
from sqlalchemy import text
from passlib.context import CryptContext

pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

db = SessionLocal()
try:
    # Check if admin exists
    user = db.execute(text("SELECT id, username FROM users WHERE username='admin'")).fetchone()
    if user:
        # Update password
        hashed = pwd.hash("Admin@123")
        db.execute(text("UPDATE users SET password_hash=:h WHERE username='admin'"), {"h": hashed})
        db.commit()
        print("✅ Admin password reset to: Admin@123")
    else:
        print("❌ Admin user not found — creating...")
        hashed = pwd.hash("Admin@123")
        db.execute(text("""
            INSERT INTO users (username, password_hash, name, role, is_active)
            VALUES ('admin', :h, 'Administrator', 'admin', 1)
        """), {"h": hashed})
        db.commit()
        print("✅ Admin user created! Username: admin | Password: Admin@123")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
