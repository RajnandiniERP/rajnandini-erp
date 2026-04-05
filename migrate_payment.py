from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    stmts = [
        """CREATE TABLE IF NOT EXISTS payment_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            row_count INTEGER DEFAULT 0,
            detected_at DATETIME,
            loaded_at DATETIME
        )""",
        """CREATE TABLE IF NOT EXISTS payment_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT,
            file_name TEXT,
            date_time TEXT,
            month TEXT,
            year INTEGER,
            settlement_id TEXT,
            type TEXT,
            order_id TEXT,
            sku TEXT,
            description TEXT,
            quantity TEXT,
            marketplace TEXT,
            fulfillment TEXT,
            order_city TEXT,
            order_state TEXT,
            product_sales REAL DEFAULT 0,
            shipping_credits REAL DEFAULT 0,
            promotional_rebates REAL DEFAULT 0,
            selling_fees REAL DEFAULT 0,
            fba_fees REAL DEFAULT 0,
            other_transaction_fees REAL DEFAULT 0,
            other REAL DEFAULT 0,
            total REAL DEFAULT 0
        )"""
    ]
    for s in stmts:
        try:
            conn.execute(text(s))
            print(f"OK: {s[:60]}...")
        except Exception as e:
            print(f"SKIP: {e}")
    conn.commit()
    print("\nDone! Portal Payment tables ready.")
