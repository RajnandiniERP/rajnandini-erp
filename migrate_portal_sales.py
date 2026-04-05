from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    stmts = [
        """CREATE TABLE IF NOT EXISTS portal_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            report_type TEXT,
            month TEXT,
            year INTEGER,
            status TEXT DEFAULT 'pending',
            row_count INTEGER DEFAULT 0,
            detected_at DATETIME,
            loaded_at DATETIME
        )""",
        """CREATE TABLE IF NOT EXISTS transaction_type_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            mapped_to TEXT,
            updated_at DATETIME
        )""",
        """CREATE TABLE IF NOT EXISTS portal_sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT,
            report_type TEXT,
            file_name TEXT,
            seller_gstin TEXT,
            invoice_number TEXT,
            invoice_date TEXT,
            transaction_type TEXT,
            mapped_type TEXT,
            order_id TEXT,
            shipment_id TEXT,
            quantity INTEGER DEFAULT 0,
            asin TEXT,
            sku TEXT,
            uniware_sku TEXT,
            item_description TEXT,
            fulfillment_channel TEXT,
            ship_from_city TEXT,
            ship_from_state TEXT,
            ship_to_city TEXT,
            ship_to_state TEXT,
            invoice_amount REAL DEFAULT 0,
            tax_exclusive_gross REAL DEFAULT 0,
            month TEXT,
            year INTEGER,
            loaded_at DATETIME
        )"""
    ]
    for s in stmts:
        try:
            conn.execute(text(s))
            print(f"OK: {s[:50]}...")
        except Exception as e:
            print(f"SKIP: {e}")
    conn.commit()
    print("\nDone! Portal Sales tables ready.")
