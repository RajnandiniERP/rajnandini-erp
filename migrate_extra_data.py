from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    stmts = [
        """CREATE TABLE IF NOT EXISTS extra_data_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT, file_name TEXT, file_path TEXT,
            report_type TEXT, status TEXT DEFAULT 'pending',
            row_count INTEGER DEFAULT 0, detected_at DATETIME, loaded_at DATETIME
        )""",
        """CREATE TABLE IF NOT EXISTS ed_fba_returns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT, file_name TEXT, return_date TEXT, order_id TEXT,
            sku TEXT, asin TEXT, fnsku TEXT, product_name TEXT, quantity TEXT,
            fulfillment_center_id TEXT, detailed_disposition TEXT, reason TEXT, customer_comments TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS ed_storage_fees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT, file_name TEXT, asin TEXT, fnsku TEXT, product_name TEXT,
            fulfillment_center TEXT, month_of_charge TEXT, average_quantity_on_hand REAL,
            item_volume REAL, storage_rate REAL, estimated_monthly_storage_fee REAL
        )""",
        """CREATE TABLE IF NOT EXISTS ed_order_reimb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT, file_name TEXT, order_id TEXT, asin TEXT, product_title TEXT,
            msku TEXT, fnsku TEXT, shipped_quantity TEXT, refund_date TEXT,
            customer_return_reason TEXT, return_item_condition TEXT,
            reimbursement_reason TEXT, reimbursement_status TEXT, reimbursed_amount_per_unit REAL
        )""",
        """CREATE TABLE IF NOT EXISTS ed_reimbursements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT, file_name TEXT, approval_date TEXT, reimbursement_id TEXT,
            case_id TEXT, amazon_order_id TEXT, reason TEXT, sku TEXT, fnsku TEXT,
            asin TEXT, product_name TEXT, condition TEXT, currency TEXT,
            amount_per_unit REAL, amount_total REAL, quantity_reimbursed_cash REAL,
            quantity_reimbursed_inventory REAL, quantity_reimbursed_total REAL
        )""",
        """CREATE TABLE IF NOT EXISTS ed_replacements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT, file_name TEXT, shipment_date TEXT, sku TEXT, asin TEXT,
            fulfillment_center_id TEXT, original_fulfillment_center_id TEXT,
            quantity REAL, replacement_reason_code TEXT,
            replacement_amazon_order_id TEXT, original_amazon_order_id TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS ed_aged_inv (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portal TEXT, file_name TEXT, asin TEXT, sku TEXT, product_name TEXT,
            fulfillment_center TEXT, quantity REAL, age_days REAL,
            surcharge_per_unit REAL, total_surcharge REAL
        )""",
    ]
    for s in stmts:
        try:
            conn.execute(text(s))
            print(f"OK: {s[:55]}...")
        except Exception as e:
            print(f"SKIP: {e}")
    conn.commit()
    print("\nDone! All Portal Extra Data tables ready.")
