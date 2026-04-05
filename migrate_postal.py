from database import engine
from sqlalchemy import text

with engine.connect() as conn:
    stmts = [
        "ALTER TABLE portal_sales ADD COLUMN ship_from_postal TEXT",
        "ALTER TABLE portal_sales ADD COLUMN ship_to_postal TEXT",
    ]
    for s in stmts:
        try:
            conn.execute(text(s))
            print(f"OK: {s}")
        except Exception as e:
            print(f"SKIP: {e}")
    conn.commit()
    print("Done!")
