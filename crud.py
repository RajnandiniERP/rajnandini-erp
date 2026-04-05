from sqlalchemy.orm import Session
from sqlalchemy import func
import models, schemas
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_company(db):
    return db.query(models.Company).first()

def save_company(db, data):
    company = db.query(models.Company).first()
    if company:
        for k, v in data.dict().items():
            setattr(company, k, v)
    else:
        company = models.Company(**data.dict())
        db.add(company)
    db.commit()
    db.refresh(company)
    return company

def authenticate_user(db, username, password):
    user = db.query(models.User).filter(models.User.username == username).first()
    if not user or not pwd_context.verify(password, user.password_hash):
        return None
    return user

def get_users(db):
    return db.query(models.User).all()

def create_user(db, data):
    user = models.User(
        username=data.username,
        password_hash=pwd_context.hash(data.password),
        name=data.name, email=data.email,
        role=data.role, branch=data.branch
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def update_user(db, user_id, data):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        return None
    update_data = data.dict(exclude_unset=True)
    if "password" in update_data:
        update_data["password_hash"] = pwd_context.hash(update_data.pop("password"))
    for k, v in update_data.items():
        setattr(user, k, v)
    db.commit()
    return user

def delete_user(db, user_id):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if user:
        db.delete(user)
        db.commit()
    return {"message": "Deleted"}

def get_items(db):
    return db.query(models.Item).all()

def create_item(db, data):
    item = models.Item(**data.dict())
    db.add(item)
    db.commit()
    db.refresh(item)
    return item

def update_item(db, item_id, data):
    item = db.query(models.Item).filter(models.Item.id == item_id).first()
    if not item:
        return None
    for k, v in data.dict(exclude_unset=True).items():
        setattr(item, k, v)
    db.commit()
    return item

def delete_item(db, item_id):
    item = db.query(models.Item).filter(models.Item.id == item_id).first()
    if item:
        db.delete(item)
        db.commit()
    return {"message": "Deleted"}

def get_sales(db):
    return db.query(models.Sale).order_by(models.Sale.uploaded_at.desc()).limit(1000).all()

def bulk_insert_sales(db, rows, uploaded_by, file_name="", delete_first=True):
    # Delete existing rows for this file first — prevents duplicates on re-import
    if delete_first and file_name:
        db.execute(
            __import__('sqlalchemy').text("DELETE FROM sales WHERE file_name = :fn"),
            {"fn": file_name}
        )
        db.commit()

    inserted = skipped = 0
    for row in rows:
        try:
            sale = models.Sale(
                month=row.get("month"), year=row.get("year"),
                portal=row.get("portal"), branch=row.get("branch"),
                seller_gstin=row.get("seller_gstin"),
                invoice_number=row.get("invoice_number"),
                invoice_date=str(row.get("invoice_date", "")),
                type=row.get("type"),
                portal_transaction_type=row.get("portal_transaction_type"),
                order_id=row.get("order_id"),
                quantity=int(row.get("quantity") or 0),
                pid=row.get("pid"), sku=row.get("sku"),
                uniware_sku=row.get("uniware_sku"),
                category=row.get("category"), fulfill=row.get("fulfill"),
                ship_from_city=row.get("ship_from_city"),
                ship_from_state=row.get("ship_from_state"),
                ship_to_city=row.get("ship_to_city"),
                ship_to_state=row.get("ship_to_state"),
                invoice_amount=float(row.get("invoice_amount") or 0),
                tax_exclusive_gross=float(row.get("tax_exclusive_gross") or 0),
                uploaded_by=uploaded_by, file_name=file_name
            )
            db.add(sale)
            inserted += 1
        except Exception:
            skipped += 1
    db.commit()
    return {"inserted": inserted, "skipped": skipped, "total": len(rows)}

def get_sales_summary(db):
    by_type = db.query(models.Sale.type, func.count(models.Sale.id), func.sum(models.Sale.invoice_amount)).group_by(models.Sale.type).all()
    by_month = db.query(models.Sale.month, models.Sale.year, func.count(models.Sale.id), func.sum(models.Sale.invoice_amount)).group_by(models.Sale.month, models.Sale.year).all()
    return {
        "by_type": [{"type": r[0], "count": r[1], "amount": float(r[2] or 0)} for r in by_type],
        "by_month": [{"month": r[0], "year": r[1], "count": r[2], "amount": float(r[3] or 0)} for r in by_month],
    }

def sync_physical_returns(db: Session, rows: list, replace: bool = True):
    from sqlalchemy import text
    if replace:
        db.execute(text("DELETE FROM physical_returns"))
        db.commit()
    objects = [
        models.PhysicalReturn(
            date         = r.get("date", ""),
            channel      = r.get("channel", ""),
            order_no     = r.get("order_no", ""),
            awb          = r.get("awb", ""),
            courier      = r.get("courier", ""),
            putway       = r.get("putway", ""),
            sku_r        = r.get("sku_r", ""),
            to_rma       = r.get("to_rma", ""),
            remark       = r.get("remark", ""),
            putaway_code = r.get("putaway_code", ""),
        )
        for r in rows
    ]
    db.bulk_save_objects(objects)
    db.commit()
    return {"saved": len(rows)}

def get_physical_returns(db: Session):
    return db.query(models.PhysicalReturn).all()
