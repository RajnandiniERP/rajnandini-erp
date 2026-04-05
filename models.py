from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.sql import func
from database import Base

class Company(Base):
    __tablename__ = "company"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    gstin = Column(String)
    address = Column(Text)
    city = Column(String)
    state = Column(String)
    phone = Column(String)
    email = Column(String)
    created_at = Column(DateTime, default=func.now())

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    name = Column(String, nullable=False)
    email = Column(String)
    role = Column(String, default="viewer")
    branch = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())

class Item(Base):
    __tablename__ = "items"
    id           = Column(Integer, primary_key=True)
    sku          = Column(String, unique=True, nullable=False)  # Uniware SKU
    category     = Column(String)
    style_id     = Column(String)
    availability = Column(String)
    uniware_stock= Column(Integer, default=0)
    fba_stock    = Column(Integer, default=0)
    sjit_stock   = Column(Integer, default=0)
    fbf_stock    = Column(Integer, default=0)
    location     = Column(String)
    remark       = Column(String)
    cost_price   = Column(Float, default=0)   # ST CS
    mrp          = Column(Float, default=0)   # Wholesale Price
    catalog_name = Column(String)
    synced_at    = Column(DateTime, default=func.now())

class SkuMapping(Base):
    __tablename__ = "sku_mappings"
    id = Column(Integer, primary_key=True)
    portal_sku = Column(String, nullable=False)
    portal = Column(String, nullable=False)
    item_master_sku = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class PortalFile(Base):
    __tablename__ = "portal_files"
    id            = Column(Integer, primary_key=True)
    portal        = Column(String, nullable=False)   # Amazon, Flipkart etc
    file_name     = Column(String, nullable=False)
    file_path     = Column(String, nullable=False)
    report_type   = Column(String)                   # B2B, B2C
    month         = Column(String)
    year          = Column(Integer)
    status        = Column(String, default="pending") # pending, loaded, ignored
    row_count     = Column(Integer, default=0)
    detected_at   = Column(DateTime, default=func.now())
    loaded_at     = Column(DateTime)

class TransactionTypeMapping(Base):
    __tablename__ = "transaction_type_mappings"
    id            = Column(Integer, primary_key=True)
    portal        = Column(String, nullable=False)
    transaction_type = Column(String, nullable=False)
    mapped_to     = Column(String)  # Sale, Return, Cancel, FreeReplacement, Ignore
    updated_at    = Column(DateTime, default=func.now())

class PortalSale(Base):
    __tablename__ = "portal_sales"
    id                   = Column(Integer, primary_key=True)
    portal               = Column(String)
    report_type          = Column(String)            # B2B / B2C
    file_name            = Column(String)
    seller_gstin         = Column(String)
    invoice_number       = Column(String)
    invoice_date         = Column(String)
    transaction_type     = Column(String)            # raw from file
    mapped_type          = Column(String)            # Sale/Return/etc after mapping
    order_id             = Column(String)
    shipment_id          = Column(String)
    quantity             = Column(Integer, default=0)
    asin                 = Column(String)
    sku                  = Column(String)            # portal SKU
    uniware_sku          = Column(String)            # mapped item master SKU
    item_description     = Column(String)
    fulfillment_channel  = Column(String)
    ship_from_city       = Column(String)
    ship_from_state      = Column(String)
    ship_from_postal     = Column(String)
    ship_to_city         = Column(String)
    ship_to_state        = Column(String)
    ship_to_postal       = Column(String)
    invoice_amount       = Column(Float, default=0)
    tax_exclusive_gross  = Column(Float, default=0)
    month                = Column(String)
    year                 = Column(Integer)
    loaded_at            = Column(DateTime, default=func.now())

class PhysicalReturn(Base):
    __tablename__ = "physical_returns"
    id           = Column(Integer, primary_key=True)
    date         = Column(String, nullable=True)
    channel      = Column(String, nullable=True, index=True)
    order_no     = Column(String, nullable=True)
    awb          = Column(String, nullable=True)
    courier      = Column(String, nullable=True)
    putway       = Column(String, nullable=True)
    sku_r        = Column(String, nullable=True)
    to_rma       = Column(String, nullable=True)
    remark       = Column(String, nullable=True)
    putaway_code = Column(String, nullable=True)
    synced_at    = Column(DateTime, default=func.now())

class Sale(Base):
    __tablename__ = "sales"
    id = Column(Integer, primary_key=True)
    month = Column(String)
    year = Column(Integer)
    portal = Column(String)
    branch = Column(String)
    seller_gstin = Column(String)
    invoice_number = Column(String)
    invoice_date = Column(String)
    type = Column(String)
    portal_transaction_type = Column(String)
    order_id = Column(String)
    quantity = Column(Integer, default=0)
    pid = Column(String)
    sku = Column(String)
    uniware_sku = Column(String)
    category = Column(String)
    fulfill = Column(String)
    ship_from_city = Column(String)
    ship_from_state = Column(String)
    ship_to_city = Column(String)
    ship_to_state = Column(String)
    invoice_amount = Column(Float, default=0)
    tax_exclusive_gross = Column(Float, default=0)
    uploaded_by = Column(String)
    uploaded_at = Column(DateTime, default=func.now())
    file_name = Column(String)
