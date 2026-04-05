# ═══════════════════════════════════════════════════════════════════
#  PHYSICAL RETURN — BACKEND PATCH
#  Add these code blocks to your existing files
# ═══════════════════════════════════════════════════════════════════


# ── 1. models.py — Add this class ────────────────────────────────
"""
class PhysicalReturn(Base):
    __tablename__ = "physical_returns"

    id           = Column(Integer, primary_key=True, index=True)
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
"""


# ── 2. schemas.py — Add these classes ────────────────────────────
"""
class PhysicalReturnRow(BaseModel):
    date:         Optional[str] = ""
    channel:      Optional[str] = ""
    order_no:     Optional[str] = ""
    awb:          Optional[str] = ""
    courier:      Optional[str] = ""
    putway:       Optional[str] = ""
    sku_r:        Optional[str] = ""
    to_rma:       Optional[str] = ""
    remark:       Optional[str] = ""
    putaway_code: Optional[str] = ""

class PhysicalReturnSync(BaseModel):
    rows: List[PhysicalReturnRow]
"""


# ── 3. crud.py — Add these functions ─────────────────────────────
"""
def sync_physical_returns(db: Session, rows: list):
    # Delete all existing rows and replace with new data
    db.execute(text("DELETE FROM physical_returns"))
    for r in rows:
        obj = models.PhysicalReturn(
            date         = r.get("date",""),
            channel      = r.get("channel",""),
            order_no     = r.get("order_no",""),
            awb          = r.get("awb",""),
            courier      = r.get("courier",""),
            putway       = r.get("putway",""),
            sku_r        = r.get("sku_r",""),
            to_rma       = r.get("to_rma",""),
            remark       = r.get("remark",""),
            putaway_code = r.get("putaway_code",""),
        )
        db.add(obj)
    db.commit()
    return {"saved": len(rows)}

def get_physical_returns(db: Session):
    return db.query(models.PhysicalReturn).all()
"""


# ── 4. main.py — Add these 2 API endpoints ───────────────────────
"""
@app.post("/api/physical-returns/sync")
def sync_physical_returns(
    data: schemas.PhysicalReturnSync,
    db: Session = Depends(get_db),
    current_user=Depends(auth.get_current_user)
):
    rows = [r.dict() for r in data.rows]
    return crud.sync_physical_returns(db, rows)


@app.get("/api/physical-returns")
def get_physical_returns(
    db: Session = Depends(get_db),
    current_user=Depends(auth.get_current_user)
):
    rows = crud.get_physical_returns(db)
    return [
        {
            "date":         r.date,
            "channel":      r.channel,
            "order_no":     r.order_no,
            "awb":          r.awb,
            "courier":      r.courier,
            "putway":       r.putway,
            "sku_r":        r.sku_r,
            "to_rma":       r.to_rma,
            "remark":       r.remark,
            "putaway_code": r.putaway_code,
        }
        for r in rows
    ]
"""
