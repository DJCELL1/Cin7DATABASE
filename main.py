import os
import time
import base64
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Depends, Header
from pydantic import BaseModel
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Text,
    Index,
)
from sqlalchemy.orm import sessionmaker, Session, declarative_base

# ---------------------------------------------------------
# ENV + LOGGING
# ---------------------------------------------------------
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cin7_cache_api")

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def normalize_sku(s: str) -> str:
    return (s or "").strip().upper()

def parse_cin7_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        cleaned = dt_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# ---------------------------------------------------------
# DB SETUP
# ---------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/cin7_products")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------------------------------------------------
# MODELS
# ---------------------------------------------------------
class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    cin7_id = Column(String, index=True)
    sku = Column(String, unique=True, index=True)
    name = Column(String, index=True)
    description = Column(Text)
    price = Column(Float)
    stock_on_hand = Column(Integer)
    category = Column(String)
    last_modified = Column(DateTime(timezone=True))
    synced_at = Column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
        Index("ix_products_sku", "sku"),
        Index("ix_products_name", "name"),
        Index("ix_products_cin7_id", "cin7_id"),
    )

class SyncWatermark(Base):
    __tablename__ = "sync_watermark"

    id = Column(Integer, primary_key=True)
    last_checked = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utcnow)

class SupplierMap(Base):
    __tablename__ = "supplier_map"

    id = Column(Integer, primary_key=True)
    supplier_name = Column(String, unique=True, index=True, nullable=False)
    cin7_contact_id = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index("ix_supplier_map_supplier_name", "supplier_name"),
    )

Base.metadata.create_all(bind=engine)

# ---------------------------------------------------------
# FASTAPI APP
# ---------------------------------------------------------
app = FastAPI(
    title="Cin7 Product Cache API + Supplier Map",
    version="3.2.0",
)

# ---------------------------------------------------------
# INTERNAL AUTH (optional)
# ---------------------------------------------------------
def require_internal_key(x_api_key: Optional[str] = Header(default=None)):
    expected = os.getenv("INTERNAL_API_KEY")
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ---------------------------------------------------------
# CIN7 CLIENT
# ---------------------------------------------------------
CIN7_API_USERNAME = os.getenv("CIN7_API_USERNAME")
CIN7_API_KEY = os.getenv("CIN7_API_KEY")
CIN7_BASE_URL = os.getenv("CIN7_BASE_URL", "https://api.cin7.com/api/v1").rstrip("/")

class Cin7Client:
    def __init__(self):
        creds = f"{CIN7_API_USERNAME}:{CIN7_API_KEY}"
        encoded = base64.b64encode(creds.encode()).decode()

        self.headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
        }
        self.base_url = CIN7_BASE_URL

    def get_products(self, modified_since: Optional[datetime], page: int, page_size: int):
        params = {"page": page, "rows": page_size}
        if modified_since:
            ms = modified_since.astimezone(timezone.utc).replace(tzinfo=None)
            params["modifiedSince"] = ms.strftime("%Y-%m-%dT%H:%M:%S")

        r = requests.get(f"{self.base_url}/Products", headers=self.headers, params=params, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []

    def get_all_products(self, modified_since: Optional[datetime]):
        out = []
        page = 1
        while True:
            batch = self.get_products(modified_since, page, 250)
            if not batch:
                break
            out.extend(batch)
            if len(batch) < 250:
                break
            page += 1
            time.sleep(0.4)
        return out

cin7_client = Cin7Client()

# ---------------------------------------------------------
# WATERMARK HELPERS
# ---------------------------------------------------------
def get_watermark(db: Session):
    return db.query(SyncWatermark).first()

def set_watermark(db: Session, ts: datetime):
    wm = get_watermark(db)
    if not wm:
        wm = SyncWatermark(last_checked=ts, updated_at=utcnow())
        db.add(wm)
    else:
        wm.last_checked = ts
        wm.updated_at = utcnow()
    return wm

# ---------------------------------------------------------
# SYNC LOGIC (ADD-ONLY)
# ---------------------------------------------------------
def sync_new_products_add_only(db: Session, modified_since: Optional[datetime]):
    payload = cin7_client.get_all_products(modified_since)
    inserted = skipped = scanned = 0

    for product_data in payload:
        scanned += 1
        cin7_id = str(product_data.get("id") or "")
        base_name = product_data.get("name") or ""
        description = product_data.get("description") or ""
        category = product_data.get("category") or ""
        parent_mod = parse_cin7_dt(product_data.get("modifiedDate") or product_data.get("createdDate"))
        options = product_data.get("productOptions") or []

        if options:
            for opt in options:
                sku = normalize_sku(opt.get("code"))
                if not sku:
                    continue
                if db.query(Product.id).filter(Product.sku == sku).first():
                    skipped += 1
                    continue
                p = Product(
                    cin7_id=cin7_id,
                    sku=sku,
                    name=f"{base_name} {opt.get('option1','')}".strip(),
                    description=description,
                    price=float(opt.get("retailPrice", 0) or 0),
                    stock_on_hand=int(opt.get("stockOnHand", 0) or 0),
                    category=category,
                    last_modified=parse_cin7_dt(opt.get("modifiedDate")) or parent_mod,
                    synced_at=utcnow(),
                )
                db.add(p)
                inserted += 1
        else:
            sku = normalize_sku(product_data.get("styleCode"))
            if not sku:
                continue
            if db.query(Product.id).filter(Product.sku == sku).first():
                skipped += 1
                continue
            db.add(Product(
                cin7_id=cin7_id,
                sku=sku,
                name=base_name,
                description=description,
                price=0,
                stock_on_hand=0,
                category=category,
                last_modified=parent_mod,
                synced_at=utcnow(),
            ))
            inserted += 1

    db.commit()
    return {"inserted": inserted, "skipped": skipped, "scanned": scanned}

# ---------------------------------------------------------
# SUPPLIER MAP ENDPOINTS
# ---------------------------------------------------------
class SupplierMapIn(BaseModel):
    supplier_name: str
    cin7_contact_id: int

@app.get("/supplier-map/get")
def supplier_map_get(name: str, db: Session = Depends(get_db)):
    row = db.query(SupplierMap).filter(SupplierMap.supplier_name.ilike(name.strip())).first()
    if not row:
        return {"found": False}
    return {"found": True, "supplier_name": row.supplier_name, "cin7_contact_id": row.cin7_contact_id}

@app.post("/supplier-map/set")
def supplier_map_set(payload: SupplierMapIn, db: Session = Depends(get_db)):
    name = payload.supplier_name.strip()
    row = db.query(SupplierMap).filter(SupplierMap.supplier_name.ilike(name)).first()
    if row:
        row.cin7_contact_id = payload.cin7_contact_id
        row.updated_at = utcnow()
        db.commit()
        return {"status": "updated"}
    db.add(SupplierMap(
        supplier_name=name,
        cin7_contact_id=payload.cin7_contact_id,
    ))
    db.commit()
    return {"status": "created"}

@app.get("/supplier-map/list")
def supplier_map_list(db: Session = Depends(get_db)):
    rows = db.query(SupplierMap).order_by(SupplierMap.supplier_name.asc()).all()
    return [{"supplier_name": r.supplier_name, "cin7_contact_id": r.cin7_contact_id} for r in rows]

# ---------------------------------------------------------
# EXISTING PRODUCT ENDPOINTS (unchanged)
# ---------------------------------------------------------
@app.get("/products/by-sku")
def get_product_by_sku(sku: str, db: Session = Depends(get_db)):
    sku = normalize_sku(sku)
    p = db.query(Product).filter(Product.sku == sku).first()
    if not p:
        raise HTTPException(status_code=404, detail="Not found")
    return p.__dict__

@app.post("/sync")
def trigger_sync(full: bool = False, db: Session = Depends(get_db), _: None = Depends(require_internal_key)):
    wm = get_watermark(db)
    modified_since = None if full else (wm.last_checked if wm else None)
    stats = sync_new_products_add_only(db, modified_since)
    set_watermark(db, utcnow())
    db.commit()
    return {"status": "ok", **stats}

# ---------------------------------------------------------
# LOCAL RUN
# ---------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
