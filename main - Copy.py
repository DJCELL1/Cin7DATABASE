import os
import time
import base64
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Depends, Header
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Index
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
    # Keep slash. Just normalize case/whitespace.
    return (s or "").strip().upper()

def parse_cin7_dt(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Cin7 often returns ISO strings with 'Z'. Convert to tz-aware UTC safely.
    """
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
    sku = Column(String, unique=True, index=True)      # cache key
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
    """
    One-row table:
    last_checked = last time we asked Cin7 for changes.
    """
    __tablename__ = "sync_watermark"

    id = Column(Integer, primary_key=True)
    last_checked = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utcnow)

Base.metadata.create_all(bind=engine)

# ---------------------------------------------------------
# FASTAPI APP
# ---------------------------------------------------------
app = FastAPI(title="Cin7 Product Cache API (Manual + Add-Only)", version="3.1.0")

# ---------------------------------------------------------
# OPTIONAL: PROTECT /sync WITH HEADER (recommended)
# Set INTERNAL_API_KEY in Railway Variables to enable.
# If unset, /sync works without auth (because chaos is fun).
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
        if not CIN7_API_USERNAME or not CIN7_API_KEY:
            logger.warning("Cin7 credentials missing (CIN7_API_USERNAME / CIN7_API_KEY).")

        creds = f"{CIN7_API_USERNAME}:{CIN7_API_KEY}"
        encoded = base64.b64encode(creds.encode()).decode()

        self.headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
        }
        self.base_url = CIN7_BASE_URL

    def get_products(
        self,
        modified_since: Optional[datetime],
        page: int = 1,
        page_size: int = 250,
    ) -> List[Dict[str, Any]]:
        params = {"page": page, "rows": page_size}

        if modified_since:
            # Cin7 expects naive timestamp string; send UTC naive
            ms = modified_since.astimezone(timezone.utc).replace(tzinfo=None)
            params["modifiedSince"] = ms.strftime("%Y-%m-%dT%H:%M:%S")

        try:
            r = requests.get(
                f"{self.base_url}/Products",
                headers=self.headers,
                params=params,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching products from Cin7: {e}")
            return []

    def get_all_products(self, modified_since: Optional[datetime]) -> List[Dict[str, Any]]:
        all_products: List[Dict[str, Any]] = []
        page = 1
        page_size = 250

        while True:
            logger.info(
                f"Cin7 fetch page={page} modified_since={modified_since.isoformat() if modified_since else 'NONE'}"
            )
            batch = self.get_products(modified_since=modified_since, page=page, page_size=page_size)
            if not batch:
                break

            all_products.extend(batch)

            if len(batch) < page_size:
                break

            page += 1
            time.sleep(0.4)  # basic rate limiting

        logger.info(f"Cin7 returned {len(all_products)} products for this sync window.")
        return all_products

cin7_client = Cin7Client()

# ---------------------------------------------------------
# WATERMARK HELPERS
# ---------------------------------------------------------
def get_watermark(db: Session) -> Optional[SyncWatermark]:
    return db.query(SyncWatermark).order_by(SyncWatermark.id.asc()).first()

def set_watermark(db: Session, ts: datetime) -> SyncWatermark:
    wm = get_watermark(db)
    if not wm:
        wm = SyncWatermark(last_checked=ts, updated_at=utcnow())
        db.add(wm)
    else:
        wm.last_checked = ts
        wm.updated_at = utcnow()
    return wm

# ---------------------------------------------------------
# SYNC LOGIC (MANUAL + ADD-ONLY)
# ---------------------------------------------------------
def sync_new_products_add_only(db: Session, modified_since: Optional[datetime]) -> Dict[str, int]:
    """
    Fetch from Cin7 since watermark, then INSERT ONLY SKUs not already in DB.
    Does NOT update existing products. (You asked for cache behavior.)
    """
    payload = cin7_client.get_all_products(modified_since=modified_since)

    inserted = 0
    skipped = 0
    scanned_records = 0

    for product_data in payload:
        scanned_records += 1

        cin7_id = str(product_data.get("id") or "")
        base_name = product_data.get("name") or ""
        description = product_data.get("description") or ""
        category = product_data.get("category") or ""
        parent_mod = parse_cin7_dt(product_data.get("modifiedDate") or product_data.get("createdDate"))

        product_options = product_data.get("productOptions") or []

        if product_options:
            for opt in product_options:
                opt_code = normalize_sku(opt.get("code"))
                if not opt_code:
                    continue

                # ADD-ONLY: skip if exists
                exists = db.query(Product.id).filter(Product.sku == opt_code).first()
                if exists:
                    skipped += 1
                    continue

                opt_mod = parse_cin7_dt(opt.get("modifiedDate")) or parent_mod
                opt_name = f"{base_name} - {(opt.get('option1') or '')} {(opt.get('option2') or '')} {(opt.get('option3') or '')}".strip()

                p = Product(
                    cin7_id=cin7_id,
                    sku=opt_code,
                    name=opt_name,
                    description=description,
                    price=float(opt.get("retailPrice", 0) or 0),
                    stock_on_hand=int(opt.get("stockOnHand", 0) or 0),
                    category=category,
                    last_modified=opt_mod or utcnow(),
                    synced_at=utcnow(),
                )
                db.add(p)
                inserted += 1

        else:
            # No options: store parent styleCode as SKU
            style_code = normalize_sku(product_data.get("styleCode"))
            if not style_code:
                continue

            exists = db.query(Product.id).filter(Product.sku == style_code).first()
            if exists:
                skipped += 1
                continue

            p = Product(
                cin7_id=cin7_id,
                sku=style_code,
                name=base_name,
                description=description,
                price=0.0,
                stock_on_hand=0,
                category=category,
                last_modified=parent_mod or utcnow(),
                synced_at=utcnow(),
            )
            db.add(p)
            inserted += 1

    db.commit()

    return {
        "cin7_payload_count": len(payload),
        "scanned_records": scanned_records,
        "inserted": inserted,
        "skipped_existing": skipped,
    }

# ---------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------
@app.get("/")
async def root():
    return {
        "message": "Cin7 Product Cache API (manual sync, add-only, watermark incremental)",
        "endpoints": {
            "search_contains": "/products/search?q=query",
            "exact_sku_query": "/products/by-sku?sku=LP5300/29S",
            "get_by_cin7_id": "/products/{cin7_id}",
            "sync_manual": "POST /sync",
            "stats": "/stats",
            "watermark": "/watermark",
            "reset": "POST /reset-database",
        },
    }

@app.get("/products/search")
async def search_products(
    q: str = Query(..., description="Contains search for product name or SKU"),
    limit: int = Query(50, le=500, description="Maximum results to return"),
    db: Session = Depends(get_db),
):
    q_clean = (q or "").strip()
    if not q_clean:
        return []

    products = (
        db.query(Product)
        .filter((Product.name.ilike(f"%{q_clean}%")) | (Product.sku.ilike(f"%{q_clean}%")))
        .limit(limit)
        .all()
    )

    return [
        {
            "cin7_id": p.cin7_id,
            "sku": p.sku,
            "name": p.name,
            "description": p.description,
            "price": p.price,
            "stock_on_hand": p.stock_on_hand,
            "category": p.category,
        }
        for p in products
    ]

@app.get("/products/by-sku")
async def get_product_by_sku_query(
    sku: str = Query(..., description="Exact SKU lookup (supports slashes like LP5300/29S)"),
    db: Session = Depends(get_db),
):
    sku_norm = normalize_sku(sku)
    product = db.query(Product).filter(Product.sku == sku_norm).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    return {
        "cin7_id": product.cin7_id,
        "sku": product.sku,
        "name": product.name,
        "description": product.description,
        "price": product.price,
        "stock_on_hand": product.stock_on_hand,
        "category": product.category,
        "last_modified": product.last_modified,
        "synced_at": product.synced_at,
    }

@app.get("/products/{cin7_id}")
async def get_product_by_id(cin7_id: str, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.cin7_id == cin7_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    return {
        "cin7_id": product.cin7_id,
        "sku": product.sku,
        "name": product.name,
        "description": product.description,
        "price": product.price,
        "stock_on_hand": product.stock_on_hand,
        "category": product.category,
        "last_modified": product.last_modified,
        "synced_at": product.synced_at,
    }

@app.get("/watermark")
async def watermark(db: Session = Depends(get_db)):
    wm = get_watermark(db)
    return {
        "last_checked": wm.last_checked if wm else None,
        "updated_at": wm.updated_at if wm else None,
    }

@app.post("/sync")
async def trigger_sync(
    full: bool = Query(False, description="If true, ignore watermark and fetch from scratch (expensive). Still add-only."),
    db: Session = Depends(get_db),
    _: None = Depends(require_internal_key),
):
    """
    Manual sync only:
    - full=false: uses watermark (cheap)
    - full=true: no modifiedSince (expensive), still only inserts missing SKUs
    """
    try:
        wm = get_watermark(db)
        modified_since = None if full else (wm.last_checked if wm else None)

        stats = sync_new_products_add_only(db, modified_since=modified_since)

        # Update watermark AFTER successful sync
        set_watermark(db, utcnow())
        db.commit()

        return {
            "status": "success",
            "mode": "full" if full else "incremental",
            "modified_since_used": modified_since,
            **stats,
        }
    except Exception as e:
        logger.exception("Sync failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    total_products = db.query(Product).count()
    wm = get_watermark(db)

    return {
        "total_products": total_products,
        "last_manual_sync": wm.last_checked if wm else None,
        "database_url": DATABASE_URL.split("@")[-1],
    }

@app.post("/reset-database")
async def reset_database(
    db: Session = Depends(get_db),
    _: None = Depends(require_internal_key),
):
    """
    Wipes cache + watermark. Use only if you want to rebuild.
    """
    try:
        # Drop tables
        Product.__table__.drop(engine, checkfirst=True)
        SyncWatermark.__table__.drop(engine, checkfirst=True)

        # Recreate
        Product.__table__.create(engine)
        SyncWatermark.__table__.create(engine)

        return {
            "status": "success",
            "message": "Database reset. Next /sync will behave like first run (incremental uses no watermark); /sync?full=true will rebuild cache (expensive).",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------
# LOCAL RUN
# ---------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
