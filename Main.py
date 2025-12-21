# Cin7 Product Sync API with FastAPI + PostgreSQL

## Setup Instructions

### 1. Install dependencies
```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary requests python-dotenv apscheduler
```

### 2. Create .env file with your Cin7 credentials
```
CIN7_API_KEY=your_api_key_here
CIN7_ACCOUNT_ID=your_account_id_here
DATABASE_URL=postgresql://user:password@localhost/cin7_products
```

### 3. Run the app
```bash
python main.py
```

---

## File: main.py

```python
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import os
from dotenv import load_dotenv
from typing import Optional, List
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/cin7_products")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Cin7 API configuration
CIN7_API_KEY = os.getenv("CIN7_API_KEY")
CIN7_ACCOUNT_ID = os.getenv("CIN7_ACCOUNT_ID")
CIN7_BASE_URL = "https://api.cin7.com/api/v1"

# Product model
class Product(Base):
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True)
    cin7_id = Column(String, unique=True, index=True)
    sku = Column(String, index=True)
    name = Column(String, index=True)
    description = Column(Text)
    price = Column(Float)
    stock_on_hand = Column(Integer)
    category = Column(String)
    last_modified = Column(DateTime)
    synced_at = Column(DateTime, default=datetime.utcnow)
    
    # Add text search index for name and description
    __table_args__ = (
        Index('ix_product_search', 'name', 'sku', postgresql_using='gin', postgresql_ops={'name': 'gin_trgm_ops', 'sku': 'gin_trgm_ops'}),
    )

# Create tables
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Cin7 Product Sync API", version="1.0.0")

# Cin7 API client
class Cin7Client:
    def __init__(self):
        self.api_key = CIN7_API_KEY
        self.account_id = CIN7_ACCOUNT_ID
        self.base_url = CIN7_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def get_products(self, modified_since: Optional[datetime] = None, page: int = 1, page_size: int = 250):
        """Fetch products from Cin7 API"""
        params = {
            "page": page,
            "rows": page_size
        }
        
        if modified_since:
            # Format: 2024-01-01T00:00:00
            params["modifiedSince"] = modified_since.strftime("%Y-%m-%dT%H:%M:%S")
        
        try:
            response = requests.get(
                f"{self.base_url}/Products",
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching products from Cin7: {e}")
            return []
    
    def get_all_products(self, modified_since: Optional[datetime] = None):
        """Fetch all products with pagination"""
        all_products = []
        page = 1
        page_size = 250
        
        while True:
            logger.info(f"Fetching page {page}...")
            products = self.get_products(modified_since=modified_since, page=page, page_size=page_size)
            
            if not products:
                break
            
            all_products.extend(products)
            
            if len(products) < page_size:
                break
            
            page += 1
        
        logger.info(f"Fetched {len(all_products)} products from Cin7")
        return all_products

cin7_client = Cin7Client()

# Database functions
def sync_products(db: Session, modified_since: Optional[datetime] = None):
    """Sync products from Cin7 to local database"""
    logger.info(f"Starting product sync {'(incremental)' if modified_since else '(full)'}")
    
    products = cin7_client.get_all_products(modified_since=modified_since)
    
    synced_count = 0
    for product_data in products:
        # Map Cin7 product fields to our model
        product = db.query(Product).filter(Product.cin7_id == str(product_data.get('id'))).first()
        
        if not product:
            product = Product(cin7_id=str(product_data.get('id')))
        
        # Update product fields
        product.sku = product_data.get('code', '')
        product.name = product_data.get('name', '')
        product.description = product_data.get('description', '')
        product.price = float(product_data.get('price', 0) or 0)
        product.stock_on_hand = int(product_data.get('stockOnHand', 0) or 0)
        product.category = product_data.get('category', '')
        
        # Parse last modified date
        modified_str = product_data.get('modifiedDate') or product_data.get('createdDate')
        if modified_str:
            try:
                product.last_modified = datetime.fromisoformat(modified_str.replace('Z', '+00:00'))
            except:
                product.last_modified = datetime.utcnow()
        
        product.synced_at = datetime.utcnow()
        
        db.add(product)
        synced_count += 1
    
    db.commit()
    logger.info(f"Synced {synced_count} products to database")
    return synced_count

def get_last_sync_time(db: Session) -> Optional[datetime]:
    """Get the timestamp of the last successful sync"""
    result = db.query(Product).order_by(Product.synced_at.desc()).first()
    return result.synced_at if result else None

# Background sync job
def run_sync_job():
    """Background job that syncs products every 15 minutes"""
    db = SessionLocal()
    try:
        last_sync = get_last_sync_time(db)
        if last_sync:
            # Sync products modified since last sync (with 1 min buffer)
            modified_since = last_sync - timedelta(minutes=1)
            sync_products(db, modified_since=modified_since)
        else:
            # First sync - get everything
            sync_products(db)
    except Exception as e:
        logger.error(f"Error in sync job: {e}")
    finally:
        db.close()

# Setup scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(run_sync_job, 'interval', minutes=15, id='sync_products')

@app.on_event("startup")
async def startup_event():
    """Start the background sync scheduler"""
    scheduler.start()
    logger.info("Background sync scheduler started (runs every 15 minutes)")

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown the scheduler"""
    scheduler.shutdown()

# API Endpoints

@app.get("/")
async def root():
    return {
        "message": "Cin7 Product Sync API",
        "endpoints": {
            "search": "/products/search?q=query",
            "get_by_sku": "/products/sku/{sku}",
            "get_by_id": "/products/{cin7_id}",
            "sync_now": "/sync",
            "stats": "/stats"
        }
    }

@app.get("/products/search")
async def search_products(
    q: str = Query(..., description="Search query for product name or SKU"),
    limit: int = Query(50, le=500, description="Maximum results to return")
):
    """Search products by name or SKU"""
    db = SessionLocal()
    try:
        # Search in name and SKU fields
        products = db.query(Product).filter(
            (Product.name.ilike(f"%{q}%")) | (Product.sku.ilike(f"%{q}%"))
        ).limit(limit).all()
        
        return [
            {
                "cin7_id": p.cin7_id,
                "sku": p.sku,
                "name": p.name,
                "description": p.description,
                "price": p.price,
                "stock_on_hand": p.stock_on_hand,
                "category": p.category
            }
            for p in products
        ]
    finally:
        db.close()

@app.get("/products/sku/{sku}")
async def get_product_by_sku(sku: str):
    """Get a product by SKU"""
    db = SessionLocal()
    try:
        product = db.query(Product).filter(Product.sku == sku).first()
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
            "synced_at": product.synced_at
        }
    finally:
        db.close()

@app.get("/products/{cin7_id}")
async def get_product_by_id(cin7_id: str):
    """Get a product by Cin7 ID"""
    db = SessionLocal()
    try:
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
            "synced_at": product.synced_at
        }
    finally:
        db.close()

@app.post("/sync")
async def trigger_sync(full: bool = Query(False, description="Perform full sync instead of incremental")):
    """Manually trigger a product sync"""
    db = SessionLocal()
    try:
        if full:
            count = sync_products(db)
        else:
            last_sync = get_last_sync_time(db)
            modified_since = last_sync - timedelta(minutes=1) if last_sync else None
            count = sync_products(db, modified_since=modified_since)
        
        return {
            "status": "success",
            "synced_count": count,
            "sync_type": "full" if full else "incremental"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/stats")
async def get_stats():
    """Get database statistics"""
    db = SessionLocal()
    try:
        total_products = db.query(Product).count()
        last_sync = get_last_sync_time(db)
        
        return {
            "total_products": total_products,
            "last_sync": last_sync,
            "database_url": DATABASE_URL.split('@')[-1]  # Hide credentials
        }
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

---

## File: requirements.txt

```
fastapi==0.104.1
uvicorn==0.24.0
sqlalchemy==2.0.23
psycopg2-binary==2.9.9
requests==2.31.0
python-dotenv==1.0.0
apscheduler==3.10.4
```

---

## File: Procfile

```
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

---

## File: .gitignore

```
.env
__pycache__/
*.pyc
*.pyo
venv/
.DS_Store
```

---

## Usage Examples

Once running, you can:

**Search for products:**
```bash
curl "http://localhost:8000/products/search?q=widget&limit=10"
```

**Get product by SKU:**
```bash
curl "http://localhost:8000/products/sku/ABC123"
```

**Trigger manual sync:**
```bash
curl -X POST "http://localhost:8000/sync"
```

**Get stats:**
```bash
curl "http://localhost:8000/stats"
```

**Full sync (first time):**
```bash
curl -X POST "http://localhost:8000/sync?full=true"
```

---

## How It Works

1. **Initial Setup**: Run `/sync?full=true` to pull all 60k products
2. **Auto-sync**: Every 15 minutes, fetches only products modified since last sync
3. **Fast Lookups**: All searches hit PostgreSQL (milliseconds) instead of Cin7 API (seconds)
4. **Fresh Data**: New products appear within 15 minutes max, or trigger manual sync

## Notes

- Adjust the sync interval in the scheduler (currently 15 minutes)
- The Cin7 API fields may differ - check their docs and adjust the mapping in `sync_products()`
- Add indexes on other fields if you need to search by them
- For production, add proper error handling, retry logic, and monitoring