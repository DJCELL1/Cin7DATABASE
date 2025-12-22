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
import base64
import time

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
CIN7_API_USERNAME = os.getenv("CIN7_API_USERNAME")
CIN7_API_KEY = os.getenv("CIN7_API_KEY")
CIN7_BASE_URL = "https://api.cin7.com/api/v1"

# Product model
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
    last_modified = Column(DateTime)
    synced_at = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Cin7 Product Sync API", version="1.0.0")

# Cin7 API client
class Cin7Client:
    def __init__(self):
        self.username = CIN7_API_USERNAME
        self.api_key = CIN7_API_KEY
        self.base_url = CIN7_BASE_URL
        
        # Create Basic Auth header
        credentials = f"{self.username}:{self.api_key}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        self.headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/json"
        }
    
    def get_products(self, modified_since: Optional[datetime] = None, page: int = 1, page_size: int = 250):
        """Fetch products from Cin7 API"""
        params = {
            "page": page,
            "rows": page_size
        }
        
        if modified_since:
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
            
            # Rate limiting: 3 requests per second = 0.34 seconds between requests
            time.sleep(0.4)
        
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
        # Get product options (variants/SKUs)
        product_options = product_data.get('productOptions', [])
        
        # If there are product options, create a record for each one
        if product_options:
            for option in product_options:
                option_code = option.get('code', '')
                if not option_code:
                    continue
                    
                # Use SKU as unique identifier since multiple options can have same cin7_id
                product = db.query(Product).filter(Product.sku == option_code).first()
                
                if not product:
                    product = Product()
                
                product.cin7_id = str(product_data.get('id'))
                product.sku = option_code
                product.name = f"{product_data.get('name', '')} - {option.get('option1', '')} {option.get('option2', '')} {option.get('option3', '')}".strip()
                product.description = product_data.get('description', '')
                product.price = float(option.get('retailPrice', 0) or 0)
                product.stock_on_hand = int(option.get('stockOnHand', 0) or 0)
                product.category = product_data.get('category', '')
                
                modified_str = option.get('modifiedDate') or product_data.get('modifiedDate') or product_data.get('createdDate')
                if modified_str:
                    try:
                        product.last_modified = datetime.fromisoformat(modified_str.replace('Z', '+00:00'))
                    except:
                        product.last_modified = datetime.utcnow()
                
                product.synced_at = datetime.utcnow()
                
                db.add(product)
                synced_count += 1
        else:
            # No options, just save the parent product
            style_code = product_data.get('styleCode', '')
            if not style_code:
                continue
                
            product = db.query(Product).filter(Product.sku == style_code).first()
            
            if not product:
                product = Product()
            
            product.cin7_id = str(product_data.get('id'))
            product.sku = style_code
            product.name = product_data.get('name', '')
            product.description = product_data.get('description', '')
            product.price = 0
            product.stock_on_hand = 0
            product.category = product_data.get('category', '')
            
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
            modified_since = last_sync - timedelta(minutes=1)
            sync_products(db, modified_since=modified_since)
        else:
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
            "database_url": DATABASE_URL.split('@')[-1]
        }
    finally:
        db.close()

@app.post("/reset-database")
async def reset_database():
    """Drop and recreate the products table"""
    try:
        # Drop the table
        Product.__table__.drop(engine, checkfirst=True)
        # Recreate it with the new schema
        Product.__table__.create(engine)
        
        return {
            "status": "success",
            "message": "Database table reset successfully. Run /sync?full=true to repopulate."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)