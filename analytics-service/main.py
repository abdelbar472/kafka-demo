"""
ANALYTICS SERVICE (Kafka Consumer)

Responsibilities:
1. Listen to "order.created" topic
2. Store order events for analysis
3. Provide analytics endpoints:
   - total orders & revenue
   - top products
   - orders per user
"""

import json
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Dict
from uuid import uuid4
from collections import defaultdict

from fastapi import FastAPI, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, Float, DateTime, JSON, Integer
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from aiokafka import AIOKafkaConsumer

# ---------- Kafka ----------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "order.created"
consumer_task = None

# ---------- Database ----------
SQLALCHEMY_DATABASE_URL = "sqlite:///./analytics.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class OrderEvent(Base):
    __tablename__ = "order_events"

    id = Column(String, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True)
    order_id = Column(String, index=True, nullable=False)
    user_email = Column(String, index=True, nullable=False)
    total = Column(Float, nullable=False)
    items = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------- Helper ----------
def store_order_event(db: Session, event: dict) -> OrderEvent:
    """Store order event (idempotent)."""
    existing = (
        db.query(OrderEvent)
        .filter(OrderEvent.event_id == event["event_id"])
        .first()
    )
    if existing:
        return existing

    order_event = OrderEvent(
        id=str(uuid4()),
        event_id=event["event_id"],
        order_id=event["order_id"],
        user_email=event["user_email"],
        total=event["total"],
        items=event["items"],
    )
    db.add(order_event)
    db.commit()
    db.refresh(order_event)
    return order_event


# ---------- Kafka Consumer ----------
async def consume_orders():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="analytics-service",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    print("✅ Analytics Service: Kafka consumer started (group: analytics-service)")

    try:
        async for msg in consumer:
            event = msg.value
            print(f"📊 Analytics received: order_id={event['order_id']} total=${event['total']:.2f}")

            db = SessionLocal()
            try:
                store_order_event(db, event)
            except Exception as e:
                print(f"   → Error: {e}")
            finally:
                db.close()
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task
    consumer_task = asyncio.create_task(consume_orders())
    yield
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass


# ---------- Schemas ----------
class SummaryResponse(BaseModel):
    total_orders: int
    total_revenue: float
    average_order_value: float
    unique_customers: int


class ProductStat(BaseModel):
    name: str
    quantity_sold: int
    revenue: float


class CustomerStat(BaseModel):
    user_email: str
    orders_count: int
    total_spent: float


# ---------- App ----------
app = FastAPI(title="Analytics Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "analytics"}


@app.get("/analytics/summary", response_model=SummaryResponse)
def get_summary(db: Session = Depends(get_db)):
    events = db.query(OrderEvent).all()

    if not events:
        return SummaryResponse(
            total_orders=0,
            total_revenue=0.0,
            average_order_value=0.0,
            unique_customers=0,
        )

    total_orders = len(events)
    total_revenue = sum(e.total for e in events)
    unique_customers = len(set(e.user_email for e in events))
    avg = total_revenue / total_orders if total_orders > 0 else 0.0

    return SummaryResponse(
        total_orders=total_orders,
        total_revenue=round(total_revenue, 2),
        average_order_value=round(avg, 2),
        unique_customers=unique_customers,
    )


@app.get("/analytics/top-products", response_model=List[ProductStat])
def get_top_products(limit: int = 10, db: Session = Depends(get_db)):
    events = db.query(OrderEvent).all()

    product_stats: Dict[str, dict] = defaultdict(lambda: {"qty": 0, "revenue": 0.0})

    for event in events:
        for item in event.items:
            name = item["name"]
            qty = item["qty"]
            price = item["price"]
            product_stats[name]["qty"] += qty
            product_stats[name]["revenue"] += qty * price

    sorted_products = sorted(
        product_stats.items(),
        key=lambda x: x[1]["revenue"],
        reverse=True,
    )[:limit]

    return [
        ProductStat(
            name=name,
            quantity_sold=stats["qty"],
            revenue=round(stats["revenue"], 2),
        )
        for name, stats in sorted_products
    ]


@app.get("/analytics/top-customers", response_model=List[CustomerStat])
def get_top_customers(limit: int = 10, db: Session = Depends(get_db)):
    events = db.query(OrderEvent).all()

    customer_stats: Dict[str, dict] = defaultdict(lambda: {"orders": 0, "spent": 0.0})

    for event in events:
        customer_stats[event.user_email]["orders"] += 1
        customer_stats[event.user_email]["spent"] += event.total

    sorted_customers = sorted(
        customer_stats.items(),
        key=lambda x: x[1]["spent"],
        reverse=True,
    )[:limit]

    return [
        CustomerStat(
            user_email=email,
            orders_count=stats["orders"],
            total_spent=round(stats["spent"], 2),
        )
        for email, stats in sorted_customers
    ]


@app.get("/analytics/events")
def list_events(db: Session = Depends(get_db)):
    events = db.query(OrderEvent).order_by(OrderEvent.created_at.desc()).limit(50).all()
    return [
        {
            "order_id": e.order_id,
            "user_email": e.user_email,
            "total": e.total,
            "items_count": len(e.items),
            "created_at": e.created_at.isoformat(),
        }
        for e in events
    ]
