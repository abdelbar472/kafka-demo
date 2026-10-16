"""
ORDER SERVICE (Kafka Producer)

Responsibilities:
1. Accept HTTP request to create an order
2. Save order to its own database
3. Publish an "order.created" event to Kafka

Key Kafka concept demonstrated:
→ PRODUCER: pushes messages into a topic
"""

from datetime import datetime
from typing import List
from uuid import uuid4
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, String, Float, DateTime, JSON
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from aiokafka import AIOKafkaProducer

# ---------- Kafka ----------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "order.created"

producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    print("✅ Order Service: Kafka producer started")
    yield
    await producer.stop()
    print("Order Service: Kafka producer stopped")


# ---------- Database ----------
SQLALCHEMY_DATABASE_URL = "sqlite:///./orders.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"

    id = Column(String, primary_key=True, index=True)
    user_email = Column(String, nullable=False, index=True)
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


# ---------- Schemas ----------
class OrderItemSchema(BaseModel):
    name: str
    qty: int = Field(gt=0)
    price: float = Field(gt=0)


class CreateOrderRequest(BaseModel):
    user_email: str
    items: List[OrderItemSchema]


class OrderResponse(BaseModel):
    id: str
    user_email: str
    total: float
    items: List[OrderItemSchema]
    created_at: datetime

    class Config:
        from_attributes = True


# ---------- App ----------
app = FastAPI(title="Order Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "order"}


@app.post("/orders", response_model=OrderResponse)
async def create_order(payload: CreateOrderRequest, db: Session = Depends(get_db)):
    total = sum(item.qty * item.price for item in payload.items)

    order = Order(
        id=str(uuid4()),
        user_email=payload.user_email,
        total=total,
        items=[item.model_dump() for item in payload.items],
    )

    db.add(order)
    db.commit()
    db.refresh(order)

    # ---------- Publish event to Kafka ----------
    event = {
        "event_id": str(uuid4()),
        "order_id": order.id,
        "user_email": order.user_email,
        "total": order.total,
        "items": order.items,
        "created_at": order.created_at.isoformat(),
    }
    await producer.send_and_wait(TOPIC, event)
    print(f"📤 Published order.created → {order.id}")

    return order


@app.get("/orders", response_model=List[OrderResponse])
def list_orders(db: Session = Depends(get_db)):
    return db.query(Order).order_by(Order.created_at.desc()).all()


@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: str, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
