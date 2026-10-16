"""
NOTIFICATION SERVICE (Kafka Consumer)

Responsibilities:
1. Continuously listen to the "order.created" topic
2. When a new event arrives → create a notification
3. Expose HTTP endpoints so we can see the notifications

Key Kafka concepts demonstrated:
→ CONSUMER: pulls messages from a topic
→ CONSUMER GROUP: "notification-service" (allows scaling later)
→ IDEMPOTENCY: uses event_id so the same event is not processed twice
"""

import json
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List
from uuid import uuid4

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, Float, DateTime, JSON, Boolean
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from aiokafka import AIOKafkaConsumer

# ---------- Kafka ----------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "order.created"
consumer_task = None

# ---------- Database ----------
SQLALCHEMY_DATABASE_URL = "sqlite:///./notifications.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Notification(Base):
    __tablename__ = "notifications"

    id = Column(String, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True)  # for idempotency
    order_id = Column(String, index=True, nullable=False)
    user_email = Column(String, nullable=False)
    total = Column(Float, nullable=False)
    items = Column(JSON, nullable=False)
    message = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    read = Column(Boolean, default=False)


Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------- Helper ----------
def create_notification_from_event(db: Session, event: dict) -> Notification:
    """Create notification from order.created event (idempotent)."""
    existing = (
        db.query(Notification)
        .filter(Notification.event_id == event["event_id"])
        .first()
    )
    if existing:
        return existing  # already processed → ignore

    notification = Notification(
        id=str(uuid4()),
        event_id=event["event_id"],
        order_id=event["order_id"],
        user_email=event["user_email"],
        total=event["total"],
        items=event["items"],
        message=f"Your order {event['order_id']} for ${event['total']:.2f} has been received.",
    )
    db.add(notification)
    db.commit()
    db.refresh(notification)
    return notification


# ---------- Kafka Consumer ----------
async def consume_orders():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="notification-service",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    print("✅ Notification Service: Kafka consumer started")

    try:
        async for msg in consumer:
            event = msg.value
            print(f"📥 Received event: order_id={event['order_id']}")

            db = SessionLocal()
            try:
                create_notification_from_event(db, event)
                print(f"   → Notification created for order {event['order_id']}")
            except Exception as e:
                print(f"   → Error processing event: {e}")
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
class NotificationResponse(BaseModel):
    id: str
    event_id: str
    order_id: str
    user_email: str
    total: float
    items: list
    message: str
    created_at: datetime
    read: bool

    class Config:
        from_attributes = True


# ---------- App ----------
app = FastAPI(title="Notification Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "notification"}


@app.get("/notifications", response_model=List[NotificationResponse])
def list_notifications(db: Session = Depends(get_db)):
    return (
        db.query(Notification)
        .order_by(Notification.created_at.desc())
        .all()
    )


@app.get("/notifications/{notification_id}", response_model=NotificationResponse)
def get_notification(notification_id: str, db: Session = Depends(get_db)):
    notification = (
        db.query(Notification)
        .filter(Notification.id == notification_id)
        .first()
    )
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    return notification
