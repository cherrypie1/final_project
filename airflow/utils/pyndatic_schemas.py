from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date


class UserProfile(BaseModel):
    user_id: int
    name: Optional[str]
    email: Optional[str]
    birth_date: Optional[date]
    gender: Optional[str]


class UserDevice(BaseModel):
    user_id: int
    device_type: Optional[str]
    os: Optional[str]
    browser: Optional[str]


class UserLocation(BaseModel):
    user_id: int
    city: Optional[str]
    country: Optional[str]
    lat: Optional[float]
    lon: Optional[float]


class EventModel(BaseModel):
    user_id: int
    event_type: str
    timestamp: datetime
    session_id: Optional[str] = None
    product_id: Optional[int] = None
    order_id: Optional[str] = None
    quantity: Optional[int] = None
    cost_usd: Optional[float] = None
    cost_eur: Optional[float] = None
    cost_rub: Optional[float] = None
    weather_temp: Optional[float] = None
    weather_desc: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None


class OrderModel(BaseModel):
    order_id: str
    user_id: int
    order_ts: datetime
    total_usd: Optional[float] = None


class OrderItemModel(BaseModel):
    order_id: str
    product_id: int
    quantity: int
    price_usd: Optional[float] = None


