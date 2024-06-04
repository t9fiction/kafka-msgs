from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator
from fastapi import FastAPI
from sqlmodel import Field, SQLModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dotenv import load_dotenv, find_dotenv
import os
import json
# from app.engine_file import engine

# Load environment variables from .env file if it exists
load_dotenv(find_dotenv())

# Get the database URL from the environment variable
BOOTSTRAP_SERVER = os.getenv('BOOTSTRAP_SERVER')
KAFKA_ORDER_TOPIC = os.getenv('KAFKA_ORDER_TOPIC')

# Ensure the connection string is retrieved correctly
if not BOOTSTRAP_SERVER:
    raise ValueError("No BOOTSTRAP_SERVER environment variable set")

class Order(SQLModel):
    id: Optional[int]=Field(default=None)
    username: str
    product_id: int
    product_name: str
    product_price: int


# The first part of the function, before the yield, will
# be executed before the application starts
@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Getting Kafka online...")
    # db.create_db_and_tables()
    yield


app = FastAPI(lifespan=lifespan, title="Kafka API")


# @app.on_event("startup")
# def startup():
#     md.create_db_and_tables()

@app.get("/")
def read_root():
    return {"App": "The App Dockerized"}


@app.post("/create_order")
async def buy_prod(order:Order):
    orderJSON=json.dumps(order).encode("utf-8")
    producer=AIOKafkaProducer(BOOTSTRAP_SERVER)
    try:
        # produce message
        await producer.send_and_wait(KAFKA_ORDER_TOPIC, orderJSON)
    finally:
        producer.stop()
    return orderJSON