from fastapi import APIRouter
from fastapi.responses import JSONResponse
from schemas.mock_schema import MockData
from threading import Thread
import time
import random

router = APIRouter()

mock_data = MockData(pricing_data=0.0, forecasting_data=0.0, carbon_data=0.0)

def update_data():
    while True:
        mock_data.pricing_data = random.randint(1, 100) + random.random()
        mock_data.forecasting_data = random.randint(1, 100) + random.random()
        mock_data.carbon_data = random.randint(1, 100) + random.random()
        time.sleep(60)

@router.on_event("startup")
def start_background_task():
    thread = Thread(target=update_data, daemon=True)
    thread.start()

@router.get("/data", response_model=MockData)
def get_data():
    return mock_data
