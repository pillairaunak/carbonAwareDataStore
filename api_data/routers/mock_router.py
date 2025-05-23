from fastapi import APIRouter
from fastapi.responses import JSONResponse
from schemas.mock_schema import MockData
from threading import Thread
import time
import random
import requests

router = APIRouter()

mock_data = MockData(pricing_data=0.0, forecasting_data=0.0, carbon_data=0.0)

def get_present_pricing_data():
    url = "https://api.electricitymap.org/v3/carbon-intensity/latest?zone=DE"
    headers = {"auth-token": "RZ3Zp595HbEVg9wlGXPa"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def update_data():
    while True:
        try:
            data = get_present_pricing_data()
            if data:
                mock_data.pricing_data = data['carbonIntensity']
        except Exception as e:
            pass
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
