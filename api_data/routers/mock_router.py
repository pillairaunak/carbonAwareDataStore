from threading import Thread
import time

from fastapi import APIRouter
from pydantic import BaseModel

from routers.datasource import DataSource
from schemas.electricitymaps import ElectricityMapSource
from schemas.mocksource import MockData

router = APIRouter()

class Data(BaseModel):
    pricing_data: float
    forecasting_data: float
    carbon_data: float


data = Data(pricing_data=0.0, forecasting_data=0.0, carbon_data=0.0)


def update_data(source: DataSource):
    while True:
        data.carbon_data = source.carbon_data()
        data.forecasting_data = source.forecasting_data()
        data.pricing_data = source.pricing_data()
        time.sleep(60)


@router.on_event("startup")
def start_background_task():
    source = ElectricityMapSource()
    thread = Thread(target=update_data, args=[source], daemon=True)
    thread.start()


@router.get("/data", response_model=Data)
def get_data():
    return data
