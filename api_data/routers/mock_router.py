from threading import Thread
import time

from fastapi import APIRouter
from pydantic import BaseModel

from routers.datasource import DataSource
from schemas.electricitymaps import ElectricityMapSource
from schemas.mocksource import MockData
from schemas.forecasting_data import ForecastingData, ForecastingDataList
from typing import Optional, List

router = APIRouter()

class Data():
    def __init__(self):
        self.carbon_data: float = 0.0
        self.forecasting_data: Optional[ForecastingDataList] = None


data = Data()


def update_data(source: DataSource):
    while True:
        carbon_data = source.carbon_data()
        if carbon_data is not None:
            data.carbon_data = carbon_data
        carbon_data_forecast = source.forecasting_data()
        if carbon_data_forecast is not None:
            data.forecasting_data = carbon_data_forecast
        time.sleep(60)


@router.on_event("startup")
def start_background_task():
    source = ElectricityMapSource()
    thread = Thread(target=update_data, args=[source], daemon=True)
    thread.start()


@router.get("/carbon-intensity")
def get_carbon_intensity():
    return data.carbon_data

@router.get("/carbon-intensity-forecast")
def get_carbon_intensity_forecast():
    return data.forecasting_data

