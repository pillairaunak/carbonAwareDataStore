from typing import Optional, List
from pydantic import BaseModel

class ForecastingData(BaseModel):
    """
    Forecasting data schema.
    """

    # The timestamp of the forecast
    timestamp: str

    # The value of the forecast
    value: float

class ForecastingDataList(BaseModel):
    """
    List of forecasting data.
    """

    # The list of forecasting data
    forecast: Optional[List[ForecastingData]] = [] 