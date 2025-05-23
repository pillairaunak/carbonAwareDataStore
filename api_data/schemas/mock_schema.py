from pydantic import BaseModel

class MockData(BaseModel):
    pricing_data: float
    forecasting_data: float
    carbon_data: float
