from pydantic import BaseModel
import random

from routers.schema import Schema


class MockData(BaseModel, Schema):

    def pricing_data(self) -> float:
        return random.randint(1, 100) + random.random()

    def forecasting_data(self) -> float:
        return random.randint(1, 100) + random.random()

    def carbon_data(self) -> float:
        return random.randint(1, 100) + random.random()

