import random

from routers.datasource import DataSource


class MockData(DataSource):

    def pricing_data(self) -> float:
        return random.randint(1, 100) + random.random()

    def forecasting_data(self) -> float:
        return random.randint(1, 100) + random.random()

    def carbon_data(self) -> float:
        return random.randint(1, 100) + random.random()

