from typing import Protocol


class DataSource(Protocol):
    def pricing_data(self) -> float:
        pass

    def forecasting_data(self) -> float:
        pass

    def carbon_data(self) -> float:
        pass
