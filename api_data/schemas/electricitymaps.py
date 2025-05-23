import requests
from routers.datasource import DataSource

CARBON_API_KEY = "RZ3Zp595HbEVg9wlGXPa"


class ElectricityMapSource(DataSource):

    def pricing_data(self) -> float:
        # TODO implement
        return random.randint(1, 100) + random.random()


    def _request_carbon_intensity_forecast(self) -> float:
        """Carbon intensity forecast. """
        url = "https://api.electricitymap.org/v3/carbon-intensity/forecast"
        # TODO add request parameters
        headers = {"auth-token": CARBON_API_KEY}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    def forecasting_data(self) -> float:
        # TODO implement
        # data = self._request_carbon_intensity_forecast()
        return random.randint(1, 100) + random.random()

    def _request_current_carbon_intensity(self) -> dict:
        url = "https://api.electricitymap.org/v3/carbon-intensity/latest?zone=DE"
        headers = {"auth-token": CARBON_API_KEY}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    def carbon_data(self) -> float:
        try:
            data = self._request_current_carbon_intensity()
            if data is not None:
                return data['carbonIntensity']
            else:
                return None

        except KeyError as e:
            pass

