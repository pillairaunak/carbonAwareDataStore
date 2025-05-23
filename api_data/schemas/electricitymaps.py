import requests
import random
from routers.datasource import DataSource
from schemas.forecasting_data import ForecastingData
from schemas.forecasting_data import ForecastingDataList

CARBON_API_KEY = "RZ3Zp595HbEVg9wlGXPa"


class ElectricityMapSource(DataSource):

    def _request_carbon_intensity_forecast(self) -> float:
        """Carbon intensity forecast. """
        # curl -X 'GET' \
#   'https://api.energy-charts.info/ren_share_forecast?country=de' \
#   -H 'accept: application/json'
        url = "https://api.energy-charts.info/ren_share_forecast?country=de"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data
        return None

    def forecasting_data(self):
        data_new = self._request_carbon_intensity_forecast()
        if data_new is not None:
            # Extract the forecast value from the response
            seconds = data_new['unix_seconds']
            values = data_new['ren_share']
            return ForecastingDataList(forecast=[ForecastingData(timestamp=str(second), value=(100-v)*8.8) for second, v in zip(seconds, values)])
        else:
            return None
    
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

