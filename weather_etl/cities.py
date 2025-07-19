from airflow.sdk import asset

@asset(
        name="weather_cities",
        uri = "https://www.jsonkeeper.com/b/L3YT",
        schedule="@daily"
)

def cities(self) -> dict:
    import requests

    response = requests.get(self.uri)
    return response.json()