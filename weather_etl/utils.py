import requests as req
import json


def load_cities() -> list:
    """
    Call an URL to load a JSON file with city data.
    """

    url = "https://www.jsonkeeper.com/b/L3YT"

    try:
        response = req.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except req.RequestException as e:
        print(f"Error fetching cities: {e}")
        return []

def dict_cities(ti) -> dict:
    """
    Convert a list of city dictionaries into a dictionary indexed by city ID.
    """
    cities = ti.xcom_pull(task_ids='get_cities')
    
    d_cities = {}
    for city in cities:
        d_cities[city["id"]] = city
    return d_cities

def get_multi_weather(cities: list) -> list:
    """
    Fetches weather data for multiple cities using the Open Meteo API.
    """

    lats = ",".join(str(c["coordinates"]["latitude"]) for c in cities)
    lons = ",".join(str(c["coordinates"]["longitude"]) for c in cities)

    query_params = {
        "latitude": lats,
        "longitude": lons,
        "current_weather": "true",
        "timezone": "auto",
    }
    try:
        response = req.get(
            "https://api.open-meteo.com/v1/forecast", params=query_params, timeout=10
        )
        response.raise_for_status()
        return response.json()
    except req.RequestException as e:
        return {"error": str(e)}

def clean_response(response: list) -> list:
    """
    Clean the response from the Open Meteo API to a dictionary indexed by location ID.
    """

    d_response = {}

    for weather in response:
        location_id = weather.get("location_id", 0)
        d_response[location_id] = weather.get("current_weather")
    return d_response

def match_cities(d_cities: list, c_response: list) -> list:
    merged = []

    for city_id, city_data in d_cities.items():
        weather_data = c_response.get(city_id)
        merged.append({
            "city_id": city_id,
            "city_name": city_data.get("name"),
            "country": city_data.get("country"),
            "continent": city_data.get("continent"),
            "weather_last_update": weather_data.get("time"),
            "temperature": weather_data.get("temperature"),
            "windspeed": weather_data.get("windspeed"),
            "winddirection": weather_data.get("winddirection"),
            "is_day": weather_data.get("is_day"),
            "weathercode": weather_data.get("weathercode")
        })

    return merged

def output_json(merged_data: list, path="output.json") -> None:
    """
    Write the merged data to a JSON file.
    """

    with open(path, "w") as f:
        json.dump({"cities": merged_data}, f, indent=4)
    print(f"Data written to {path}")