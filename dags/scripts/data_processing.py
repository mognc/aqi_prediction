import requests
import pandas as pd
from datetime import datetime
from feast import FeatureStore

API_KEY = '133565bf364f6ff5bcf87f0fd1a407ef'
BASE_URL_WEATHER = 'http://api.openweathermap.org/data/2.5/weather'
BASE_URL_AIR_QUALITY = 'http://api.openweathermap.org/data/2.5/air_pollution'

def get_weather_data(city):
    url = f"{BASE_URL_WEATHER}?q={city}&appid={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'coord' not in data:
            print(f"Missing 'coord' in weather data for {city}: {data}")
            return None
        return {
            'city': data['name'],
            'lon': data['coord']['lon'],
            'lat': data['coord']['lat'],
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'temp': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'temp_min': data['main']['temp_min'],
            'temp_max': data['main']['temp_max'],
            'pressure': data['main']['pressure'],
            'humidity': data['main']['humidity'],
            'visibility': data['visibility'],
            'wind_speed': data['wind']['speed'],
            'wind_deg': data['wind']['deg'],
            'clouds_all': data['clouds']['all'],
            'dt': datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S'),
            'sunrise': datetime.utcfromtimestamp(data['sys']['sunrise']).strftime('%Y-%m-%d %H:%M:%S'),
            'sunset': datetime.utcfromtimestamp(data['sys']['sunset']).strftime('%Y-%m-%d %H:%M:%S'),
            'country': data['sys']['country']
        }
    else:
        print(f"Failed to get weather data for {city}: {response.status_code} - {response.text}")
        return None

def get_air_quality_data(lat, lon):
    url = f"{BASE_URL_AIR_QUALITY}?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            'aqi': data['list'][0]['main']['aqi'],
            'co': data['list'][0]['components']['co'],
            'no': data['list'][0]['components']['no'],
            'no2': data['list'][0]['components']['no2'],
            'o3': data['list'][0]['components']['o3'],
            'so2': data['list'][0]['components']['so2'],
            'pm2_5': data['list'][0]['components']['pm2_5'],
            'pm10': data['list'][0]['components']['pm10'],
            'nh3': data['list'][0]['components']['nh3'],
            'air_quality_dt': datetime.utcfromtimestamp(data['list'][0]['dt']).strftime('%Y-%m-%d %H:%M:%S')
        }
    else:
        print(f"Failed to get air quality data: {response.status_code} - {response.text}")
        return None

def collect_and_preprocess():
    cities = ['Lahore', 'Karachi', 'Islamabad']
    csv_file_path = '/home/mognc/airflow/project_output/weather_data.csv'
    all_data = []
    for city in cities:
        weather_data = get_weather_data(city)
        if weather_data:
            air_quality_data = get_air_quality_data(weather_data['lat'], weather_data['lon'])
            if air_quality_data:
                combined_data = {**weather_data, **air_quality_data}
                all_data.append(combined_data)
    # Save to CSV
    df = pd.DataFrame(all_data)
    df.to_csv(csv_file_path,mode = 'a', index=False, header= False)

    store = FeatureStore(repo_path="/home/mognc/airflow/weather_features_store/feature_repo")
    store.materialize_incremental(end_date=datetime.utcnow())

    print(f"Data saved to {csv_file_path}")
