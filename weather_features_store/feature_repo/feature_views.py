from feast import FeatureView, FileSource, Field, Entity
from datetime import timedelta
from feast.types import Float32, Int64, String
import pandas as pd

city = Entity(name="city", join_keys=["city"])

csv_path = '/home/mognc/airflow/project_output/weather_data.csv'
parquet_path = '/home/mognc/airflow/weather_features_store/feature_repo/data/weather.parquet'

df = pd.read_csv(csv_path)

if 'event_timestamp' not in df.columns:
    df['event_timestamp'] = pd.to_datetime('now')
df.to_parquet(parquet_path, engine='pyarrow')

weather_data_source = FileSource(
    path=parquet_path,
    event_timestamp_column="event_timestamp",
)

weather_features = FeatureView(
    name="weather_features",
    entities=[city],
    ttl=timedelta(days=1),
    schema=[
        Field(name="city", dtype=String),
        Field(name="weather_main", dtype=String),
        Field(name="weather_description", dtype=String),
        Field(name="temp", dtype=Float32),
        Field(name="feels_like", dtype=Float32),
        Field(name="temp_min", dtype=Float32),
        Field(name="temp_max", dtype=Float32),
        Field(name="pressure", dtype=Int64),
        Field(name="humidity", dtype=Int64),
        Field(name="visibility", dtype=Int64),
        Field(name="wind_speed", dtype=Float32),
        Field(name="wind_deg", dtype=Int64),
        Field(name="clouds_all", dtype=Int64),
        Field(name="dt", dtype=String),
        Field(name="aqi", dtype=Float32),
        Field(name="co", dtype=Float32),
        Field(name="no", dtype=Float32),
        Field(name="no2", dtype=Float32),
        Field(name="o3", dtype=Float32),
        Field(name="so2", dtype=Float32),
        Field(name="pm2_5", dtype=Float32),
        Field(name="pm10", dtype=Float32),
        Field(name="nh3", dtype=Float32),
        Field(name="air_quality_dt", dtype=String)
    ],
    source=weather_data_source,
    online=True,
)
