import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Parameters for the weather data fetch
url = "https://archive-api.open-meteo.com/v1/archive"
base_params = {
    "hourly": [
        "temperature_2m", "precipitation", "rain", "snowfall","snow_depth", "cloud_cover", "wind_speed_10m","wind_direction_10m", "is_day", "sunshine_duration"
    ]
}

# Path to the CSV file
input_csv_path = r'C:\Your_File_Path\Your_File_Name.csv'
# Read the CSV file
input_df = pd.read_csv(input_csv_path)

# Add transformed date and hour columns to the original input dataframe
input_df['Transformed_Date'] = pd.to_datetime(input_df['Timestamp'], unit='ns').dt.date
input_df['Transformed_Hour'] = pd.to_datetime(input_df['Timestamp'], unit='ns').dt.strftime('%Y-%m-%d %H:00:00')

# Initialize the result dataframe to collect weather data
result_df = pd.DataFrame()

# Loop through each set of coordinates and timestamps
for index, row in input_df.iterrows():
    latitude = row['INSPVAS__Latitude']
    longitude = row['INSPVAS__Longitude']
    timestamp = row['Timestamp']
    
    # Convert the timestamp to a datetime object
    datetime_obj = datetime.utcfromtimestamp(float(timestamp) / 1e9)
    date = datetime_obj.date().isoformat()
    hour_str = datetime_obj.strftime('%Y-%m-%d %H:00:00')
    
    # Update the params for the current location and date
    params = base_params.copy()
    params.update({
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date,
        "end_date": date
    })

    # Fetch the weather data using the Open-Meteo API
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(3).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(4).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(5).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(6).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(7).ValuesAsNumpy()
    hourly_is_day = hourly.Variables(8).ValuesAsNumpy()
    hourly_sunshine_duration = hourly.Variables(9).ValuesAsNumpy()

    # Create a dictionary to hold the fetched hourly data
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
    }
    hourly_data["latitude"] = latitude  # Add latitude to the data
    hourly_data["longitude"] = longitude  # Add longitude to the data
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["rain"] = hourly_rain
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    hourly_data["is_day"] = hourly_is_day
    hourly_data["sunshine_duration"] = hourly_sunshine_duration
    
    # Convert the dictionary to a DataFrame
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    
    # Add the original transformed hour to the fetched data for comparison
    hourly_dataframe['Transformed_Hour'] = hourly_dataframe['date'].dt.strftime('%Y-%m-%d %H:00:00')
    
    # Append the hourly data to the result dataframe
    result_df = pd.concat([result_df, hourly_dataframe], ignore_index=True)

# Merge the original dataframe with the collected weather data
merged_df = input_df.merge(result_df, left_on=['INSPVAS__Latitude', 'INSPVAS__Longitude'], right_on=['latitude', 'longitude'], suffixes=('', '_weather'))

# Filter the merged dataframe to keep only rows where Transformed_Hour matches the hour in the 'date' column
filtered_df = merged_df[merged_df['Transformed_Hour'] == merged_df['Transformed_Hour_weather']]

# Save the filtered merged dataframe to a new CSV file
output_csv_path = r'C:\Your_File_Path\Your_File_Name_output.csv'
filtered_df.to_csv(output_csv_path, index=False)
# Print the file path for reference
print("Filtered merged data saved to:", output_csv_path)