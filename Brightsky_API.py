import requests
import pandas as pd
import numpy as np
import datetime

# Function to get weather information from the Brightsky API and add it to a DataFrame
def weather_info(csv_path):
    # Read the input CSV file into a DataFrame
    df = pd.read_csv(csv_path)

    # Convert the 'Timestamp' column from nanoseconds to datetime and split into date and time columns
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ns')
    df['Timestamp_date'] = df['Timestamp'].dt.date
    df['Timestamp_time'] = df['Timestamp'].dt.time

    # Initiating empty data columns for storing weather information
    df['Visibility(m)_GPS'] = ''
    df['Precipitation(mm)_GPS'] = ''
    df['Temperature(Celsius)_GPS'] = ''
    df['Cloud_cover_GPS'] = ''
    df['Sunshine(Mins/last hour)_GPS'] = ''
    df['Weather_Condition_GPS'] = ''
    df['weather_station_distance (m)_GPS'] = ''
    df['weather_station_name_GPS'] = ''

    # Loop through each row in the DataFrame
    for i in range(len(df)):
        # Construct the Brightsky API URL using latitude, longitude, and date
        url = r"https://api.brightsky.dev/weather?lat=" + str(df['INSPVAS__Latitude'][i]) + "&lon=" + str(df['INSPVAS__Longitude'][i]) + "&date=" + str(df['Timestamp_date'][i])

        # Send a GET request to the API
        r = requests.get(url)
        print(r.json)  # Print the JSON response

        if r.status_code == 200:  # Check if the request was successful
            weather_data = r.json()['weather']  # Extract the 'weather' data from the response
            timestamp = df['Timestamp_time'][i]  # Get the timestamp for the current row
            
            # Convert the 'timestamp' value in the API response to a datetime object
            weather_data = [
                {
                    'timestamp': datetime.datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S%z'),
                    'visibility': data['visibility'],
                    'precipitation': data['precipitation'],
                    'temperature': data['temperature'],
                    'cloud_cover': data['cloud_cover'],
                    'sunshine': data['sunshine'],
                    'condition': data['condition']
                }
                for data in weather_data
            ]
            
            # Find the matching weather data for the corresponding timestamp
            matching_data = next((data for data in weather_data if data['timestamp'].hour == timestamp.hour), None)
            
            if matching_data:  # If matching data is found, update the DataFrame with weather information
                df.at[i, 'Visibility(m)_GPS'] = matching_data.get("visibility")
                df.at[i, 'Precipitation(mm)_GPS'] = matching_data.get("precipitation")
                df.at[i, 'Temperature(Celsius)_GPS'] = matching_data.get("temperature")
                df.at[i, 'Cloud_cover_GPS'] = matching_data.get("cloud_cover")
                df.at[i, 'Sunshine(Mins/last hour)_GPS'] = matching_data.get("sunshine")
                df.at[i, 'Weather_Condition_GPS'] = matching_data.get("condition")
            
            # Extract weather station information from the response and update the DataFrame
            sources = r.json().get('sources', [])
            if sources:
                df.at[i, 'weather_station_distance (m)_GPS'] = sources[0].get('distance')
                df.at[i, 'weather_station_name_GPS'] = sources[0].get('station_name')
    
    return df

# CSV file path
csv_file_path = r'C:\Your_File_Path\Your_File_Name.csv'
# Function call to get weather information and store the results in a DataFrame
df_results = weather_info(csv_file_path)
# Save the results to a new CSV file
df_results.to_csv(r'C:\Your_File_Path\Your_Output_File_Name.csv', index=False)