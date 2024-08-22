import requests
import pandas as pd
import numpy as np
import datetime

# Function to get weather information from OpenWeatherMap API and add it to a DataFrame
def weather_info(csv_path):
    # Read the input CSV file into a DataFrame
    df = pd.read_csv(csv_path)
    
    # Convert 'Timestamp' from nanoseconds to seconds and fill missing values with 0
    df['Timestamp_in_Seconds'] = (df['Timestamp'] / 1e9).fillna(0).astype(int)

    # Initialize empty columns for storing weather data from OpenWeatherMap API
    df['Temperature(Celsius)_OpenWeather'] = np.nan
    df['Feels_Like(Celsius)_OpenWeather'] = np.nan
    df['Pressure(hPa)_OpenWeather'] = np.nan
    df['Humidity(%)_OpenWeather'] = np.nan
    df['Temp_Min(Celsius)_OpenWeather'] = np.nan
    df['Temp_Max(Celsius)_OpenWeather'] = np.nan
    df['Wind_Speed(m/s)_OpenWeather'] = np.nan
    df['Wind_Direction(deg)_OpenWeather'] = np.nan
    df['Cloud_Cover_OpenWeather'] = np.nan
    df['Weather_Main_OpenWeather'] = pd.Series(dtype='str')
    df['Weather_Description_OpenWeather'] = pd.Series(dtype='str')

    # OpenWeatherMap API key
    api_key = 'Your_API_Key'
    # Loop through each row in the DataFrame
    for i in range(len(df)):
        print(df['Timestamp_in_Seconds'][i] - 1800)  # Debug print for checking timestamp range
        try:
            # Construct OpenWeatherMap API URL using latitude, longitude, and timestamp range
            url = f"https://history.openweathermap.org/data/2.5/history/city?lat={df['INSPVAS__Latitude'][i]}
            &lon={df['INSPVAS__Longitude'][i]}&type=hour&start={df['Timestamp_in_Seconds'][i]-1800}
            &end={df['Timestamp_in_Seconds'][i]+1800}&units=metric&appid={api_key}"

            r = requests.get(url)  # Send GET request to the API
            response_json = r.json()  # Parse the JSON response
            if r.status_code == 200:  # Check if the request was successful
                if 'list' in response_json:  # Check if 'list' is in the response
                    weather_data = response_json['list'][0]  # Get the first entry in the weather data list
                    # Update the DataFrame with weather information
                    df.at[i, 'Temperature(Celsius)_OpenWeather'] = weather_data['main'].get('temp')
                    df.at[i, 'Feels_Like(Celsius)_OpenWeather'] = weather_data['main'].get('feels_like')
                    df.at[i, 'Pressure(hPa)_OpenWeather'] = weather_data['main'].get('pressure')
                    df.at[i, 'Humidity(%)_OpenWeather'] = weather_data['main'].get('humidity')
                    df.at[i, 'Temp_Min(Celsius)_OpenWeather'] = weather_data['main'].get('temp_min')
                    df.at[i, 'Temp_Max(Celsius)_OpenWeather'] = weather_data['main'].get('temp_max')
                    df.at[i, 'Wind_Speed(m/s)_OpenWeather'] = weather_data['wind'].get('speed')
                    df.at[i, 'Wind_Direction(deg)_OpenWeather'] = weather_data['wind'].get('deg')
                    df.at[i, 'Cloud_Cover_OpenWeather'] = weather_data['clouds'].get('all')
                    df.at[i, 'Weather_Main_OpenWeather'] = weather_data['weather'][0].get('main')
                    df.at[i, 'Weather_Description_OpenWeather'] = weather_data['weather'][0].get('description')
                else:
                    print(f"No weather data found for index {i}. Response: {response_json}")  # Print if no data is found
            else:
                print(f"Error for request at index {i}: {r.status_code}, {r.text}")  # Print error if request fails
        except Exception as e:
            print(f"Exception for index {i}: {e}")  # Print exception if any occurs during the process
    return df  # Return the updated DataFrame

# CSV file path
csv_file_path = r'C:\Your_File_Path\Your_File_Name.csv'
# Function call to get weather information and store the results in a DataFrame
df_results = weather_info(csv_file_path)
# Save the results to a new CSV file
df_results.to_csv(r'C:\Your_File_Path\Your_File_Name_output.csv', index=False)