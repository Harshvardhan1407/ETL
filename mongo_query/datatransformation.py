from datetime import timedelta, datetime
import holidays
import numpy as np
from config.db_port import get_database
from logs import logs_config
from sklearn.cluster import DBSCAN
import pandas as pd

db = get_database()

def init_transformation(sensor_data, site_id, precomputed_holidays=None, start_date=None, end_date=None):
    df_raw = pd.DataFrame(sensor_data)
    
    """ elimintation null data or low data"""
    if len(df_raw)<10:
        return None
    
    sensor_id = df_raw['sensor_id'].unique()[0]
    
    df = df_raw[['creation_time','sensor_id', 'instant_cum_KW', 'instant_cum_KVA', 'R_Voltage','Y_Voltage', 'B_Voltage', 'R_Current', 'Y_Current', 'B_Current', 'R_PF','Y_PF', 'B_PF', 'cumm_PF', 'status', 'opening_KWh','closing_KWh', 'opening_KVAh', 'closing_KVAh', 'count']]
    
    """ datetime conversion """
    df['creation_time'] = pd.to_datetime(df['creation_time'])
    # df.loc[:,'creation_time'] = pd.to_datetime(df['creation_time'])  # Modify entire column

 

    logs_config.logger.info(f"inital length of dataframe {sensor_id}:{len(df)}")
    
    if start_date is None or end_date is None:
        # Set default values
        start_date = datetime.strptime('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime('2024-03-30 23:59:59', '%Y-%m-%d %H:%M:%S')
    
    """ holidays data addition """
    if precomputed_holidays is None:
        holiday = holidays_list(start_date, end_date)
    df = df[(df['creation_time'] >= start_date) & (df['creation_time'] <= end_date)]
    df['is_holiday'] = df['creation_time'].dt.date.isin(holiday).astype(int)

    df.set_index('creation_time', inplace=True)   

    """ validation and filtering """
    # df.loc[df['cumm_PF'] > 1,'opening_KWh'] = np.nan

    # no_current_df = df[(df['R_Current'] == 0) & (df['B_Current'] == 0) & (df['Y_Current'] == 0)]

    # load_with_no_current_df = no_current_df[(no_current_df['instant_cum_KVA']>0.03) & (no_current_df['instant_cum_KW']>0.03)]
    
    # if len(load_with_no_current_df)!=0:
    #     if len(no_current_df[(no_current_df['instant_cum_KW']>0) | (no_current_df['instant_cum_KVA']>0)])<len(df)/100:
    #         df.loc[load_with_no_current_df.index, 'opening_KWh'] = np.nan
    #     else:
    #         return None

    # no_voltage_df = df[(df['R_Voltage'] == 0) & (df['Y_Voltage'] == 0) & (df['B_Voltage'] == 0)]
    # no_voltage_but_cuurrent = no_voltage_df[(no_voltage_df['R_Current'] != 0) & (no_voltage_df['B_Current'] != 0) & (no_voltage_df['Y_Current'] != 0)]
    # if len(no_voltage_but_cuurrent)!=0:
    #     if len(no_voltage_but_cuurrent)<len(df)/100:
    #         df.loc[no_voltage_but_cuurrent.index, 'opening_KWh'] = np.nan
    #     else:
    #         return None

    filtered_df = df[df['opening_KWh']>df['closing_KWh']]
    if len(filtered_df)<len(df)/100:
        df.loc[filtered_df.index, 'opening_KWh'] = np.nan
    else:
        return None

    """ done"""

    if len(df) > 3000:
        # outage situation
        df.loc[df['opening_KWh'] == 0, "opening_KWh"] = np.nan
        df.loc[df['opening_KWh'].first_valid_index():]

        df.bfill(inplace=True)

        # missing packet
        sensor_df = df[['opening_KWh']].resample(rule="15min").asfreq()
        sensor_df = sensor_df.infer_objects(copy=False)
        sensor_df.interpolate(method="linear", inplace=True)

        # no consumption / same reading
        if sensor_df['opening_KWh'].nunique() < 10:
            return None

        # previous value of opening_KWh
        sensor_df['prev_KWh'] = sensor_df['opening_KWh'].shift(1)
        sensor_df.dropna(inplace=True)
        if len(sensor_df[sensor_df['prev_KWh'] > sensor_df['opening_KWh']]) > 25:
            return None

        # consumed unit
        sensor_df['consumed_unit'] = sensor_df['opening_KWh'] - sensor_df['prev_KWh']
        sensor_df.loc[sensor_df['consumed_unit'] < 0, "opening_KWh"] = sensor_df["prev_KWh"]
        sensor_df.loc[sensor_df['consumed_unit'] < 0, "consumed_unit"] = 0

        description = sensor_df.describe()
        Q2 = description.loc['75%', 'consumed_unit']
        if Q2 < 1:
            return None
        if sensor_df['consumed_unit'].nunique() < 10:
            return None

        # eliminating id's based on slope
        numeric_index = pd.to_numeric(sensor_df.index)
        correlation = np.corrcoef(numeric_index, sensor_df['opening_KWh'])[0, 1]
        coeffs = np.polyfit(numeric_index, sensor_df['opening_KWh'], 1)

        slope = coeffs[0]
        if not np.abs(correlation) > 0.8 and slope > 0:
            return None

        # outlier detection
        epsilon = 11
        min_samples = 3
        dbscan = DBSCAN(eps=epsilon, min_samples=min_samples)
        sensor_df['db_outlier'] = dbscan.fit_predict(sensor_df[['consumed_unit']])

        sensor_df.loc[sensor_df['db_outlier'] == -1, 'consumed_unit'] = np.nan
        
        sensor_df.bfill(inplace=True)
        df1 = add_lags(sensor_df)
        df2 = create_features(df1)
        df2.reset_index(inplace=True)
        if df2.empty is False:
            try:
                weather_data = data_from_weather_api(site_id, start_date, end_date)
                # logs_config.logger.info(f"length of weather_data:{len(weather_data)}")

                if not weather_data.empty:
                    weather_data['time'] = pd.to_datetime(weather_data['time'])
                    weather_data.set_index('time', inplace=True)

                    weather_data = weather_data[~weather_data.index.duplicated(keep='first')]
                    weather_data = weather_data.resample('15 min').ffill()

                    # Convert the creation_time columns to datetime if they are not already
                    weather_data.reset_index(inplace=True)
                    weather_data['creation_time'] = pd.to_datetime(weather_data['time'])
                    # df2['creation_time'] = pd.to_datetime(df2['creation_time'])
                    # return weather_data, df2
                    merged_df = weather_data.merge(df2, on='creation_time', how="inner")
                    merged_df["sensor_id"] = sensor_id
                    mongo_dict = merged_df.to_dict(orient='records')
                    # logs_config.logger.info(f"length of dict:{len(mongo_dict)}")
                    logs_config.logger.info(f"length of dataframe at last {sensor_id}:{len(merged_df)}")

                    return mongo_dict

            except Exception as e:
                print(e)


def holidays_list(start_date_str, end_date_str):
    try:
        start_date = start_date_str.date()
        end_date = end_date_str.date()
        holiday_list = []
        india_holidays = holidays.CountryHoliday('India', years=start_date.year)
        current_date = start_date
        while current_date <= end_date:
            if current_date in india_holidays or current_date.weekday() == 6:
                holiday_list.append(current_date)
            current_date += timedelta(days=1)
        return holiday_list
    except Exception as e:
        return None


# def holidays_list(start_date, end_date):
#   try:
#       india_holidays = holidays.CountryHoliday('India', years=start_date.year)
#       return [d for d in range(start_date, end_date + timedelta(days=1)) if d in india_holidays or d.weekday() == 6]
#   except Exception as e:
#       return None

def add_lags(df):
    try:
        target_map = df['consumed_unit'].to_dict()
        # 15 minutes, 30 minutes, 1 hour
        df['lag1'] = (df.index - pd.Timedelta('15 minutes')).map(target_map)
        df['lag2'] = (df.index - pd.Timedelta('30 minutes')).map(target_map)
        df['lag3'] = (df.index - pd.Timedelta('1 day')).map(target_map)
        df['lag4'] = (df.index - pd.Timedelta('7 days')).map(target_map)
        df['lag5'] = (df.index - pd.Timedelta('15 days')).map(target_map)
        df['lag6'] = (df.index - pd.Timedelta('30 days')).map(target_map)
        df['lag7'] = (df.index - pd.Timedelta('45 days')).map(target_map)
    except KeyError as e:
        print(f"Error: {e}. 'consumed_unit' column not found in the DataFrame.")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

    return df


def create_features(hourly_data):
    hourly_data = hourly_data.copy()

    # Check if the index is in datetime format
    if not isinstance(hourly_data.index, pd.DatetimeIndex):
        hourly_data.index = pd.to_datetime(hourly_data.index)

    hourly_data['day'] = hourly_data.index.day
    hourly_data['hour'] = hourly_data.index.hour
    hourly_data['month'] = hourly_data.index.month
    hourly_data['dayofweek'] = hourly_data.index.dayofweek
    hourly_data['quarter'] = hourly_data.index.quarter
    hourly_data['dayofyear'] = hourly_data.index.dayofyear
    hourly_data['weekofyear'] = hourly_data.index.isocalendar().week
    hourly_data['year'] = hourly_data.index.year
    return hourly_data
def data_from_weather_api(site, startDate, endDate):
    ''' Fetch weather data from CSV file based on date range'''

    # logger.info("Weather data fetching")
    
    try:
        start_date = startDate.strftime('%Y-%m-%d %H:%M:%S')
        end_date = endDate.strftime('%Y-%m-%d %H:%M:%S')
        # conn = get_connection()
        # collection_name = os.getenv("weatherData")
        loadProfile = db.weather_data

        documents = []
        query = loadProfile.find({
            "site_id": site,
            "time": {
                "$gte": start_date,
                "$lte": end_date
            }
        })
        for doc in query:
            documents.append(doc)
        try:

            df = pd.DataFrame(documents)
            return df
        except Exception as e:
            print(e)
    except Exception as e:
        print("Error:", e)