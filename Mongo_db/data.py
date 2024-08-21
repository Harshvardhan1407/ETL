from concurrent.futures import ThreadPoolExecutor
import os
import concurrent
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId

import mongo_query.datatransformation as datatransformation
import mongo_query.sensor_ids as sensor_ids
from config.db_port import get_database
from logs import logs_config


db = get_database()
sensor = db.load_profile_jdvvnl

def data_fetch(sensor_id, site_id):
    try:
        from_id = f"{sensor_id}-2024-01-01 00:00:00"
        to_id = f"{sensor_id}-2024-03-31 23:59:59"
        query = {"_id": {"$gte": from_id, "$lt": to_id}}

        results = list(sensor.find(query))
        for doc in results:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        if results:
            df = datatransformation.init_transformation(results, site_id)
            if df is None:
                logs_config.logger.info(f"Nothing transformed for sensor_id: {sensor_id}")
            else:
                logs_config.logger.info(f"Fetched and transformed {len(df)} records for sensor_id: {sensor_id}")
            return df
        else:
            logs_config.logger.info(f"No records found for sensor_id: {sensor_id}")
            return None

    except Exception as e:
        logs_config.logger.error(f"Error fetching data for sensor_id {sensor_id}: {e}", exc_info=True)
        return None

def fetch_data_for_sensors(circle_id, output_dir="sensor_data"):
    os.makedirs(output_dir, exist_ok=True)

    sensors = sensor_ids.sensor_ids(circle_id)
    sensorids = [doc["id"] for doc in sensors]
    site_ids = [doc["site_id"] for doc in sensors]

    all_dicts = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(data_fetch, sensor_id, site_id): (sensor_id, site_id) for sensor_id, site_id in zip(sensorids, site_ids)}

        for future in concurrent.futures.as_completed(futures):
            dicts = future.result()
            if dicts is not None and len(dicts) > 0:
                all_dicts.extend(dicts)

    if all_dicts:
        combined_df = pd.DataFrame(all_dicts)
        
        # Convert all object type columns to strings if necessary
        combined_df = combined_df.applymap(lambda x: str(x) if isinstance(x, ObjectId) else x)

        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, os.path.join(output_dir, f"{circle_id}_data.parquet"))
        
        logs_config.logger.info(f"Saved data for circle_id: {circle_id}")
        return "saved"
    else:
        logs_config.logger.info(f"No data to save for circle_id: {circle_id}")
        return "no data"

