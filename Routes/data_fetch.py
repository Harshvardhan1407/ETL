from Mongo_db import data , circle_id
from flask import Blueprint,jsonify,request
from logs import logs_config
from mongo_query import sensor_ids

data_bp= Blueprint('data_fetch',__name__)

@data_bp.route("/data",methods = ['POST','GET'])
def data_find():
    logs_config.logger.info("Data Fetching")
    circle_ids = request.args.get('circle_id')
    # sensors = sensor_ids.sensor_ids(circle_id)
    # sensors = request.args.get("sensors")
    
    results = data.fetch_data_for_sensors(circle_ids)
    

    return results