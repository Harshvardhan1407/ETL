from mongo_query.sensor_ids import sensor_ids
from flask import Blueprint,jsonify,request
from logs import logs_config


sensor_bp= Blueprint('sensorids',__name__)

@sensor_bp.route("/sensors",methods=['POST','GET'])
def get_sensor_ids():
    circle_id = request.args.get("circle_id")
    return jsonify(sensor_ids(circle_id))