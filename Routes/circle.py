from Mongo_db import circle_id
from flask import Blueprint,jsonify
from logs import logs_config


circle_bp= Blueprint('circle',__name__)

@circle_bp.route("/circles",methods=['POST','GET'])
def circle_ids():
    logs_config.logger.info("Circle IDS called")
    return jsonify(circle_id.circle_id())