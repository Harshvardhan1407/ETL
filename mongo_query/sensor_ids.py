from config.db_port import get_database
from logs import logs_config
def sensor_ids(circle_id="d0f23f90-facd-11ed-a890-0242bed38519"):
    db = get_database()
    sensor = db.jdvvnlSensor
    try:
        query = {
            "circle_id": circle_id,
            "type": "AC_METER",
            "admin_status": {"$in": ["N", "S", "U"]},
            "utility": "2"
        }
        projection = {"id": 1, "_id": 0,"site_id":1}
        sensor_ids = sensor.find(query, projection)
        return list(sensor_ids)

    except Exception as e:
        logs_config.logger.error("Error fetching sensor IDs:", exc_info=True)
        raise e