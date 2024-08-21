from config.db_port import get_database
from logs import logs_config

def circle_id():
    db = get_database()
    circle = db.circle
    
    try:
        data = circle.find({},{"_id":0,"id":1},)
        result = list(data)
        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        logs_config.logger.info("Fetched circle IDs from database")
        return result
    except Exception as e:
        logs_config.logger.error("Error fetching circle IDs:", exc_info=True)
        raise e