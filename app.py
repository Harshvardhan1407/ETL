from flask import Flask
from Routes.data_fetch import data_bp
from Routes.circle import circle_bp
from Routes.sensorids import sensor_bp
from logs import logs_config
import logging
from logging.handlers import RotatingFileHandler


app = Flask(__name__)
logs_config.logger.info("App started")
app.register_blueprint(circle_bp)
app.register_blueprint(data_bp)
app.register_blueprint(sensor_bp)


