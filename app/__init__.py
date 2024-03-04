from flask import Flask
from .api.routes import api_blueprint
import logging
from logging.handlers import RotatingFileHandler

def create_app():
    app = Flask(__name__)

    handler = RotatingFileHandler('/var/log/ReplicaSyncAPI/service.log', maxBytes=10000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    app.logger.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.register_blueprint(api_blueprint)

    return app