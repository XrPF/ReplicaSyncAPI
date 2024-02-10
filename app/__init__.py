from flask import Flask
from .api.routes import api_blueprint
from .services.mongodb_service import MongoDBService
import logging

def create_app():
    app = Flask(__name__)

    handler = logging.FileHandler('api.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    app.logger.setLevel(logging.INFO)
    app.logger.addHandler(handler)

    app.register_blueprint(api_blueprint)

    MongoDBService()

    return app