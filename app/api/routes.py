from flask import Blueprint, request
from app.services.mongodb_service import MongoDBService
from threading import Thread

api_blueprint = Blueprint('api', __name__)
mongodb_service = MongoDBService()

@api_blueprint.route('/sync', methods=['POST'])
def sync_data():
    data = request.get_json()
    #if not data or 'db_name' not in data or 'collection_name' not in data:
    #    return {"message": "Missing required parameters: db_name, collection_name"}, 400

    db_name = data.get('db_name', None)
    collection_name = data.get('collection_name', None)
    upsert_key = data.get('upsert_key', None)

    # Start a new thread to run the data synchronization
    thread = Thread(target=mongodb_service.compare_and_update, args=(db_name, collection_name, upsert_key))
    thread.start()

    return {"message": "Waking up data synchronization processes"}, 202

@api_blueprint.route('/status', methods=['GET'])
def sync_status():
    progress = mongodb_service.sync_status_progress()

    return {"progress": progress}, 200