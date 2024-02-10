from flask import Blueprint, request
from app.services.mongodb_service import MongoDBService

api_blueprint = Blueprint('api', __name__)
mongodb_service = MongoDBService()

@api_blueprint.route('/sync', methods=['POST'])
def sync_data():
    data = request.get_json()
    if not data or 'db_name' not in data or 'collection_name' not in data:
        return {"message": "Missing required parameters: db_name, collection_name"}, 400

    db_name = data.get('db_name')
    collection_name = data.get('collection_name')
    batch_num = data.get('batch_num', None)
    upsert_key = data.get('upsert_key', None)

    mongodb_service.compare_and_update(db_name, collection_name, batch_num, upsert_key)
    return {"message": "Data synchronization ended"}, 202