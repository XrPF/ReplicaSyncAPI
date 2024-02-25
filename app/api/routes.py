import os
import json
import requests
from flask import Blueprint, request, current_app, Response
from app.services.mongodb_service import MongoDBService
from app.services.mongodb_service import MongoDBCollectionService
from threading import Thread
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR, THREADS_COLLECTOR
from prometheus_client.core import CollectorRegistry
from prometheus_client.multiprocess import MultiProcessCollector

api_blueprint = Blueprint('api', __name__)
mongodb_service = MongoDBService()
mongodb_collection_service = MongoDBCollectionService(mongodb_service)

ROLE = os.getenv("ROLE")
VM_WORKER_LIST = os.getenv("VM_WORKER_LIST")
WORKERS = [f'http://{vm}' for vm in VM_WORKER_LIST.split(',')]

@api_blueprint.route('/sync', methods=['POST'])
def sync_data():
    data = request.get_json()
    db_name = data.get('db_name', None)
    collection_name = data.get('collection_name', None)
    upsert_key = data.get('upsert_key', None)
    app = current_app._get_current_object()
    
    if ROLE in ["worker", "standalone"]:
        # Start a new thread to run the data synchronization
        thread_sync = Thread(target=mongodb_service.sync_collection, args=(app, db_name, collection_name, upsert_key))
        thread_sync.start()

    elif ROLE == "master":
        for worker in WORKERS:
            requests.post(f'{worker}/sync', json=data)

    return {"message": "Waking up lazy workers to start synchronization processes"}, 202

@api_blueprint.route('/fullSync', methods=['POST'])
def full_sync_data():
    data = request.get_json() or {}
    db_name = data.get('db_name', None)
    collection_name = data.get('collection_name', None)
    upsert_key = data.get('upsert_key', None)
    app = current_app._get_current_object()

    if ROLE in ["worker", "standalone"]:
        # Start a new thread to run the data synchronization
        thread_sync = Thread(target=mongodb_service.sync_collection, args=(app, db_name, collection_name, upsert_key))
        thread_sync.start()
    elif ROLE in ["master", "standalone"]:
        # Start a new thread to run the replica real-time synchronization
        thread_replica = Thread(target=mongodb_service.start_replication, args=(app, db_name, collection_name))
        thread_replica.start()
        if ROLE == "master":
            # Broadcast to all workers to start full sync
            for worker in WORKERS:
                requests.post(f'{worker}/fullSync', json=data)

    return {"message": "Master has taken the whip, waking up scared workers to start synchronization processes"}, 202

@api_blueprint.route('/replicate', methods=['POST'])
def start_replicate_data():
    data = request.get_json()
    db_name = data.get('db_name', None)
    collection_name = data.get('collection_name', None)
    app = current_app._get_current_object()

    if ROLE in ["master", "standalone"]:
        # Start a new thread to run the replica real-time synchronization
        thread_replica = Thread(target=mongodb_service.start_replication, args=(app, db_name, collection_name))
        thread_replica.start()

    return {"message": "Master has taken the control, Real-Time-Replication workers waking up"}, 202

@api_blueprint.route('/killReplica', methods=['POST'])
def stop_replicate_data():
    if ROLE in ["master", "standalone"]:
        # Start a new thread to run the replica real-time synchronization
        thread_replica = Thread(target=mongodb_service.stop_replication)
        thread_replica.start()

    return {"message": "Master is killing Real-Time-Replication workers"}, 202

@api_blueprint.route('/status', methods=['GET'])
def sync_status():
    if ROLE in ["worker", "standalone"]:
        progress = mongodb_collection_service.sync_status_progress()
        if isinstance(progress, str): 
            progress = json.loads(progress)

    elif ROLE == "master":
        progress = {}
        for worker in WORKERS:
            response = requests.get(f'{worker}/status')
            worker_progress = response.json().get('progress')
            if isinstance(worker_progress, str):
                worker_progress = json.loads(worker_progress)
            progress.update(worker_progress)

    return {"progress": progress}, 200

@api_blueprint.route('/metrics')
def metrics():
    # Create a new CollectorRegistry instance.
    registry = CollectorRegistry()

    # Process collectors
    registry.register(PROCESS_COLLECTOR)
    registry.register(PLATFORM_COLLECTOR)
    registry.register(GC_COLLECTOR)
    registry.register(THREADS_COLLECTOR)

    # Create a new MultiProcessCollector instance.
    MultiProcessCollector(registry)

    # Generate and return the latest metrics from the registry.
    return Response(generate_latest(registry), mimetype=CONTENT_TYPE_LATEST)