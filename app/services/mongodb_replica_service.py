import os
import time
import logging
from logging.handlers import RotatingFileHandler
import datetime
from multiprocessing import current_process
from pymongo import MongoClient
from bson import json_util
import json
from pymongo.errors import ConnectionFailure
from app.services.mongodb_service import MongoDBService

class MongoDBReplicaService(MongoDBService):
    def __init__(self, uri1, uri2, prometheus_service):
        self.syncSrc = MongoClient(uri1)
        self.syncDst = MongoClient(uri2)
        self.prometheus_service = prometheus_service

    def replicate_changes(self, db_name, collection_name):
        thread_name = current_process().name
        thread_id = current_process().pid
        logger_name = f'{thread_name}_{thread_id}_{db_name}_{collection_name}'
        logger = logging.getLogger(logger_name)
        handler = RotatingFileHandler(f'/var/log/ReplicaSyncAPI/{logger_name}.log', maxBytes=10000000, backupCount=5)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        collection_src = self.get_collection(db_name, collection_name, self.syncSrc)
        collection_dst = self.get_collection(db_name, collection_name, self.syncDst)

        token_file = f'/opt/replicator/resume_token_{db_name}_{collection_name}.txt'

        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                resume_token = json_util.loads(f.read())
        else:
            resume_token = None

        logger.info(f'[{thread_name}] Starting to replicate changes for {db_name}.{collection_name}')
        last_change_time = datetime.datetime.now()

        while True:
            for _ in range(3):
                try:
                    with collection_src.watch(resume_after=resume_token) as stream:
                        for change in stream:
                            operation_type = change['operationType']
                            document_key = change['documentKey']
                            logger.debug(f'[{thread_name}][{db_name}.{collection_name}] Change detected: {operation_type} {document_key}')
                            if operation_type == 'insert':
                                full_document = change['fullDocument']
                                collection_dst.insert_one(full_document)
                            elif operation_type == 'update':
                                update_description = change['updateDescription']
                                update_document = {}
                                if 'updatedFields' in update_description:
                                    update_document['$set'] = update_description['updatedFields']
                                if 'removedFields' in update_description:
                                    update_document['$unset'] = {field: "" for field in update_description['removedFields']}
                                logger.info(f"[{thread_name}][{db_name}.{collection_name}] UpdateFields: {update_description['updatedFields']} RemovedFields: {update_description['removedFields']} UpdateDocument: {update_document}")
                                collection_dst.update_one(document_key, update_document, upsert=True)
                            elif operation_type == 'delete':
                                collection_dst.delete_one(document_key)
                            elif operation_type == 'replace':
                                full_document = change['fullDocument']
                                collection_dst.replace_one(document_key, full_document)
                            logger.info(f'[{thread_name}][{db_name}.{collection_name}] Operation: {operation_type} ID: {document_key}')
                            
                            self.prometheus_service.observe_stream_replication_latency(thread_name, db_name, collection_name, operation_type, (datetime.datetime.now() - last_change_time).total_seconds())
                            self.prometheus_service.increment_stream_service_counter(thread_name, db_name, collection_name, operation_type)

                            resume_token = change['_id']
                            with open(f'/opt/replicator/resume_token_{db_name}_{collection_name}.txt', 'w') as f:
                                f.write(json_util.dumps(resume_token))
                            retry_delay = 1

                        elapsed_time = datetime.datetime.now() - last_change_time
                        if elapsed_time > datetime.timedelta(minutes=5):
                            logger.info(f'[{thread_name}][{db_name}.{collection_name}] No changes detected in the last 5 minutes')
                    break
                except ConnectionFailure:
                    self.prometheus_service.increment_stream_service_errors(thread_name, db_name, collection_name, 'ConnectionFailure')
                    logger.error(f'[{thread_name}][{db_name}.{collection_name}] Connection error, retrying...')
                    time.sleep(retry_delay)
                    retry_delay *= 2
                except Exception as e:
                    self.prometheus_service.increment_stream_service_errors(thread_name, db_name, collection_name, 'Exception')
                    logger.error(f'[{thread_name}][{db_name}.{collection_name}] Error in replicate_changes: {e}')
