import os
import time
import logging
from logging.handlers import RotatingFileHandler
import datetime
from multiprocessing import current_process
from pymongo import MongoClient
from bson import json_util
import json
from pymongo.errors import ConnectionFailure, PyMongoError
from app.services.mongodb_service import MongoDBService

class MongoDBReplicaService(MongoDBService):
    def __init__(self, uri1, uri2, prometheus_service):
        self.syncSrc = MongoClient(uri1)
        self.syncDst = MongoClient(uri2)
        self.prometheus_service = prometheus_service
        self.thread_name = current_process().name
        self.thread_id = current_process().pid
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger_name = f'{self.thread_name}_{self.thread_id}'
        logger = logging.getLogger(logger_name)
        log_file_path = os.getenv('LOG_FILE_PATH', f'/var/log/ReplicaSyncAPI/{logger_name}.log')
        handler = RotatingFileHandler(log_file_path, maxBytes=10000000, backupCount=5)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def replicate_changes(self, db_name, collection_name):
        logger = self.logger
        collection_src = self.get_collection(db_name, collection_name, self.syncSrc)
        collection_dst = self.get_collection(db_name, collection_name, self.syncDst)

        token_file = f'/opt/replicator/resume_token_{db_name}_{collection_name}.txt'
        resume_token = self._read_resume_token(token_file)

        logger.info(f'Starting to replicate changes for {db_name}.{collection_name}')
        last_change_time = datetime.datetime.now()
        retry_delay = 1

        while True:
            try:
                with collection_src.watch(resume_after=resume_token) as stream:
                    for change in stream:
                        self._process_change(change, collection_dst, db_name, collection_name, last_change_time)
                        resume_token = change['_id']
                        self._write_resume_token(token_file, resume_token)
                        retry_delay = 1

                    elapsed_time = datetime.datetime.now() - last_change_time
                    if elapsed_time > datetime.timedelta(minutes=5):
                        logger.info(f'No changes detected in the last 5 minutes')
                break
                
            except ConnectionFailure:
                self._handle_error('ConnectionFailure', db_name, collection_name, retry_delay)
                retry_delay = min(retry_delay * 2, 60)
                
            except PyMongoError as e:
                self._handle_pymongo_error(e, db_name, collection_name, token_file)
                
            except Exception as e:
                self._handle_generic_error(e, db_name, collection_name)

    def _read_resume_token(self, token_file):
        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                return json_util.loads(f.read())
        return None

    def _write_resume_token(self, token_file, resume_token):
        with open(token_file, 'w') as f:
            f.write(json_util.dumps(resume_token))

    def _process_change(self, change, collection_dst, db_name, collection_name, last_change_time):
        logger = self.logger
        operation_type = change['operationType']
        document_key = change['documentKey']
        logger.debug(f'Change detected: {operation_type} {document_key}')
        
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
            logger.info(f"UpdateFields: {update_description['updatedFields']} RemovedFields: {update_description['removedFields']} UpdateDocument: {update_document}")
            collection_dst.update_one(document_key, update_document, upsert=True)
        elif operation_type == 'delete':
            collection_dst.delete_one(document_key)
        elif operation_type == 'replace':
            full_document = change['fullDocument']
            collection_dst.replace_one(document_key, full_document)
        
        logger.info(f'Operation: {operation_type} ID: {document_key}')
        self.prometheus_service.observe_stream_replication_latency(self.thread_name, db_name, collection_name, operation_type, (datetime.datetime.now() - last_change_time).total_seconds())
        self.prometheus_service.increment_stream_service_counter(self.thread_name, db_name, collection_name, operation_type)

    def _handle_error(self, error_type, db_name, collection_name, retry_delay):
        self.prometheus_service.increment_stream_service_errors(self.thread_name, db_name, collection_name, error_type)
        self.logger.error(f'{error_type} error, retrying in {retry_delay} seconds...')
        time.sleep(retry_delay)

    def _handle_pymongo_error(self, error, db_name, collection_name, token_file):
        error_code = getattr(error, 'code', 'Unknown')
        self.prometheus_service.increment_stream_service_errors(self.thread_name, db_name, collection_name, 'PyMongoError_' + str(error_code))
        
        if error_code == 286:
            self.logger.error('Resume token no longer in oplog, starting from scratch.')
            self._write_resume_token(token_file, None)
        else:
            self.logger.error(f'PyMongoError: {error}')

    def _handle_generic_error(self, error, db_name, collection_name):
        self.prometheus_service.increment_stream_service_errors(self.thread_name, db_name, collection_name, 'Exception')
        self.logger.error(f'Error in replicate_changes: {error}')
