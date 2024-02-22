import os
import time
import logging
from multiprocessing import current_process
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from app.services.mongodb_service import MongoDBService

class MongoDBReplicaService(MongoDBService):
    def __init__(self, uri1, uri2):
        self.syncSrc = MongoClient(uri1)
        self.syncDst = MongoClient(uri2)

    def replicate_changes(self, db_name, collection_name):
        # Create a new logger for this thread
        thread_name = current_process().name
        thread_id = current_process().pid
        logger_name = f'{thread_name}_{thread_id}_{db_name}_{collection_name}'
        logger = logging.getLogger(logger_name)
        handler = logging.FileHandler(f'{logger_name}.log')
        logger.addHandler(handler)

        collection_src = self.get_collection(db_name, collection_name, self.syncSrc)
        collection_dst = self.get_collection(db_name, collection_name, self.syncDst)

        token_file = f'/opt/replicator/resume_token_{db_name}_{collection_name}.txt'
        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                resume_token = f.read()
        else:
            resume_token = None

        logger.info(f'[{thread_name}] Starting to replicate changes for {db_name}.{collection_name}')
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
                                collection_dst.update_one(document_key, update_description)
                            elif operation_type == 'delete':
                                collection_dst.delete_one(document_key)
                            elif operation_type == 'replace':
                                full_document = change['fullDocument']
                                collection_dst.replace_one(document_key, full_document)
                            logger.info(f'[{thread_name}][{db_name}.{collection_name}] Operation: {operation_type} ID: {document_key}')

                            # Save the resume token
                            resume_token = change['_id']
                            with open(f'/opt/replicator/resume_token_{db_name}_{collection_name}.txt', 'w') as f:
                                f.write(str(resume_token))
                            retry_delay = 1
                    break
                except ConnectionFailure:
                    logger.error(f'[{thread_name}][{db_name}.{collection_name}] Connection error, retrying...')
                    time.sleep(retry_delay)
                    retry_delay *= 2
                except Exception as e:
                    logger.error(f'[{thread_name}][{db_name}.{collection_name}] Error in replicate_changes: {e}')