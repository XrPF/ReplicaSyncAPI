import os
import gc
import math
import psutil
import logging
import objgraph
import threading
import tracemalloc
from dotenv import load_dotenv
from multiprocessing import Manager
from memory_profiler import profile
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
from mongodb_collection_service import MongoDBCollectionService

logger = logging.getLogger(__name__)

class MongoDBService:
    def __init__(self):
        self.load_env_vars()
        self.init_mongo_connections()
        self.init_document_processing()
        logger.info(f'[{self.machine_id}] ReplicaSyncAPI initialized. {self.max_workers} workers available. {self.total_machines} machines available.')
        logger.info(f'[{self.machine_id}] Garbage collector is enabled: {gc.isenabled()}. Garbage collector threshold: {gc.get_threshold()}')

    def load_env_vars(self):
        load_dotenv()
        VM_WORKER_LIST = os.getenv('VM_WORKER_LIST')
        ROLE = os.getenv('ROLE')
        split_workers = VM_WORKER_LIST.split(',') if VM_WORKER_LIST else []
        self.total_machines = len(split_workers)
        if ROLE != 'master':
            self.machine_id = int(os.getenv('VM_WORKER_ID'))
        else:
            self.machine_id = "master-0"
        self.max_workers = int(os.getenv('MAX_WORKERS', 1))
        self.percentage = float(os.getenv('PERCENTAGE', 0.2))
    
    def init_mongo_connections(self):
        uri1 = os.getenv('MONGO_CONNECTION_STRING_1') or self.build_mongo_uri('MONGO_HOSTS_1', 'MONGO_OPTS_1')
        uri2 = os.getenv('MONGO_CONNECTION_STRING_2') or self.build_mongo_uri('MONGO_HOSTS_2', 'MONGO_OPTS_2')
        self.syncSrc = MongoClient(uri1)
        self.syncDst = MongoClient(uri2)
        self.db_name = os.getenv('DB_NAME')
        self.collection_name = os.getenv('COLLECTION_NAME')

    def build_mongo_uri(self, hosts_env_var, opts_env_var):
        return f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv(hosts_env_var)}/?{os.getenv(opts_env_var)}"
    
    def init_document_processing(self):
        self.total_docs = 0
        self.processed_docs = 0
        self.processed_docs_lock = threading.Lock()
    
    def get_collection(self, db_name, collection_name, client):
        db = client[db_name]
        collection = db[collection_name]
        return collection
    
    def close_connections(self):
        self.syncSrc.close()
        self.syncDst.close()

    def target_dbs_collections(self, db_name=None, collection_name=None):
        collections_to_sync = []
        if db_name is not None and collection_name is not None:
            self.db_name = db_name
            self.collection_name = collection_name
            logger.info(f'[{self.machine_id}] Database {self.db_name} and collection {self.collection_name} received. Starting sync...')
            collections_to_sync.append((self.db_name, self.collection_name))
        elif db_name is None and collection_name is None:
            if self.db_name is not None and self.collection_name is not None:
                logger.info(f'[{self.machine_id}] Database {self.db_name} and collection {self.collection_name} found on .env vars. Starting sync...')
                collections_to_sync.append((self.db_name, self.collection_name))
            elif self.db_name is not None and self.collection_name is None:
                logger.info(f'[{self.machine_id}] Database {self.db_name} found on .env vars. Looking for collections to sync...')
                for collection_name in self.syncSrc[self.db_name].list_collection_names():
                    self.collection_name = collection_name
                    logger.info(f'[{self.machine_id}] Added collection {self.db_name}.{self.collection_name}')
                    collections_to_sync.append((self.db_name, self.collection_name))
            else:
                logger.info(f'[{self.machine_id}] No database and collection received. Looking for databases and collections to sync...')
                for db_name in self.syncSrc.list_database_names():
                    if db_name not in ['config', 'local', 'admin']:
                        logger.info(f'[{self.machine_id}] Found database {db_name}. Looking for collections to sync...')
                        for collection_name in self.syncSrc[db_name].list_collection_names():
                            self.db_name = db_name
                            self.collection_name = collection_name
                            logger.info(f'[{self.machine_id}] Added collection {self.db_name}.{self.collection_name}')
                            collections_to_sync.append((self.db_name, self.collection_name))
        elif db_name is not None and collection_name is None:
            self.db_name = db_name
            logger.info(f'[{self.machine_id}] Database {self.db_name} received. Looking for collections to sync...')
            for collection_name in self.syncSrc[self.db_name].list_collection_names():
                self.collection_name = collection_name
                logger.info(f'[{self.machine_id}] Added collection {self.db_name}.{self.collection_name}')
                collections_to_sync.append((self.db_name, self.collection_name))
        return collections_to_sync

    @profile
    def sync_collection(self, db_name=None, collection_name=None, upsert_key=None):
        tracemalloc.start()

        mongodb_collections = MongoDBCollectionService(self)    
        for db_name, collection_name in self.target_dbs_collections(db_name, collection_name):
            self.coll_src = self.get_collection(db_name, collection_name, self.syncSrc)
            self.coll_dst = self.get_collection(db_name, collection_name, self.syncDst)
            self.total_docs = self.coll_src.estimated_document_count()
            logger.info(f'[{self.machine_id}] ({self.db_name}) Estimated docs: {self.total_docs} in collection {self.collection_name}')
            batch_size = mongodb_collections.calculate_batch_size(self.total_docs)
            parent_batches = math.ceil(self.total_docs / batch_size)
            batches_per_machine = math.ceil(parent_batches / self.total_machines)
            start_batch = (self.machine_id - 1) * batches_per_machine
            end_batch = min(start_batch + batches_per_machine, parent_batches)
            logger.info(f'[{self.machine_id}] Batch size is {batch_size}. Parent batches: {parent_batches}. Batches per machine: {batches_per_machine}. Start batch: {start_batch}. End batch: {end_batch}')
            mongodb_collections.process_batches(batch_size, start_batch, end_batch, upsert_key)
            logger.info(f'[{self.machine_id}] Sync ended for {self.db_name}.{self.collection_name}. Closed connections to databases and exiting...')
            
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            with open('/var/log/ReplicaSyncAPI/tracemalloc.log', 'w') as file:
                for stat in top_stats[:10]:
                    file.write(str(stat))
                    file.write('\n')

            with open('/var/log/ReplicaSyncAPI/objgraph.log', 'w') as file:
                objgraph.show_most_common_types(limit=10, file=file)

            with open('/var/log/ReplicaSyncAPI/gc.log', 'w') as file:
                for obj in gc.get_objects():
                    file.write(str(obj))
                    file.write('\n')

        self.close_connections()
        gc.collect()
    
    def start_replication(self, db_name=None, collection_name=None):
        self.executor = None
        self.futures = []
        collections_to_replicate = self.target_dbs_collections(db_name, collection_name)
        total_collections_to_replicate = len(collections_to_replicate)
        self.executor = ThreadPoolExecutor(max_workers=total_collections_to_replicate)
        self.futures = [self.executor.submit(self.replicate_changes, db_name, collection_name) for db_name, collection_name in collections_to_replicate]
    
    def stop_replication(self):
        for future in self.futures:
            future.cancel()
        if self.executor:
            self.executor.shutdown()

    def replicate_changes(self, db_name, collection_name):
        self.db_name = db_name
        self.collection_name = collection_name
        collection_src = self.get_collection(self.db_name, self.collection_name, self.syncSrc)
        collection_dst = self.get_collection(self.db_name, self.collection_name, self.syncDst)

        logger.info(f'[Real-Time-Replication] Starting to replicate changes for {self.db_name}.{self.collection_name}')
        try:
            with collection_src.watch() as stream:
                for change in stream:
                    operation_type = change['operationType']
                    document_key = change['documentKey']
                    logger.debug(f'[Real-Time-Replication] ({self.db_name}.{self.collection_name}) Change detected: {operation_type} {document_key}')
                    if operation_type == 'insert':
                        full_document = change['fullDocument']
                        collection_dst.insert_one(full_document)
                        logger.info(f'[Real-Time-Replication] ({self.db_name}.{self.collection_name}) Inserted document: {document_key}')
                    elif operation_type == 'update':
                        update_description = change['updateDescription']
                        collection_dst.update_one(document_key, update_description)
                        logger.info(f'[Real-Time-Replication] ({self.db_name}.{self.collection_name}) Updated document: {document_key}')
                    elif operation_type == 'delete':
                        collection_dst.delete_one(document_key)
                        logger.info(f'[Real-Time-Replication] ({self.db_name}.{self.collection_name}) Deleted document: {document_key}')
                    elif operation_type == 'replace':
                        full_document = change['fullDocument']
                        collection_dst.replace_one(document_key, full_document)
                        logger.info(f'[Real-Time-Replication] ({self.db_name}.{self.collection_name}) Replaced document: {document_key}')
        except Exception as e:
            logger.error(f'[Real-Time-Replication] ({self.db_name}.{self.collection_name}) Error in replicate_changes: {e}')
            raise