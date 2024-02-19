import os
import time
import math
import random
import logging
import threading
import subprocess
from dotenv import load_dotenv
from multiprocessing import Manager
from pymongo import MongoClient, UpdateOne
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class MongoDBService:
    def __init__(self):
        self.load_env_vars()
        self.init_mongo_connections()
        self.init_document_processing()

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
    
    def calculate_batch_size(self, total_docs):
        return math.ceil((int(total_docs * self.percentage) // 100) / self.max_workers)
    
    def calculate_sleep_time(self):
        base_sleep_time = min(self.max_workers, 60)
        return random.uniform((base_sleep_time / 2) / self.total_machines, base_sleep_time)
    
    def get_processed_batches(self, batch_file):
        manager = Manager()
        processed_batches = manager.list()
        if os.path.exists(batch_file):
            with open(batch_file, 'r') as f:
                processed_batches.extend([int(line.strip()) for line in f])
        return processed_batches
    
    def fetch_documents(self, i, batch_size, session):
        return self.coll_src.find(session=session, no_cursor_timeout=True).sort('_id', 1).skip(i).limit(batch_size)
    
    def build_operations(self, i, cursor, upsert_key):
        operations = []
        num_ids = 0
        for doc in cursor:
            num_ids += 1
            update_key = {'_id': doc['_id']}
            if upsert_key is not None:
                update_key[upsert_key] = doc[upsert_key]
            operations.append(UpdateOne(update_key, {'$set': doc}, upsert=True))
            logger.debug(f'[{threading.current_thread().name}] ({i}): Upsert document with _id: {doc["_id"]}')
        return operations, num_ids
    
    def write_documents(self, i, operations, batch_file, num_ids):
        if operations:
            try:
                self.coll_dst.bulk_write(operations)
                with open(batch_file, 'a') as f:
                    f.write(f'{i}\n')
                with self.processed_docs_lock:
                    self.processed_docs += num_ids
            except Exception as e:
                logger.error(f'[{threading.current_thread().name}] ({i}): ERROR in bulk_write: {e}')
                raise

    def log_and_sleep(self, i, operations, num_ids, read_time, write_time, sleep_time):
        progress = self.sync_status_progress().split('%')[0]
        logger.info(f'[{threading.current_thread().name}] ({i}): Fetched {num_ids} docs in {round(read_time, 3)}s. Written {num_ids} docs in {round(write_time, 3)}s. Progress: {progress}%')
        if operations and write_time < read_time:
            if write_time * 2 < read_time:
                 read_sleep_time = random.uniform(read_time + sleep_time, read_time * 2 + sleep_time)
            else:
                read_sleep_time = random.uniform(read_time, read_time * 2)
            logger.warn(f"[{threading.current_thread().name}] ({i}): Read threshold exceeded, let's take a break for {read_sleep_time} seconds...")
            time.sleep(read_sleep_time)

    def sync_status_progress(self):
        total_docs_per_machine = self.total_docs / self.total_machines
        progress = round((self.processed_docs / total_docs_per_machine) * 100, 2)
        progress_bar = '#' * int(progress) + '-' * (100 - int(progress))
        return f'{progress}% [{progress_bar}]'
    
    def process_batch(self, i, batch_size, batch_file, upsert_key=None):
        logger.debug(f'[{threading.current_thread().name}] ({i}): Start batch')
        sleep_time = self.calculate_sleep_time()
        logger.debug(f'[{threading.current_thread().name}] ({i}): Sleeping for {round(sleep_time, 1)} seconds...')
        time.sleep(sleep_time)

        with self.syncSrc.start_session() as session:
            cursor = None
            try:
                start_time = time.time()
                cursor = self.fetch_documents(i, batch_size, session)
                operations, num_ids = self.build_operations(i, cursor, upsert_key)
                end_time = time.time()
                read_time = round(end_time - start_time, 3)
                start_time = time.time()
                self.write_documents(i, operations, batch_file, num_ids)
                end_time = time.time()
                write_time = round(end_time - start_time, 3)
                self.log_and_sleep(i, operations, num_ids, read_time, write_time, sleep_time)
            except Exception as e:
                logger.error(f'[{threading.current_thread().name}] ({i}): ERROR: {e}')
                raise
            finally:
                if cursor:
                    cursor.close()
                session.end_session()

    def process_batches(self, batch_size, batch_file, start_batch, end_batch, upsert_key=None):
        parent_batches = math.ceil(self.total_docs / batch_size)
        logger.info(f'Processing batches {start_batch}-{end_batch} with batch size is {batch_size}')

        last_processed_batch = -1
        if os.path.exists(batch_file):
            with open(batch_file, 'r') as f:
                lines = f.read().splitlines()
                if lines:
                    try:
                        last_processed_batch = int(lines[-1])  # get the last line
                    except ValueError:
                        logger.error(f"Cannot convert {lines[-1]} to integer")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in range(start_batch, min(end_batch, parent_batches)):
                if i > last_processed_batch:
                    executor.submit(self.process_batch, i * batch_size, batch_size, batch_file, upsert_key)

        logger.info(f'Processed up to batch {end_batch}')

    def compare_and_update(self, db_name=None, collection_name=None):
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

    def sync_collection(self, db_name=None, collection_name=None, upsert_key=None):
        for db_name, collection_name in self.compare_and_update(db_name, collection_name):
            self.db_name = db_name
            self.collection_name = collection_name
            batch_file=f'/tmp/{self.db_name}_{self.collection_name}_batch.txt'
            self.coll_src = self.get_collection(self.db_name, self.collection_name, self.syncSrc)
            self.coll_dst = self.get_collection(self.db_name, self.collection_name, self.syncDst)
            self.total_docs = self.coll_src.estimated_document_count()
            logger.info(f'[{self.machine_id}] ({self.db_name}) Estimated docs: {self.total_docs} in collection {self.collection_name}')
            batch_size = self.calculate_batch_size(self.total_docs)
            parent_batches = math.ceil(self.total_docs / batch_size)
            batches_per_machine = math.ceil(parent_batches / self.total_machines)
            start_batch = (self.machine_id - 1) * batches_per_machine
            end_batch = min(start_batch + batches_per_machine, parent_batches)
            logger.info(f'[{self.machine_id}] Batch size is {batch_size}. Parent batches: {parent_batches}. Batches per machine: {batches_per_machine}. Start batch: {start_batch}. End batch: {end_batch}')
            self.process_batches(batch_size, batch_file, start_batch, end_batch, upsert_key)
            if os.path.exists(batch_file):
                os.remove(batch_file)
            logger.info(f'[{self.machine_id}] Sync ended for {self.db_name}.{self.collection_name}. Closed connections to databases and exiting...')
        self.close_connections()
    
    def start_replication(self, db_name=None, collection_name=None):
        self.executor = None
        self.futures = []
        collections_to_replicate = self.compare_and_update(db_name, collection_name)
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