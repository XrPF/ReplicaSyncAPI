import time
import math
import random
import logging
import threading
import gc
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor
from bson import ObjectId
from bson.errors import InvalidId
from app.services.prometheus_service import PrometheusService

logger = logging.getLogger(__name__)

class MongoDBCollectionService:
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service
        self.prometheus_service = PrometheusService.getInstance()
        
    def calculate_batch_size(self, total_docs, avg_doc_size, base_max_batch_size, target_batch_size):
        dynamic_max_batch_size = max(int(base_max_batch_size * (target_batch_size / avg_doc_size)), 1)
        size_factor = max(min(1, target_batch_size / avg_doc_size), 0.1)
        batch_size = min(math.ceil(total_docs * size_factor), dynamic_max_batch_size)
        batch_size = max(batch_size, 1)
        return batch_size
    
    def calculate_sleep_time(self):
        base_sleep_time = min(self.mongodb_service.total_machines, 60)
        return random.uniform((base_sleep_time / 2) / self.mongodb_service.total_machines, base_sleep_time)
    
    def is_object_id(self, value):
        try:
            ObjectId(self, value)
            return True
        except (InvalidId, TypeError):
            return False
        
    def build_operations(self, cursor, upsert_key):
        operations = []
        num_ids = 0
        for doc in cursor:
            num_ids += 1
            update_key = {'_id': doc['_id']}
            if self.mongodb_service.coll_is_sharded and upsert_key is not None:
                if isinstance(upsert_key, str):
                    update_key[upsert_key] = doc[upsert_key]
                elif isinstance(upsert_key, list):
                    for key in upsert_key:
                        update_key[key] = doc.get(key)
            operations.append(UpdateOne(update_key, {'$set': doc}, upsert=True))
            logger.debug(f'[{threading.current_thread().name}] ({threading.current_thread().name}): Upsert document with _id: {doc["_id"]}')
        if not operations:
            logger.info(f'[{threading.current_thread().name}] No documents found in the cursor')
        return operations, num_ids                
    
    def write_documents(self, operations, num_ids, db_name, collection_name):
        if operations:
            try:
                self.mongodb_service.coll_dst.bulk_write(operations)
                with self.mongodb_service.processed_docs_lock:
                    self.mongodb_service.processed_docs += num_ids
                self.prometheus_service.increment_sync_processed_docs_counter(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, value=num_ids)
            except Exception as e:
                logger.error(f'[{threading.current_thread().name}] ERROR in bulk_write: {e}')
                self.prometheus_service.increment_sync_errors_counter(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, error_type='bulk_write')
                raise
        else:
            logger.info(f'[{threading.current_thread().name}] No operations to write')

    def log_and_sleep(self, num_ids, read_time, write_time, sleep_time, db_name, collection_name):
        progress = self.sync_status_progress().split('%')[0]
        logger.info(f'[{threading.current_thread().name}] Fetched {num_ids} docs in {read_time}s. Written {num_ids} docs in {write_time}s. Progress: {progress}%')
        if write_time < read_time:
            if write_time * 2 < read_time:
                 read_sleep_time = random.uniform(read_time, read_time * 2)
            else:
                read_sleep_time = random.uniform(read_time + sleep_time, read_time * 2 + sleep_time)
            self.prometheus_service.set_sync_sleep_time_gauge(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, value=read_sleep_time)
            logger.debug(f"[{threading.current_thread().name}] Read threshold exceeded, let's take a break for {round(read_sleep_time, 0)} seconds...")
            time.sleep(read_sleep_time)

    def sync_status_progress(self):
        total_docs_per_machine = self.mongodb_service.total_docs / self.mongodb_service.total_machines
        progress = round((self.mongodb_service.processed_docs / total_docs_per_machine) * 100, 2)
        progress_bar = '#' * int(progress) + '-' * (100 - int(progress))
        return f'{progress}% [{progress_bar}]'
    
    def process_batch(self, app, min_id, max_id, db_name, collection_name, upsert_key=None):
        with app.app_context():
            sleep_time = self.calculate_sleep_time()
            self.prometheus_service.set_sync_sleep_time_gauge(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, value=sleep_time)
            logger.debug(f'[{threading.current_thread().name}] Break time, drinking a cup of coffee. Wait me {round(sleep_time, 0)} seconds...')
            time.sleep(sleep_time)
    
            with self.mongodb_service.syncSrc.start_session() as session:
                cursor = None
                try:
                    is_object_id_type = self.is_object_id(min_id)
                    start_time = time.time()

                    if is_object_id_type:
                        cursor = self.mongodb_service.coll_src.find({'_id': {'$gte': ObjectId(min_id), '$lt': ObjectId(max_id)}}, session=session, no_cursor_timeout=True)
                    else:
                        cursor = self.mongodb_service.coll_src.find({'_id': {'$gte': min_id, '$lt': max_id}}, session=session, no_cursor_timeout=True).sort('_id', 1)
                    
                    operations, num_ids = self.build_operations(cursor, upsert_key)
                    end_time = time.time()
                    read_time = round(end_time - start_time, 1)
                    self.prometheus_service.observe_sync_read_time_histogram(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, value=read_time)
                    start_time = time.time()
                    self.write_documents(operations, num_ids, db_name, collection_name)
                    end_time = time.time()
                    write_time = round(end_time - start_time, 1)
                    self.prometheus_service.observe_sync_write_time_histogram(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, value=write_time)
                    self.log_and_sleep(num_ids, read_time, write_time, sleep_time, db_name, collection_name)
                except Exception as e:
                    self.prometheus_service.increment_sync_errors_counter(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, error_type='fetch_write')
                    logger.error(f'[{threading.current_thread().name}] ERROR: {e}')
                    raise
                finally:
                    if cursor:
                        cursor.close()  
                    session.end_session()
                    gc.collect()

    def process_batches(self, app, batch_size, start_batch, end_batch, db_name, collection_name, upsert_key=None):
        batch_min_id = self.mongodb_service.coll_src.find().sort('_id', 1).limit(1)[0]['_id']
        is_object_id_type = self.is_object_id(batch_min_id)
    
        if start_batch > 0:
            for b_loop in range(start_batch):
                if is_object_id_type:
                    batch = list(self.mongodb_service.coll_src.find({'_id': {'$gte': ObjectId(batch_min_id)}}).sort('_id', 1).limit(batch_size))
                else:
                    batch = list(self.mongodb_service.coll_src.find({'_id': {'$gte': batch_min_id}}).sort('_id', 1).limit(batch_size))
                
                batch_min_id = batch[-1]['_id']
                logger.info(f'[{b_loop}/{start_batch}] Skipping batch. Min _id: {batch_min_id}')
        
        logger.info(f'[Main-Thread] Starting to process batches from {start_batch} to {end_batch}. Min _id: {batch_min_id}')
    
        for i in range(start_batch, end_batch):
            if is_object_id_type:
                batch = list(self.mongodb_service.coll_src.find({'_id': {'$gte': ObjectId(batch_min_id)}}, no_cursor_timeout=True).sort('_id', 1).limit(batch_size))
            else:
                batch = list(self.mongodb_service.coll_src.find({'_id': {'$gte': batch_min_id}}, no_cursor_timeout=True).sort('_id', 1).limit(batch_size))
            
            batch_count = len(batch)
    
            if batch_count > 0:
                last_document = batch[batch_count - 1]
                batch_max_id = last_document['_id']
                logger.info(f'[Main-Thread] Processing batch {i+1} of {end_batch}. Min _id: {batch_min_id}. Max _id: {batch_max_id}')
                self.process_batch(app, batch_min_id, batch_max_id, db_name, collection_name, upsert_key)
                batch_min_id = batch_max_id
            else:
                logger.info(f'[Main-Thread] No more documents to process')
                break
    
        logger.debug(f'Processed up to batch {end_batch}')
