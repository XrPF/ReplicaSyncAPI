import time
import math
import random
import logging
import threading
import gc
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor
from app.services.prometheus_service import PrometheusService

logger = logging.getLogger(__name__)

class MongoDBCollectionService:
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service
        self.prometheus_service = PrometheusService.getInstance()

    def calculate_batch_size(self, total_docs):
        if total_docs >= 1_000_000:
            divisor = 100
        elif 1_000 <= total_docs < 1_000_000:
            divisor = 10
        else:
            divisor = 1

        num_batches = max((int(total_docs * self.mongodb_service.percentage) // divisor), 1)
        max_workers = max(min(self.mongodb_service.max_workers, num_batches), 1)
        batch_size = math.ceil(num_batches / max_workers)
        return max(batch_size, 1)
    
    def calculate_sleep_time(self):
        base_sleep_time = min(self.mongodb_service.max_workers, 60)
        return random.uniform((base_sleep_time / 2) / self.mongodb_service.total_machines, base_sleep_time)
    
    def fetch_documents(self, i, batch_size, session):
        return self.mongodb_service.coll_src.find(session=session, no_cursor_timeout=True).sort('_id', 1).skip(i).limit(batch_size)

    def build_operations(self, i, cursor, upsert_key):
        operations = []
        num_ids = 0
        for doc in cursor:
            num_ids += 1
            update_key = {'_id': doc['_id']}
            if self.mongodb_service.coll_is_sharded and upsert_key is not None:
                update_key[upsert_key] = doc[upsert_key]
            operations.append(UpdateOne(update_key, {'$set': doc}, upsert=True))
            logger.debug(f'[{threading.current_thread().name}] ({threading.current_thread().name}): Upsert document with _id: {doc["_id"]}')

        return operations, num_ids                
    
    def write_documents(self, i, operations, num_ids, db_name, collection_name):
        if operations:
            try:
                self.mongodb_service.coll_dst.bulk_write(operations)
                with self.mongodb_service.processed_docs_lock:
                    self.mongodb_service.processed_docs += num_ids
                #self.prometheus_service.sync_processed_docs_counter(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name).inc(num_ids)
            except Exception as e:
                #self.prometheus_service.sync_errors_counter(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, error_type='bulk_write')
                logger.error(f'[{threading.current_thread().name}] ({i}): ERROR in bulk_write: {e}')
                raise

    def log_and_sleep(self, i, num_ids, read_time, write_time, sleep_time, db_name, collection_name):
        progress = self.sync_status_progress().split('%')[0]
        logger.info(f'[{threading.current_thread().name}] ({i}): Fetched {num_ids} docs in {read_time}s. Written {num_ids} docs in {write_time}s. Progress: {progress}%')
        if write_time < read_time:
            if write_time * 2 < read_time:
                 read_sleep_time = random.uniform(read_time, read_time * 2)
            else:
                read_sleep_time = random.uniform(read_time + sleep_time, read_time * 2 + sleep_time)
            #self.prometheus_service.sync_sleep_time_gauge(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name).set(read_sleep_time)
            logger.debug(f"[{threading.current_thread().name}] ({i}): Read threshold exceeded, let's take a break for {round(read_sleep_time, 0)} seconds...")
            time.sleep(read_sleep_time)

    def sync_status_progress(self):
        total_docs_per_machine = self.mongodb_service.total_docs / self.mongodb_service.total_machines
        progress = round((self.mongodb_service.processed_docs / total_docs_per_machine) * 100, 2)
        progress_bar = '#' * int(progress) + '-' * (100 - int(progress))
        return f'{progress}% [{progress_bar}]'
    
    def process_batch(self, app, i, batch_size, db_name, collection_name, upsert_key=None):
        with app.app_context():
            sleep_time = self.calculate_sleep_time()
            #self.prometheus_service.sync_sleep_time_gauge(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name).set(sleep_time)
            logger.debug(f'[{threading.current_thread().name}] ({i}): Break time, drinking a cup of coffee. Wait me {round(sleep_time, 0)} seconds...')
            time.sleep(sleep_time)

            with self.mongodb_service.syncSrc.start_session() as session:
                cursor = None
                try:
                    start_time = time.time()
                    cursor = self.fetch_documents(i, batch_size, session)
                    operations, num_ids = self.build_operations(i, cursor, upsert_key)
                    end_time = time.time()
                    read_time = round(end_time - start_time, 1)
                    #self.prometheus_service.sync_read_time_histogram(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name).observe(read_time)
                    start_time = time.time()
                    self.write_documents(i, operations, num_ids, db_name, collection_name)
                    end_time = time.time()
                    write_time = round(end_time - start_time, 1)
                    #self.prometheus_service.sync_write_time_histogram(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name).observe(write_time)
                    self.log_and_sleep(i, num_ids, read_time, write_time, sleep_time, db_name, collection_name)
                except Exception as e:
                    #self.prometheus_service.sync_errors_counter(thread_name=threading.current_thread().name, db_name=db_name, collection_name=collection_name, error_type='fetch_write')
                    logger.error(f'[{threading.current_thread().name}] ({i}): ERROR: {e}')
                    raise
                finally:
                    if cursor:
                        cursor.close()  
                    session.end_session()
                    gc.collect()

    def process_batches(self, app, batch_size, start_batch, end_batch, db_name, collection_name, upsert_key=None):
        parent_batches = math.ceil(self.mongodb_service.total_docs / batch_size)
        last_processed_batch = 0
        if self.mongodb_service.total_docs > 0:
            last_processed_batch = math.floor((self.mongodb_service.processed_docs / self.mongodb_service.total_docs) * parent_batches)
        with ThreadPoolExecutor(max_workers=self.mongodb_service.max_workers) as executor:
            for i in range(start_batch, min(end_batch, parent_batches)):
                if i > last_processed_batch:
                    executor.submit(self.process_batch, app, i * batch_size, batch_size, db_name, collection_name, upsert_key)
        logger.debug(f'Processed up to batch {end_batch}')
