import time
import math
import random
import logging
import threading
import gc
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class MongoDBCollectionService:
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service

    def calculate_batch_size(self, total_docs):
        batch_size = math.ceil((int(total_docs * self.mongodb_service.percentage) // 100) / self.mongodb_service.max_workers)
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
            if upsert_key is not None:
                update_key[upsert_key] = doc[upsert_key]
            operations.append(UpdateOne(update_key, {'$set': doc}, upsert=True))
            logger.debug(f'[{threading.current_thread().name}] ({threading.current_thread().name}): Upsert document with _id: {doc["_id"]}')

        return operations, num_ids                
    
    def write_documents(self, i, operations, num_ids):
        if operations:
            try:
                self.mongodb_service.coll_dst.bulk_write(operations)
                with self.mongodb_service.processed_docs_lock:
                    self.mongodb_service.processed_docs += num_ids
            except Exception as e:
                logger.error(f'[{threading.current_thread().name}] ({i}): ERROR in bulk_write: {e}')
                raise

    def log_and_sleep(self, i, num_ids, read_time, write_time, sleep_time):
        progress = self.sync_status_progress().split('%')[0]
        logger.info(f'[{threading.current_thread().name}] ({i}): Fetched {num_ids} docs in {round(read_time, 3)}s. Written {num_ids} docs in {round(write_time, 3)}s. Progress: {progress}%')
        if write_time < read_time:
            if write_time * 2 < read_time:
                 read_sleep_time = random.uniform(read_time, read_time * 2)
            else:
                read_sleep_time = random.uniform(read_time + sleep_time, read_time * 2 + sleep_time)
            logger.debug(f"[{threading.current_thread().name}] ({i}): Read threshold exceeded, let's take a break for {round(read_sleep_time, 2)} seconds...")
            time.sleep(read_sleep_time)

    def sync_status_progress(self):
        total_docs_per_machine = self.mongodb_service.total_docs / self.mongodb_service.total_machines
        progress = round((self.mongodb_service.processed_docs / total_docs_per_machine) * 100, 2)
        progress_bar = '#' * int(progress) + '-' * (100 - int(progress))
        return f'{progress}% [{progress_bar}]'
    
    def process_batch(self, app, i, batch_size, upsert_key=None):
        with app.app_context():
            sleep_time = self.calculate_sleep_time()
            logger.info(f'[{threading.current_thread().name}] ({i}): Waking up, drinking a cup of coffee. Wait me {round(sleep_time, 1)} seconds...')
            time.sleep(sleep_time)

            with self.mongodb_service.syncSrc.start_session() as session:
                cursor = None
                try:
                    start_time = time.time()
                    cursor = self.fetch_documents(i, batch_size, session)
                    operations, num_ids = self.build_operations(i, cursor, upsert_key)
                    end_time = time.time()
                    read_time = round(end_time - start_time, 3)
                    start_time = time.time()
                    self.write_documents(i, operations, num_ids)
                    end_time = time.time()
                    write_time = round(end_time - start_time, 3)
                    self.log_and_sleep(i, num_ids, read_time, write_time, sleep_time)
                except Exception as e:
                    logger.error(f'[{threading.current_thread().name}] ({i}): ERROR: {e}')
                    raise
                finally:
                    if cursor:
                        cursor.close()  
                    session.end_session()
                    gc.collect()

    def process_batches(self, app, batch_size, start_batch, end_batch, upsert_key=None):
        parent_batches = math.ceil(self.mongodb_service.total_docs / batch_size)
        last_processed_batch = math.floor((self.mongodb_service.processed_docs / self.mongodb_service.total_docs) * parent_batches)
        with ThreadPoolExecutor(max_workers=self.mongodb_service.max_workers) as executor:
            for i in range(start_batch, min(end_batch, parent_batches)):
                if i > last_processed_batch:
                    executor.submit(self.process_batch, app, i * batch_size, batch_size, upsert_key)
        logger.info(f'Processed up to batch {end_batch}')
