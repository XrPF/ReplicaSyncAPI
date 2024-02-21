import time
import math
import random
import logging
import threading
import gc
import tracemalloc
import objgraph
from flask import current_app
from memory_profiler import profile
from contextlib import closing
from multiprocessing import Manager
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class MongoDBCollectionService:
    def __init__(self, mongodb_service):
        self.mongodb_service = mongodb_service

    def calculate_batch_size(self, total_docs):
        return math.ceil((int(total_docs * self.mongodb_service.percentage) // 100) / self.mongodb_service.max_workers)
    
    def calculate_sleep_time(self):
        base_sleep_time = min(self.mongodb_service.max_workers, 60)
        return random.uniform((base_sleep_time / 2) / self.mongodb_service.total_machines, base_sleep_time)
    
    def fetch_documents(self, i, batch_size, session):
#        with current_app.app_context():
        tracemalloc.start()
        result = self.mongodb_service.coll_src.find(session=session, no_cursor_timeout=True).sort('_id', 1).skip(i).limit(batch_size)
       
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        with open(f'/var/log/ReplicaSyncAPI/tracemalloc_fetch_documents_{threading.current_thread().name}.log', 'w') as file:
            for stat in top_stats[:10]:
                file.write(str(stat))
                file.write('\n')

        with open(f'/var/log/ReplicaSyncAPI/objgraph_fetch_documents_{threading.current_thread().name}.log', 'w') as file:
            objgraph.show_most_common_types(limit=10, file=file)

        return result
    
    def build_operations(self, i, cursor, upsert_key):
#        with current_app.app_context():
        tracemalloc.start()
        operations = []
        num_ids = 0
        for doc in cursor:
            num_ids += 1
            update_key = {'_id': doc['_id']}
            if upsert_key is not None:
                update_key[upsert_key] = doc[upsert_key]
            operations.append(UpdateOne(update_key, {'$set': doc}, upsert=True))
            logger.debug(f'[{threading.current_thread().name}] ({threading.current_thread().name}): Upsert document with _id: {doc["_id"]}')

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        with open(f'/var/log/ReplicaSyncAPI/tracemalloc_build_operations_{threading.current_thread().name}.log', 'w') as file:
            for stat in top_stats[:10]:
               file.write(str(stat))
               file.write('\n')

        with open(f'/var/log/ReplicaSyncAPI/objgraph_build_operations_{threading.current_thread().name}.log', 'w') as file:
            objgraph.show_most_common_types(limit=10, file=file)

        return operations, num_ids                
    
    def write_documents(self, i, operations, num_ids):
        tracemalloc.start()
        if operations:
            try:
                self.mongodb_service.coll_dst.bulk_write(operations)
                with self.mongodb_service.processed_docs_lock:
                    self.mongodb_service.processed_docs += num_ids
            except Exception as e:
                logger.error(f'[{threading.current_thread().name}] ({i}): ERROR in bulk_write: {e}')
                raise

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        with open(f'/var/log/ReplicaSyncAPI/tracemalloc_write_documents_{threading.current_thread().name}.log', 'w') as file:
            for stat in top_stats[:10]:
                file.write(str(stat))
                file.write('\n')

        with open(f'/var/log/ReplicaSyncAPI/objgraph_write_documents_{threading.current_thread().name}.log', 'w') as file:
            objgraph.show_most_common_types(limit=10, file=file)

        with open(f'/var/log/ReplicaSyncAPI/gc_write_documents_{threading.current_thread().name}.log', 'w') as file:
            for obj in gc.get_objects():
                file.write(str(obj))
                file.write('\n')

    def log_and_sleep(self, i, num_ids, read_time, write_time, sleep_time):
        progress = self.sync_status_progress().split('%')[0]
        logger.debug(f'[{threading.current_thread().name}] ({i}): Fetched {num_ids} docs in {round(read_time, 3)}s. Written {num_ids} docs in {round(write_time, 3)}s. Progress: {progress}%')
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
    
    @profile
    def process_batch(self, app, i, batch_size, upsert_key=None):
        with app.app_context():
            tracemalloc.start()
            sleep_time = self.calculate_sleep_time()
            logger.debug(f'[{threading.current_thread().name}] ({i}): Waking up, drinking a cup of coffee. Wait me {round(sleep_time, 1)} seconds...')
            time.sleep(sleep_time)

            with self.mongodb_service.syncSrc.start_session() as session:
                cursor = None
                try:
                    start_time = time.time()
                    cursor = self.fetch_documents(i, batch_size, session)
                    with closing([]) as operations:
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

            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            with open(f'/var/log/ReplicaSyncAPI/tracemalloc_process_batch_{threading.current_thread().name}.log', 'w') as file:
                for stat in top_stats[:10]:
                    file.write(str(stat))
                    file.write('\n')

            with open(f'/var/log/ReplicaSyncAPI/objgraph_process_batch_{threading.current_thread().name}.log', 'w') as file:
                objgraph.show_most_common_types(limit=10, file=file)

    @profile
    def process_batches(self, app, batch_size, start_batch, end_batch, upsert_key=None):
        tracemalloc.start()
        parent_batches = math.ceil(self.mongodb_service.total_docs / batch_size)
        last_processed_batch = math.floor((self.mongodb_service.processed_docs / self.mongodb_service.total_docs) * parent_batches)
        with ThreadPoolExecutor(max_workers=self.mongodb_service.max_workers) as executor:
            for i in range(start_batch, min(end_batch, parent_batches)):
                if i > last_processed_batch:
                    executor.submit(self.process_batch, app, i * batch_size, batch_size, upsert_key)
        logger.debug(f'Processed up to batch {end_batch}')

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        with open('/var/log/ReplicaSyncAPI/tracemalloc_process_batches.log', 'w') as file:
            for stat in top_stats[:10]:
                file.write(str(stat))
                file.write('\n')

        with open('/var/log/ReplicaSyncAPI/objgraph_process_batches.log', 'w') as file:
            objgraph.show_most_common_types(limit=10, file=file)