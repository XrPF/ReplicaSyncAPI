import gc
import os
import time
import math
import psutil
import random
import logging
import threading
import concurrent.futures
from dotenv import load_dotenv
from multiprocessing import Manager
from pymongo import MongoClient, UpdateOne
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class MongoDBService:
    def __init__(self):
        load_dotenv()
        # MongoDB Connection
        uri1 = os.getenv('MONGO_CONNECTION_STRING_1')
        uri2 = os.getenv('MONGO_CONNECTION_STRING_2')
        if os.getenv('DB_NAME') is not None and os.getenv('COLLECTION_NAME') is not None:
            self.db_name = os.getenv('DB_NAME')
            self.collection_name = os.getenv('COLLECTION_NAME')
        self.syncSrc = MongoClient(uri1)
        self.syncDst = MongoClient(uri2)
        if self.db_name is not None and self.collection_name is not None:
            self.coll_src = self.get_collection(self.db_name, self.collection_name, self.syncSrc)
            self.coll_dst = self.get_collection(self.db_name, self.collection_name, self.syncDst)
        # Memory Management
        total_memory = psutil.virtual_memory().total / float(1024 ** 2)
        base_memory = self.memory_usage_psutil()
        available_memory = total_memory - base_memory
        memory_per_worker = (5 / 100 ) * total_memory
        self.max_workers = int(available_memory / memory_per_worker)
        self.mem_threshold = 0.4 * total_memory
        logger.info(f'Memory Management: Total {total_memory} MB || Base {base_memory} MB || Avail {available_memory}')
        logger.info(f'Memory Management: Max workers {self.max_workers} || Mem threshold {self.mem_threshold} || Mem x worker {memory_per_worker}')
        # Percentage of documents to be processed
        self.percentage = 0.2
        self.total_docs = 0
        self.processed_docs = 0
        self.processed_docs_lock = threading.Lock()
        # Time Management
        self.percentage_diff_threshold = 20

    def get_collection(self, db_name, collection_name, client):
        db = client[db_name]
        collection = db[collection_name]
        return collection
    
    def close_connections(self):
        self.syncSrc.close()
        self.syncDst.close()
    
    def recycle_connections(self):
        self.close_connections()
        gc.collect()
        self.coll_src = self.get_collection(self.db_name, self.collection_name, self.syncSrc)
        self.coll_dst = self.get_collection(self.db_name, self.collection_name, self.syncDst)
        sleep_time = random.uniform(2 * self.max_workers, 3 * self.max_workers)
        logger.info(f'Recycling connections. Sleeping for {sleep_time} seconds...')
        time.sleep(sleep_time)


    def memory_usage_psutil(self):
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / float(2 ** 20)
        return max(mem, 0)
    
    def garbage_collect(self, thread_name, i, sleep_time):
        mem_usage = self.memory_usage_psutil()
        logger.info(f'[{thread_name}] ({i}): Memory usage is {round(mem_usage, 3)} MB')
        if mem_usage > self.mem_threshold:
            logger.info(f'[{thread_name}] ({i}): Memory usage is {round(mem_usage, 3)} MB. Garbage collecting...')
            self.close_connections()
            gc.collect()
            logger.info(f'[{thread_name}] ({i}): Garbage collected. Sleeping for {sleep_time} seconds...')
            time.sleep(sleep_time)
    
    def process_batch(self, i, batch_size, batch_file, upsert_key=None):
        logger.debug(f'[{threading.current_thread().name}] ({i}): Start batch')
        coll_src = self.coll_src
        coll_dest = self.coll_dst
        operations = []
        num_ids = 0
        sleep_time = random.uniform(self.max_workers // 2, self.max_workers)
        logger.debug(f'[{threading.current_thread().name}] ({i}): Sleeping for {round(sleep_time, 1)} seconds...')
        time.sleep(sleep_time)
        start_time = time.time()

        with self.syncSrc.start_session() as session:
            cursor = coll_src.find(session=session, no_cursor_timeout=True).sort('_id', 1).skip(i).limit(batch_size)        
            try:
                for doc in cursor:
                    num_ids += 1
                    update_key = {'_id': doc['_id']}
                    if upsert_key is not None:
                        update_key[upsert_key] = doc[upsert_key]
                    operations.append(UpdateOne(update_key, {'$set': doc}, upsert=True))
                    logger.debug(f'[{threading.current_thread().name}] ({i}): Upsert document with _id: {doc["_id"]}')
                end_time = time.time()
                read_time = round(end_time - start_time, 3)
                logger.debug(f'[{threading.current_thread().name}] ({i}): Fetched {num_ids} documents in {read_time} seconds')

                if operations:
                    try:
                        start_time = time.time()
                        coll_dest.bulk_write(operations)
                        with open(batch_file, 'a') as f:
                            f.write(f'{i}\n')
                        with self.processed_docs_lock:
                            self.processed_docs += num_ids
                    except Exception as e:
                        logger.error(f'[{threading.current_thread().name}] ({i}): ERROR in bulk_write: {e}')
                    else:
                        end_time = time.time()
                        write_time = round(end_time - start_time, 3)
                        logger.debug(f'[{threading.current_thread().name}] ({i}): Document upserts: {len(operations)} in {write_time} seconds')
                else:
                    end_time = time.time()
                    logger.debug(f'[{threading.current_thread().name}] ({i}): Document upserts: 0 in {round(end_time - start_time, 3)} seconds')
            finally:
                cursor.close()
                session.end_session()
                time.sleep(round(sleep_time, 1) / (3/2))
                percent_diff = (abs(read_time - write_time) / min(read_time, write_time)) * 100
                logger.info(f'[{threading.current_thread().name}] ({i}): Read time: {read_time} seconds || Write time: {write_time} seconds || Percent diff: {round(percent_diff, 2)}%')
                if write_time < read_time:
                    if percent_diff > self.percentage_diff_threshold:
                        if math.floor(read_time) > 0:
                            num_digits = math.floor(math.log10(math.floor(read_time))) + 1
                        else:
                            num_digits = 1
                        logger.warn(f"[{threading.current_thread().name}] ({i}): Threshold exceeded, let's take a break for {num_digits * round(sleep_time, 1)} seconds...")
                        time.sleep(num_digits * sleep_time)

    def calculate_batch_size(self, total_docs):
        return math.ceil((int(total_docs * self.percentage) // 100) / self.max_workers)
    
    def get_processed_batches(self, batch_file):
        manager = Manager()
        processed_batches = manager.list()
        if os.path.exists(batch_file):
            with open(batch_file, 'r') as f:
                processed_batches.extend([int(line.strip()) for line in f])
        return processed_batches

    def process_batches(self, batch_size, batch_file, upsert_key=None):
        parent_batches = math.ceil(self.total_docs / batch_size)
        batches_per_group = math.ceil(parent_batches / self.max_workers)
        logger.info(f'Processing {batches_per_group} with {self.max_workers} workers and batch size is {batch_size}')

        last_processed_batch = -1
        if os.path.exists(batch_file):
            with open(batch_file, 'r') as f:
                last_processed_batch = int(f.read().strip())

        for group_start in range(0, parent_batches, batches_per_group):
            with ThreadPoolExecutor(self.max_workers) as executor:
                for i in range(group_start, min(group_start + batches_per_group, parent_batches)):
                    if i > last_processed_batch:
                        executor.submit(self.process_batch, i * batch_size, batch_size, batch_file, upsert_key)

            logger.info(f'Processed up to batch {last_processed_batch}')

    def compare_and_update(self, db_name=None, collection_name=None, upsert_key=None):
        if db_name is not None and collection_name is not None:
            self.db_name = db_name
            self.collection_name = collection_name
            self.coll_src = self.get_collection(db_name, collection_name, self.syncSrc)
            self.coll_dst = self.get_collection(db_name, collection_name, self.syncDst)

        batch_file=f'/tmp/{self.db_name}_{self.collection_name}_batch.txt'

        logger.info(f'Looking for documents in {self.db_name}.{self.collection_name}')

        total_docs = self.syncSrc[self.db_name][self.collection_name].estimated_document_count()

        logger.info(f'Sync started for database {self.db_name}: {total_docs} estimated total documents in collection {self.collection_name}')

        # Set the total_docs and processed_docs
        self.total_docs = total_docs
        self.processed_docs = 0
        batch_size = self.calculate_batch_size(total_docs)
        start_time = time.time()
        self.process_batches(batch_size, batch_file, upsert_key)
        end_time = time.time()
        logger.info(f'Sync ended for {self.db_name}.{self.collection_name} in {round(end_time - start_time, 3)} seconds')

        if os.path.exists(batch_file):
            os.remove(batch_file)

        logger.info(f'Cleaned up {batch_file}. Closed connections to databases and exiting...')

    def sync_status_progress(self):
        progress = round((self.processed_docs / self.total_docs) * 100, 2)
        progress_bar = '#' * int(progress / 100) + '-' * (100 - int(progress / 100))
        return f'{progress}% [{progress_bar}]'