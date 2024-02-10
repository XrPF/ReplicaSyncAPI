import gc
import os
import time
import psutil
import random
import logging
import threading
import concurrent.futures
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class MongoDBService:
    def __init__(self):
        load_dotenv()
        uri1 = os.getenv('MONGO_CONNECTION_STRING_1')
        uri2 = os.getenv('MONGO_CONNECTION_STRING_2')
        self.client1 = MongoClient(uri1)
        self.client2 = MongoClient(uri2)

    def get_collection(self, db_name, collection_name, client):
        db = client[db_name]
        collection = db[collection_name]
        return collection
    
    def memory_usage_psutil(self):
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / float(2 ** 20)
        return max(mem, 0)
    
    def garbage_collect(self, thread_name, i, sleep_time, mem_threshold):
        mem_usage = self.memory_usage_psutil()
        if mem_usage > mem_threshold:
            logger.info(f'[{thread_name}] ({i}): Memory usage is {round(mem_usage, 3)} MB. Garbage collecting...')
            gc.collect()
            logger.info(f'[{thread_name}] ({i}): Garbage collected. Sleeping for {sleep_time} seconds...')
            time.sleep(sleep_time)

    def process_batch(self, i, batch_size, coll_src, coll_dest, upsert_key=None):
        logger.debug(f'[{threading.current_thread().name}] ({i}): Start batch')
        start_time = time.time()
        start_mem = self.memory_usage_psutil()
        cursor = coll_src.find().skip(i).limit(batch_size)        
        operations = []
        num_ids = 0

        for doc in cursor:
            num_ids += 1
            update_key = {'_id': doc['_id']}
            if upsert_key is not None:
                update_key[upsert_key] = doc[upsert_key]
            operations.append(UpdateOne(update_key, {'$set': doc}, upsert=True))
            logger.debug(f'[{threading.current_thread().name}] ({i}): Upsert document with _id: {doc["_id"]}')
        
        if operations:
            try:
                coll_dest.bulk_write(operations)
            except Exception as e:
                logger.error(f'[{threading.current_thread().name}] ({i}): ERROR in bulk_write: {e}')
            else:
                end_time = time.time()
                logger.info(f'[{threading.current_thread().name}] ({i}): Document upserts: {len(operations)} in {round(end_time - start_time, 3)} seconds')
        else:
            end_time = time.time()
            logger.info(f'[{threading.current_thread().name}] ({i}): Document upserts: 0 in {round(end_time - start_time, 3)} seconds')

        operations.clear()
        end_mem = self.memory_usage_psutil()
        logger.info(f'[{threading.current_thread().name}] ({i}): Memory usage: {round(end_mem - start_mem, 3)} MB')
        return threading.current_thread().name, i

    def compare_and_update(self, db_name, collection_name, batch_num=None, upsert_key=None):
        total_memory = psutil.virtual_memory().total / float(1024 ** 2)
        base_memory = 0.4 / 100 * total_memory
        available_memory = total_memory - base_memory
        memory_per_worker = 0.2 / 100 * total_memory
        max_workers = int(available_memory / memory_per_worker)
        mem_threshold = memory_per_worker 
        percentage = 0.1
        coll_src = self.get_collection(db_name, collection_name, self.client1)
        coll_dest = self.get_collection(db_name, collection_name, self.client2)

        logger.info(f'Looking for documents in {db_name}.{collection_name}')

        total_docs = coll_src.estimated_document_count()

        logger.info(f'Sync started for database {db_name}: {total_docs} estimated total documents in collection {collection_name}')

        if batch_num:
            percentage = batch_num

        batch_size = (int(total_docs * percentage) // 100) // max_workers

        logger.info(f'Initializating {max_workers} maximum threads with batch size of {batch_size} documents each')


        start_time = time.time()
        with ThreadPoolExecutor(max_workers) as executor:
            futures = []
            for i in range(0, total_docs, batch_size):
                future = executor.submit(self.process_batch, i, batch_size, coll_src, coll_dest, upsert_key)
                futures.append(future)
                self.garbage_collect(threading.current_thread().name, i, random.randint(90, 120), mem_threshold)
                
            while True:
                future = executor.submit(self.process_batch, i, batch_size, coll_src, coll_dest, upsert_key)
                if future.result() is None:
                    break
                futures.append(future)
                i += batch_size
                thread_name, i = future.result()
                self.garbage_collect(threading.current_thread().name, i, random.randint(90,120), mem_threshold)

            for future in concurrent.futures.as_completed(futures):
                if future.result() is not None:
                    thread_name, i = future.result()
                    logger.debug(f'[{thread_name}] ({i}): End batch')

        end_time = time.time()
        logger.info(f'Sync ended for {db_name}.{collection_name} in {round(end_time - start_time, 3)} seconds')
