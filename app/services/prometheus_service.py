from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.multiprocess import MultiProcessCollector
from prometheus_client.core import CollectorRegistry

class PrometheusService:
    _instance = None

    @staticmethod
    def getInstance():
        if PrometheusService._instance == None:
            PrometheusService()
        return PrometheusService._instance

    def __init__(self):
        if PrometheusService._instance != None:
            raise Exception("This class is a singleton!")
        else:
            PrometheusService._instance = self

            # Create a new CollectorRegistry instance.
            self.registry = CollectorRegistry()

            # Create a new MultiProcessCollector instance.
            MultiProcessCollector(self.registry)

            # Stream service metrics (replica real-time synchronization)
            self.stream_service_operations_counter = Counter('replica_sync_api_stream_operation_count', 'Counter for the stream service operations', ['thread_name', 'db_name', 'collection_name', 'operation'], registry=self.registry)
            self.stream_active_threads_gauge = Gauge('replica_sync_api_stream_active_threads_gauge', 'Gauge for the active threads in the API', ['thread_name'], registry=self.registry)
            self.stream_errors_counter = Counter('replica_sync_api_stream_errors_count', 'Counter for the number of errors', ['thread_name', 'db_name', 'collection_name', 'error_type'], registry=self.registry)
            self.stream_replication_latency_histogram = Histogram('replica_sync_api_stream_latency', 'Latency of replication operations (histogram)', ['thread_name', 'db_name', 'collection_name', 'operation'], registry=self.registry)
            # Sync service metrics (batch data synchronization)
            self.sync_processed_docs_counter = Counter('replica_sync_api_sync_processed_docs_count', 'Counter for the number of processed documents', ['thread_name', 'db_name', 'collection_name'], registry=self.registry)
            self.sync_read_time_histogram = Histogram('replica_sync_api_sync_read_time', 'Time to read documents (histogram)', ['thread_name', 'db_name', 'collection_name'], registry=self.registry)
            self.sync_write_time_histogram = Histogram('replica_sync_api_sync_write_time', 'Time to write documents (histogram)', ['thread_name', 'db_name', 'collection_name'], registry=self.registry)
            self.sync_sleep_time_gauge = Gauge('replica_sync_api_sync_sleep_time', 'Gauge for time to sleep between batches', ['thread_name', 'db_name', 'collection_name'], registry=self.registry)
            self.sync_errors_counter = Counter('replica_sync_api_sync_errors', 'Counter for the number of errors', ['thread_name', 'error_type', 'db_name', 'collection_name'], registry=self.registry)

    def increment_stream_service_counter(self, thread_name, db_name, collection_name, operation):
        self.stream_service_operations_counter.labels(thread_name=thread_name, db_name=db_name, collection_name=collection_name, operation=operation).inc()
    
    def increment_stream_service_errors(self, thread_name, db_name, collection_name, error_type):
        self.stream_errors_counter.labels(thread_name=thread_name, db_name=db_name, collection_name=collection_name, error_type=error_type).inc()
    
    def set_stream_active_threads(self, thread_name, value):
        self.stream_active_threads_gauge.labels(thread_name=thread_name).set(value)
    
    def observe_stream_replication_latency(self, thread_name, db_name, collection_name, operation, value):
        self.stream_replication_latency_histogram.labels(thread_name=thread_name, db_name=db_name, collection_name=collection_name, operation=operation).observe(value)

    def increment_sync_processed_docs_counter(self, thread_name, db_name, collection_name, value):
        self.sync_processed_docs_counter.labels(thread_name=thread_name, db_name=db_name, collection_name=collection_name).inc(value)

    def observe_sync_read_time_histogram(self, thread_name, db_name, collection_name, value):
        self.sync_read_time_histogram.labels(thread_name=thread_name, db_name=db_name, collection_name=collection_name).observe(value)

    def observe_sync_write_time_histogram(self, thread_name, db_name, collection_name, value):
        self.sync_write_time_histogram.labels(thread_name=thread_name, db_name=db_name, collection_name=collection_name).observe(value)

    def set_sync_sleep_time_gauge(self, thread_name, db_name, collection_name, value):
        self.sync_sleep_time_gauge.labels(thread_name=thread_name, db_name=db_name, collection_name=collection_name).set(value)

    def increment_sync_errors_counter(self, thread_name, error_type, db_name, collection_name):
        self.sync_errors_counter.labels(thread_name=thread_name, error_type=error_type, db_name=db_name, collection_name=collection_name).inc()