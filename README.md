# ReplicaSyncAPI

ReplicaSyncAPI is a Python-based API that synchronizes data between two MongoDB replica sets. It reads data from source replica set and upserts the documents to the dest replica set.

## Getting Started

These instructions will guide you on how to get a copy of the project up and running on your local machine for development and testing purposes.

Current version: beta-2.0

### Prerequisites

- Python 3.8 or higher (3.8.10 recommended)
- MongoDB 4.4 tested (2 replicasets || 1 replicaset 1 mongo-s || 1 replicaset 1 standalone should work)
- pip 20.0.2 or higher (Python package installer)

### Installation

1. Clone the repository:

```
git clone https://github.com/XrPF/ReplicaSyncAPI.git
```

2. Navigate into the cloned project directory:

```
cd ReplicaSyncAPI
```
3. Install the required Python dependencies (optional - use a virtualenv):

```
pip install -r requirements.txt
```

4. Set up your environment variables in the .env file:

```
ROLE="worker" # master or worker in cluster mode, standalone in standalone mode
VM_WORKER_LIST=<your_host_running_replicaSyncApi> # Comma-sepparated of host:port list
MACHINE_ID=<incremental_number_to_id_machine> # From 01 to N (workers only)
# MongoDB
MONGO_USER=<your_mongo_user>
MONGO_PASSWORD=<your_mongo_password>
MONGO_HOSTS_1=<your_mongo_hosts_1>
MONGO_HOSTS_2=<your_mongo_hosts_2>
MONGO_OPTS_1=<your_mongo_options_1>
MONGO_OPTS_2=<your_mongo_options_2>
# Optional
MONGO_CONNECTION_STRING_1=<your_connection_string_1> # Instead of the previous MONGO_HOSTS_1 and MONGO_OPTS_1
MONGO_CONNECTION_STRING_2=<your_connection_string_2> # Instead of the previous MONGO_HOSTS_2 and MONGO_OPTS_2
DB_NAME=<your_db_name>
COLLECTION_NAME=<your_collection_name>
MAX_WORKERS=<max_workers_to_parallel_sync>
PERCENTAGE=<percentage_of_documents_per_batch>
```
5. Run the application:

```
python -m app.main
```

The application will start and wait for a /POST call in order to start synchronizing data between the two MongoDB replica sets.

6. Make a /POST call to the API:
```
# Just sync in batches <your_db_name>.<your_collection_name>
curl -X POST -H "Content-Type: application/json" -d '{"db_name": "<your_db_name>", "collection_name": "<your_collection_name>"}' http://127.0.0.1:2717/sync

# Just replicate (like will do a "normal" SECONDARY node in a replica set):
curl -X POST -H "Content-Type: application/json" -d '{"db_name": "<your_db_name>", "collection_name": "<your_collection_name>"}' http://127.0.0.1:2717/replicate

# Kill the replication progress:
curl -X POST http://127.0.0.1:2717/killReplica

# Both replicate and sync in batches in parallel multi-thread processing
curl -X POST -H "Content-Type: application/json" -d '{"db_name": "<your_db_name>", "collection_name": "<your_collection_name>"}' http://127.0.0.1:2717/fullSync
```
Note: running on localhost/standalone mode target your IP or the IP where the API is running on. When running on cluster mode, you must target master only, it will wake up the lazy workers for you, no need to call them all.

Also you can /GET the current running task with the next endpoint. If running on cluster mode, target master IP/hostname/url :
```
curl http://127.0.0.1:2717/status
```

## Running the tests

Currently working on it.

```
Coming soon...
```

## Built With

* [Python](https://www.python.org/)
* [MongoDB](https://www.mongodb.com/)

## Authors

* **XrPF**

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Disclaimer

Please note that ReplicaSyncAPI is currently in beta. While we are constantly working to improve and expand its capabilities, it may not be fully stable and could potentially cause issues with your MongoDB instances.

ReplicaSyncAPI is provided "as is", without warranty of any kind, express or implied. In no event shall the authors or copyright holders be liable for any claim, damages or other liability, whether in an action of contract, tort or otherwise, arising from, out of or in connection with the software or the use or other dealings in the software.

Use of ReplicaSyncAPI is at your own risk and discretion. We highly recommend thoroughly testing it in a controlled environment before using it in production. Always ensure you have up-to-date backups of your data.
