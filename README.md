# ReplicaSyncAPI

ReplicaSyncAPI is a Python-based API that synchronizes data between two MongoDB replica sets. It reads data from one replica set, compares it with the data in the second replica set, and updates or inserts any differences into the second replica set.

## Getting Started

These instructions will guide you on how to get a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.8 or higher (3.8.10 recommended)
- MongoDB
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
MONGODB_CONNECTION_STRING_1=<your_connection_string_1>
MONGODB_CONNECTION_STRING_2=<your_connection_string_2>
```

4. Run the application:

```
python -m app/main
```

The application will start and wait for a /POST call in order to start synchronizing data between the two MongoDB replica sets.

5. Make a /POST call to the API
```
curl -X POST -H "Content-Type: application/json" -d '{"db_name": "<your_db_name>", "collection_name": "<your_collection_name>"}' http://127.0.0.1:5000/sync
```

## Running the tests

To run the tests, use the following command:

```
python -m unittest discover tests
```

## Built With

* [Python](https://www.python.org/)
* [MongoDB](https://www.mongodb.com/)

## Authors

* **XrPF**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
