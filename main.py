import json
import redis
from pymongo import MongoClient
from urllib.parse import quote_plus
import logging

class MovieQueryFacade:
    def __init__(self, redis_host, redis_port, redis_password, mongo_uri, db_name="sample_mflix"):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.mongo_uri = mongo_uri
        self.db_name = db_name

        self.redis_client = None
        self.mongo_client = None
        self.db = None

        self._connect_to_redis()
        self._connect_to_mongo()

    def _connect_to_redis(self):
        try:
            # Connect to Redis
            self.redis_client = redis.StrictRedis(
                host=self.redis_host, port=self.redis_port, password=self.redis_password, decode_responses=True
            )
            logging.info("Connected to Redis")
        except Exception as e:
            logging.error(f"Error connecting to Redis: {e}")

    def _connect_to_mongo(self):
        try:
            # Connect to MongoDB with username and password
            mongo_uri_with_auth = self.mongo_uri.format(
                username=quote_plus(""),  # replace with your MongoDB username
                password=quote_plus("")   # replace with your MongoDB password
            )
            self.mongo_client = MongoClient(mongo_uri_with_auth)
            self.db = self.mongo_client[self.db_name]
            logging.info(f"Connected to MongoDB. Using database: {self.db_name}")
        except Exception as e:
            logging.error(f"Error connecting to MongoDB: {e}")

    def query_top_n(self):
        try:
            top_n = int(input("Enter the value for top_n: "))
            from_year = int(input("Enter the starting year: "))
            to_year = int(input("Enter the ending year: "))
        except ValueError:
            logging.error("Invalid input. Please enter valid integers.")
            return None

        if self.db is None:
            logging.error("MongoDB connection not established.")
            return None

        try:
            # Check Redis for existing data
            redis_key = f"top-{top_n}-{from_year}-{to_year}"
            redis_data = self.redis_client.get(redis_key)

            if redis_data:
                # If data exists in Redis, return it
                return redis_data
            else:
                # Data doesn't exist in Redis, develop MongoDB aggregation pipeline
                pipeline = [
                    {"$match": {"year": {"$gte": from_year, "$lte": to_year}}},
                    {"$unwind": "$comments"},
                    {"$group": {
                        "_id": "$_id",
                        "title": {"$first": "$title"},
                        "comment_count": {"$sum": 1}  # Counting comments
                    }},
                    {"$sort": {"comment_count": -1}},
                    {"$limit": top_n},
                    {"$project": {"_id": 0, "name": "$title", "comment_count": 1}}
                ]

                # Execute aggregation pipeline in MongoDB
                result_from_mongo = list(self.db.movies.aggregate(pipeline))

                # Convert result to JSON format
                json_result = json.dumps(result_from_mongo, default=str)

                # Save result to Redis for future retrieval
                self.redis_client.set(redis_key, json_result)

                # Save result to MongoDB collection with a dynamic name
                collection_name = f"top-{top_n}-{from_year}-{to_year}"
                self.db[collection_name].insert_many(result_from_mongo)
            return json_result
        except Exception as e:
            logging.error(f"Error querying MongoDB: {e}")
            return None

    # Configure logging
logging.basicConfig(level=logging.INFO)
# Example usage:
redis_host = ''
redis_port = 12719  # Default Redis port
redis_password = ''
mongo_uri = 'mongodb+srv://{username}:{password}@cluster.64elgvb.mongodb.net/sample_mflix?retryWrites=true&w=majority'

facade = MovieQueryFacade(redis_host, redis_port, redis_password, mongo_uri)
result = facade.query_top_n()

print(result)
