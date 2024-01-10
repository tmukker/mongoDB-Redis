import redis
from pymongo.server_api import ServerApi
import logging
from pymongo import MongoClient, errors
import json
import numpy as np
import time
import statistics
import csv

# =========================== Reading from Redis ===========================
def readFromRedis(key):
    try:
        value = redis_client.get(key)
        if value:
            return json.loads(value.decode('utf-8'))
        else:
            return None
    except Exception as err_redis:
        logging.error("Error reading from Redis: {}".format(err_redis))


# =========================== Writing to Redis ===========================
def writeToRedis(key, value):
    try:
        redis_client.set(key, json.dumps(value))
    except Exception as err_redis:
        logging.error("Error writing to Redis: {}".format(err_redis))


# =========================== Writing to MongoDB ===========================
def writeToMongoDB(key, value, unique_field='title'):
    try:
        with MongoClient(mongo_uri, tls=True, tlsCertificateKeyFile='cert.pem',
                         server_api=ServerApi('1')) as mongodb_client:
            mongodb_db = mongodb_client['sample_mflix']

            if key in mongodb_db.list_collection_names():
                for document in value:
                    identifier = document.get(unique_field, 'default_identifier')
                    filter_criteria = {unique_field: identifier}
                    update_data = {'$set': document}
                    mongodb_db[key].update_one(filter_criteria, update_data, upsert=True)
            else:
                mongodb_collection = mongodb_db[key]
                if isinstance(value, str):
                    value = json.loads(value)
                mongodb_collection.insert_many(value)
    except errors.PyMongoError as err_mongo:
        logging.error(f"Error writing to MongoDB for key {key}: {err_mongo}")


# =========================== Cleaning JSON String ===========================
def clean_json(data):
    data = data.replace("'", '"')
    data = data.replace("\\", "")
    return data


# =========================== Query ===========================
def query_top_n(n, from_year, to_year):
    try:
        key = f"top-{n}-{from_year}-{to_year}"
        value = readFromRedis(key)

        if value:
            # print("Querying Redis")
            logging.info("Querying Redis")
            result = value
        else:
            # print("Querying MongoDB")
            logging.info("Querying MongoDB")
            pipeline = [
                {"$match": {"year": {"$gte": from_year, "$lte": to_year}}},
                {"$group": {"_id": {"title": "$title", "year": "$year"}, "comment_count": {"$sum": 1}}},
                {"$project": {"_id": 0, "title": "$_id.title", "year": "$_id.year", "comment_count": 1}},
                {"$sort": {"comment_count": -1}},
                {"$limit": n}
            ]
            cursor = mongo_db.movies.aggregate(pipeline)
            result = json.loads(json.dumps(list(cursor)))
            writeToRedis(key, result)
            writeToMongoDB(key, result)

        return result
    except Exception as err:
        logging.error("Error querying MongoDB: {}".format(err))


# =========================== Test Case 1 - Empty Redis ===========================
def test_case_1(top_n, from_year, to_year):
    response_times = []

    for _ in range(100):
        # Clear Redis
        redis_client.flushdb()

        # Measure query time
        start_time_query = time.time()
        result = query_top_n(top_n, from_year, to_year)
        end_time_query = time.time()
        query_time = end_time_query - start_time_query

        response_times.append(query_time)

    # Calculate statistics
    avg_response_time = statistics.mean(response_times)
    percentile_50 = np.percentile(response_times, 50)
    percentile_90 = np.percentile(response_times, 90)

    print("\nPerformance Measurement (Empty Redis):")
    print(f"Average Response Time: {avg_response_time} seconds")
    print(f"50th Percentile Response Time: {percentile_50} seconds")
    print(f"90th Percentile Response Time: {percentile_90} seconds")

    # Save response times to CSV
    csv_file_path = "test_case_1_results.csv"
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Response Time"])
        writer.writerows([[t] for t in response_times])

    print(f"Response times saved to {csv_file_path}")

# =========================== Test Case 2 - Redis with Data ===========================
def test_case_2(top_n, from_year, to_year):
    response_times = []

    for _ in range(100):
        # Measure query time
        start_time_query = time.time()
        result = query_top_n(top_n, from_year, to_year)
        end_time_query = time.time()
        query_time = end_time_query - start_time_query

        response_times.append(query_time)

    # Calculate statistics
    avg_response_time = statistics.mean(response_times)
    percentile_50 = np.percentile(response_times, 50)
    percentile_90 = np.percentile(response_times, 90)

    print("\nPerformance Measurement (Redis with Data):")
    print(f"Average Response Time: {avg_response_time} seconds")
    print(f"50th Percentile Response Time: {percentile_50} seconds")
    print(f"90th Percentile Response Time: {percentile_90} seconds")

    # Save response times to CSV
    csv_file_path = "test_case_2_results.csv"
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Response Time"])
        writer.writerows([[t] for t in response_times])  # Updated variable name

    print(f"Response times saved to {csv_file_path}")

if __name__ == '__main__':
    print("Running App.....\n")

    # =========================== Connecting to MongoDB ===========================
    try:
        mongo_uri = ("MONGODB_URI")
        mongo_client = MongoClient(mongo_uri, tls=True, tlsCertificateKeyFile='cert.pem', server_api=ServerApi('1'))
        mongo_db = mongo_client['sample_mflix']
        logging.info('Connected to MongoDB')
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")

    # =========================== Connecting to Redis ===========================
    try:
        redis_host = 'REDIS_HOST'
        redis_port = 'REDIS_PORT'
        redis_password = 'REDIS_PASSWORD'

        if redis_password:
            redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
            logging.info("Connected to Redis")
        else:
            logging.error("Supply a password for Redis")
    except Exception as e:
        logging.error("Error connecting to Redis: {}".format(e))

        # Get inputs from user
    top_n = int(input("Enter the Number top n: "))
    t_from = int(input("From: "))
    t_to = int(input("To: "))

    # Run test cases
    print("\nTest Case 1: Redis is empty")
    test_case_1(top_n, t_from, t_to)

    print("\nTest Case 2: Redis contains query results")
    test_case_2(top_n, t_from, t_to)
