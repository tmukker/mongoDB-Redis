import redis
from pymongo.server_api import ServerApi
import logging
from pymongo import MongoClient, errors
import json
import numpy as np
import time
import statistics
import csv

if __name__ == '__main__':
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
            with MongoClient(mongo_uri, tls=True, tlsCertificateKeyFile='YOUR_CERTIFICATE.pem',
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
            start_time = time.time()
            key = f"top-{n}-{from_year}-{to_year}"
            value = readFromRedis(key)

            if value:
                print("Querying Redis")
                logging.info("Querying Redis")
                writeToMongoDB(key, value)
                result = value
            else:
                print("Querying MongoDB")
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

            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Query Execution Time: {execution_time} seconds")
            return result, execution_time
        except Exception as err:
            logging.error("Error querying MongoDB: {}".format(err))

    # =========================== Test Case 1 - Empty Redis ===========================
    def performance_test_case_1(query_function, n, from_year, to_year, trials):
        execution_times = []

        for _ in range(trials):
            redis_client.flushall()  # Clear Redis
            result, execution_time = query_function(n, from_year, to_year)
            execution_times.append(execution_time)

        avg_time = statistics.mean(execution_times)
        percentile_50 = statistics.median(execution_times)

        if len(execution_times) > 0:
            percentile_90 = np.percentile(execution_times, 90)
        else:
            percentile_90 = 0  # Set a default value if the list is empty

        return avg_time, percentile_50, percentile_90, execution_times

    # =========================== Test Case 2 - Redis with Data ===========================
    def performance_test_case_2(query_function, n, from_year, to_year, trials):
        execution_times = []

        for _ in range(trials):
            result, execution_time = query_function(n, from_year, to_year)
            execution_times.append(execution_time)

        avg_time = statistics.mean(execution_times)
        percentile_50 = statistics.median(execution_times)

        if len(execution_times) > 0:
            percentile_90 = np.percentile(execution_times, 90)
        else:
            percentile_90 = 0

        return avg_time, percentile_50, percentile_90, execution_times

    print("Running App.....\n")

    # =========================== Connecting to MongoDB ===========================
    try:
        mongo_uri = ("YOUR_MONGODB_URI")
        mongo_client = MongoClient(mongo_uri, tls=True, tlsCertificateKeyFile='YOUR_CERTIFICATE.pem', server_api=ServerApi('1'))
        mongo_db = mongo_client['sample_mflix']
        logging.info('Connected to MongoDB')
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")

    # =========================== Connecting to Redis ===========================
    try:
        redis_host = 'YOUR_REDIS_HOST'
        redis_port = "YOUR_REDIS_PORT"
        redis_password = 'YOUR_REDIS_PASSWORD'

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

    # =========================== Performance Testing ===========================
    num_trials = 100

    # Case 1: Empty Redis
    avg_time_empty, percentile_50_empty, percentile_90_empty, execution_times_empty = performance_test_case_1(
        query_top_n, top_n, t_from, t_to, num_trials)

    # Case 2: Redis with data
    avg_time_with_data, percentile_50_with_data, percentile_90_with_data, execution_times_with_data = performance_test_case_2(
        query_top_n, top_n, t_from, t_to, num_trials)

    # Save results to CSV
    csv_headers = ["Case", "Average Time", "50th Percentile", "90th Percentile", "Execution Times"]
    csv_data = [
        ["Empty Redis", avg_time_empty, percentile_50_empty, percentile_90_empty, execution_times_empty],
        ["Redis with Data", avg_time_with_data, percentile_50_with_data, percentile_90_with_data,
         execution_times_with_data],
    ]

    csv_file_path = "performance_results.csv"
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(csv_headers)
        writer.writerows(csv_data)

    print(f"Performance results saved to {csv_file_path}")