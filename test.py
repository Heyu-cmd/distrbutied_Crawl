#mongo_test
from pymongo import MongoClient
import time
import hashlib
import redis
client = MongoClient('localhost', 27017)
db = client.spider


# redis test
redis_client = redis.Redis(host='localhost', port=6379, db=0,decode_responses=True)
url = "www.baidu.com"
print(redis_client.get(url))
if redis_client.get(url) is not None:
    print("已存在")
redis_client.set(url, 1)
record = {
    "url": url,
    "state": "new",
    "queue_time": time.time()
}
print(record)
db.url.insert_one({
            'id':hashlib.md5(url.encode()).hexdigest()},
            {'$set':record
        })
records = db.url.find({'state':'new'})
print(records)
