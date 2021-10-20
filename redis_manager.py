import hashlib
from datetime import datetime
import redis
import pymongo
class RedisManager:

    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=4, mongo_host='localhost'):
        self.redis_client = redis.Redis(host=redis_host,port=redis_port,db=redis_db,decode_responses=True)

        self.mongo = pymongo.MongoClient(mongo_host, 27017)
        self.db = self.mongo.spider
    def enqueueUrl(self, url, status):
        if self.redis_client.get(url) is not None:
            return
        self.redis_client.set(url, 1)
        record = {
            'id': hashlib.md5(url.encode()).hexdigest(),
            'url':url,
            'status':status,
            'queue_time':datetime.utcnow(),
        }

        # 向mongo插入数据
        self.db.url.insert_one(record)

if __name__ == '__main__':
    redis_client = RedisManager()
    url = "https://www.baidu.com"
    status = "new"
    for i in range(100):
        url2 = url+str(i)
        redis_client.enqueueUrl(url2, status)