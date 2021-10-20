from datetime import datetime
from datetime import timedelta
import hashlib
import time

import redis
from pymongo import MongoClient
from pymongo import IndexModel, ASCENDING, DESCENDING

class MongoManager:

    def __init__(self, server_ip='localhost', client=None, expires=timedelta(days=30)):
        """
        :param server_ip:
        :param client: mongo database client
        :param expires: timedelta of amount of time before a cache entry is considered expired
        """
        # if client object is not passed
        # then try connecting to mongodb at the default localhost port
        self.client = MongoClient(server_ip, 27017)

        # create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
        self.db = self.client.spider
        self.table = self.db.url

        # # create index if db is empty
        # if self.table.count() == 0:
        #     self.table.create_index([("status",ASCENDING)])


    def dequeueUrl(self, size = 50):
        records = self.table.find({'status':'new'}).limit(size)
        ids = []
        records_re =[]
        for record in records:
            ids.append(record['_id'])
            dict_record= {
                # '_id': record['_id'],
                "id": record['id'],
                "url": record['url'],
            }
            records_re.append(dict_record)
        self.table.update_many(
            {
                '_id':{'$in':ids}
            },
            {
                '$set':{'status':'downloading'}
            }
        )

        if records_re:
            return records_re
        else:
            return None


    def finishItems(self, ids):
        """

        :param ids: urls which is crawled
        :return:
        """
        self.table.update_many(
            {
                'id': {'$in': ids}
            },
            {
                '$set': {'status': 'finish'}
            }
        )
    def clear(self):
        # warning ！！！
        # if excute this code, data of table will be cleared
        self.table.drop()
if __name__ == '__main__':
    mongo_client = MongoManager()
    mongo_client.clear()
    # records = mongo_client.dequeueUrl(10)
    #
    # for r in records:
    #     print(r)
