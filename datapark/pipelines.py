# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json

import pymongo
from kafka import KafkaProducer
from scrapy.utils.project import get_project_settings

settings = get_project_settings()


class BrandMongoPipeline(object):
    def __init__(self):
        self.client = pymongo.MongoClient(host=settings['MONGO_HOST'], port=settings['MONGO_PORT'])
        db = self.client['datapark']
        self.collection = db['brand']

    def process_item(self, item, spider):
        try:
            self.collection.insert(dict(item))
        except Exception as e:
            spider.logger.error(e)
        return item

    def close_spider(self, spider):
        self.client.close()



class BrandKafkaPipeline(object):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=settings['KAFKA_URI'], value_serializer= lambda v: v.encode('utf-8'))

    def process_item(self, item, spider):
        topic = settings['TOPIC_BRAND']
        item = dict(item)
        json_item = json.dumps(item, ensure_ascii=False)
        print type(json_item), 'json_item type'
        self.producer.send(topic, json_item)
        self.producer.flush()
        return item

    # 这里遇到的问题: 一开始从__del__(稀构函数)里面关闭的close,导致的问题,是producer没有写入kafka就关闭了,
    # 正确的关闭应该是从scrapy的关闭函数进行关闭。
    def close_spider(self, spider):
        self.producer.close()
