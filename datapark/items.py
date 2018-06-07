# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DataparkItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class BrandItem(scrapy.Item):
    # define the fields for your item here like:
    _id = scrapy.Field()
    post_url = scrapy.Field()
    post_title = scrapy.Field()
    content_text = scrapy.Field()
    content_html = scrapy.Field()
    crawl_time = scrapy.Field()
    site_name = scrapy.Field()
    type = scrapy.Field()
    module = scrapy.Field()


class ProductItem(scrapy.Item):
    _id = scrapy.Field()
    post_url = scrapy.Field()
    post_title = scrapy.Field()
    content_text = scrapy.Field()
    content_html = scrapy.Field()
    crawl_time = scrapy.Field()
    site_name = scrapy.Field()
    type = scrapy.Field()
    module = scrapy.Field()
