# -*- coding: utf-8 -*-
import sqlite3
import time

import scrapy

from datapark.misc.name_map import name_map
from ..misc.sqlite_tools import get_then_change_latest_url


####################################  conferencespider    ###########################
# class Eshow365Spider(scrapy.Spider):
#     name = 'eshow365'
#     start_urls = ['http://www.eshow365.com/zhanhui/0-0-0-0/0/%E5%B9%BF%E5%91%8A%20%E8%90%A5%E9%94%80']
#     custom_settings = {
#         'ITEM_PIPELINES': {
#             'datapark.pipelines.BrandMongoPipeline': 300,
#             'datapark.pipelines.BrandKafkaPipeline': 301,
#         }
#     }
#
#     def parse(self, response):
#
#         # 本次最新的文章的url
#         first_url = ''
#         # 上一次的最新的文章的url
#         latest_url = ''
#         posts = response.xpath('//div[@class="sslist"]')
#         for post in posts:
#             post_url = response.urljoin(post.xpath('p[@class="zhtitle"]/a/@href').extract_first())
#             post_title = post.xpath('p[@class="zhtitle"]/a//text()').extract()
#             post_title = ''.join(post_title).strip()
#             item = {
#                 '_id': post_url,
#                 'post_url': post_url,
#                 'post_title': post_title
#             }
#
#             # 把第一条数据作为最新的数据，存储到sqlite中
#             if not first_url:
#                 first_url = post_url
#                 latest_url = get_then_change_latest_url(self.name, first_url)
#             # 从sqlite中取出上一次最新的数据，与本次的数据做对比，如果相同则认为文章抓到了上次已经抓过的数据，如果不同则认为文章还没有抓完
#             if post_url == latest_url:
#                 print u'%s - 爬到了上次爬到的地方' % self.name
#                 return
#
#             request = scrapy.Request(url=post_url, callback=self.parse_post)
#             request.meta['item'] = item
#             yield request
#
#     def parse_post(self, response):
#         item = response.meta['item']
#         ps = response.xpath('//div[@class="zhxxcontent"]/p')
#         conference_time = ''
#         conference_address = ''
#         for index, p in enumerate(ps):
#             txt = p.xpath('string(.)').extract_first()
#             if u'举办时间' in txt:
#                 conference_time = txt.split(u'举办时间：')[-1]
#             if u'举办展馆' in txt:
#                 conference_address = txt.split(u'举办展馆：')[-1]
#         item['conference_time'] = conference_time
#         item['conference_address'] = conference_address
#         item['crawl_time'] = int(time.time())
#         item['site_name'] = name_map[self.name]
#         item['type'] = 'conference'
#         item['module'] = 'brand'
#         yield item
#
#
# class Events_ireasearchSpider(scrapy.Spider):
#     name = 'events_ireasearch'
#     start_urls = ['http://events.iresearch.cn/']
#     custom_settings = {
#         'ITEM_PIPELINES': {
#             'datapark.pipelines.BrandMongoPipeline': 300,
#             'datapark.pipelines.BrandKafkaPipeline': 301,
#         }
#     }
#
#     def parse(self, response):
#         # 本次最新的文章的url
#         first_url = ''
#         # 上一次的最新的文章的url
#         latest_url = ''
#         posts = response.xpath('//*[@id="databox"]/li')
#         for post in posts:
#             post_url = response.urljoin(post.xpath('div[@class="info"]/h3/a/@href').extract_first())
#             post_title = post.xpath('div[@class="info"]/h3/a/text()').extract_first()
#             conference_info = post.xpath('div[@class="info"]/p/text()').extract_first()
#             conference_time = conference_info.split(' ')[0]
#             conference_address = conference_info.split(' ')[-1]
#             item = {
#                 '_id': post_url,
#                 'post_url': post_url,
#                 'post_title': post_title,
#                 'conference_time': conference_time,
#                 'conference_address': conference_address,
#                 'crawl_time': int(time.time()),
#                 'site_name': name_map[self.name],
#                 'type': 'conference',
#                 'module': 'brand',
#             }
#
#             # 把第一条数据作为最新的数据，存储到sqlite中
#             if not first_url:
#                 first_url = post_url
#                 latest_url = get_then_change_latest_url(self.name, first_url)
#             # 从sqlite中取出上一次最新的数据，与本次的数据做对比，如果相同则认为文章抓到了上次已经抓过的数据，如果不同则认为文章还没有抓完
#             if post_url == latest_url:
#                 print u'%s - 爬到了上次爬到的地方' % self.name
#                 return
#             yield item
