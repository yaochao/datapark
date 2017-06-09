#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by yaochao on 2017/6/7
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())

# # 执行指定的spider
# process.crawl('socialbeta')
# process.crawl('qdaily')
# process.crawl('jiemian')
# process.crawl('toodaylab')
# process.crawl('madisonboom')
# process.crawl('iwebad')
# process.crawl('adquan')
# process.crawl('digitaling')
# process.crawl('iresearch')
# process.crawl('ebrun')
# process.crawl('eshow365')
# process.crawl('events_ireasearch')
# process.start()


# 执行所有的spider
for spider in process.spider_loader.list():
    process.crawl(spider)
process.start()
