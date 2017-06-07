# -*- coding: utf-8 -*-
import scrapy


class SocialbetaSpider(scrapy.Spider):
    name = 'socialbeta'
    allowed_domains = ['socialbeta.com']
    start_urls = ['http://socialbeta.com/']

    def parse(self, response):
        pass
