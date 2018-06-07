#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by yaochao on 2017/8/7

from name_map import common_map

def get_headers(url):
    d = common_map[url]
    return d['headers'] if d.has_key('headers') else None