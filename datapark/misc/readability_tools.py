#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by yaochao on 2017/8/4

from readability import Document

def get_summary(content):
    doc = Document(content)
    summary = doc.summary(html_partial=True)
    return summary