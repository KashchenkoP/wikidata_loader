# !/usr/bin/python
# -*- coding: utf-8 -*-
from itertools import islice
import neo4j

import utils

if __name__ == '__main__':
    driver = neo4j.GraphDatabase.driver(uri='bolt://35.204.164.2:7687', auth=('neo4j', 'cdert_433450'))
    query = utils.clear()
    # stage 1
    utils.flush(query, driver=driver)

    # stage 2
    with open('C:\Users\user\PycharmProjects\untitled1\wikidata\data.json') as f:
        bufer = f.read()
    driver.close()
