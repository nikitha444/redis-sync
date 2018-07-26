import unittest

import os
from sqlalchemy import Integer, String
from redis import StrictRedis, ConnectionError
import one_time_migration
# import migration
from download_rdb import download_rdb
from jsonpath_ng import parse
from rdbtools import RdbParser, RdbCallback, encodehelpers
from redis import StrictRedis
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import bindparam, Column, create_engine, exc, Integer, MetaData
import argparse
import hashlib
import json
import re
import time
import sys


def test_connection(red):
    try:
        red.ping()
        print("Successfully connected to redis server")
        print("")
    except ConnectionError as ce:
        print("CANNOT connect to redis. Make sure you have installed redis and have started it")
        print("")
        raise


class TestOTM(unittest.TestCase):

    def setUp(self):
        self.red = StrictRedis(decode_responses=True, encoding="UTF-8")
        # os.system("python migration.py --db_name test")
        self.rule = [
            {
                "matching_pattern": "users:(?P<user_id>\d+)$",
                "table_name": "user_table",
                "key_type": "hash",
                "dependency": ["users:(?P<user_id>\d+):comments"],
                "columns": [
                    {
                        "source": "pattern",
                        "isPrimaryKey": True,
                        "column_name": "user_id",
                        "column_data_type": Integer,
                        "value": {
                            "group_pattern": "user_id"
                        }

                    },
                    {
                        "source": "key",
                        "isPrimaryKey": False,
                        "column_name": "display_name",
                        "column_data_type": String(70),
                        "value": {
                            "field": "display_name"
                        }
                    }
                    ]
            },
            {
                "matching_pattern": "posts:(?P<post_id>\d+):comments$",
                "table_name": "posts_comments_table",
                "key_type": "list",
                "format": "multi_row",  # single_row or multi_row
                "columns": [
                    {
                        "source": "pattern",
                        "isPrimaryKey": False,
                        "column_name": "post_id",
                        "column_data_type": Integer,
                        "value": {
                            "group_pattern": "post_id"
                        }
                    },
                    {
                        "source": "key",
                        "isPrimaryKey": True,
                        "column_name": "comment_id",
                        "column_data_type": String(100),
                        "value": {
                            "field": "comment_id"
                        }
                    }
                ]
            }
        ]

        self.key_name = "users:69"
        self.data = {
            "display_name": "ashish",
            "id": "69"
        }
        self.db_type = "mysql"
        self.user = "root"
        self.db_pd = "root"
        self.host = "localhost"
        self.port = 3306
        self.db_name = "test"
        self.db_name = "test"
        target_db = self.db_type + r'://' + self.user + ":" + self.db_pd + "@" + self.host + \
                    ":" + str(self.port) \
                    + r'/' + self.db_name


    def tearDown(self):
        pass

    def test_get_value_from_source(self):
        # hash source=pattern
        self.assertEqual(one_time_migration.get_value_from_source(self.rule[0]["columns"][0]["source"],
                                                                  self.key_name, self.data,
                                                                  self.rule[0]["matching_pattern"],
                                                                  self.rule[0]["columns"][0]["value"],
                                                                  None, 'hash'), '69')

        # hash source=key
        self.assertEqual(one_time_migration.get_value_from_source(self.rule[0]["columns"][1]["source"],
                                                                  self.key_name,
                                                                  self.data, self.rule[0]["matching_pattern"],
                                                                  self.rule[0]["columns"][1]["value"],
                                                                  None, 'hash'), 'ashish')

        #list/set/sorted_set single row
        self.assertEqual(one_time_migration.get_value_from_source(self.rule[1]["columns"][1]["source"],
                                                                "posts:69:comments",['34','33','33'],
                                                                self.rule[1]["matching_pattern"],
                                                                self.rule[1]["columns"][1]["value"],
                                                                "single_row",'list'),"34 33 33")

        #list/set/sorted_set multi row
        self.assertEqual(one_time_migration.get_value_from_source(self.rule[1]["columns"][1]["source"],
                                                                "posts:69:comments", ['34', '33', '33'],
                                                                self.rule[1]["matching_pattern"],
                                                                self.rule[1]["columns"][1]["value"],
                                                                "multi_row", 'list'), ['34','33','33'])
