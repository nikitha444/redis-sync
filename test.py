import unittest
from redis import StrictRedis
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, exc, MetaData, bindparam, Integer, select
import one_time_migration as migration
import re
import argparse


class ContMigration(unittest.TestCase):

    def setUp(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-rh", "--redis_host", help="Redis Host Address", type=str, default="localhost")
        parser.add_argument("-ru", "--redis_db_user", help="Redis User", type=str, default="root")
        parser.add_argument("-rp", "--redis_db_password", help="Redis Password", type=str, default=None)
        parser.add_argument("-rport", "--redis_port", help="Redis Port", type=int, default=6379)
        parser.add_argument("-th", "--target_db_host", help="Target Database Host Address", type=str,
                            default="localhost")
        parser.add_argument("-tu", "--target_db_user", help="Target Database User", type=str, default="root")
        parser.add_argument("-tp", "--target_db_password", help="Target Database Password", type=str, default="root")
        parser.add_argument("-tport", "--target_db_port", help="Target Database Port", type=int, default=6379)
        parser.add_argument("-db", "--target_db", help="Target Database Server", type=str, default="mysql")
        parser.add_argument("-dname", "--db_name", help="Target Database Name", type=str, default="redis")
        args = parser.parse_args()
        migration.target_db_setup(args.target_db, args.target_db_user, args.target_db_password, args.target_db_host,
                                  args.target_db_port, args.db_name)
        self.tables = migration.one_time_migration("dump.rdb", args)

    def test_insert_cont(self):

        key_name = "questions:666665:answers"
        value2 = {}
        value1 = set()
        value3 = set()
        value2[key_name] = "9"
        value1.add("9")
        print("value1")
        print(value1)

        keys = {}
        keys = {key_name: value2[key_name], **keys}
        migration.bulk_insertion(keys, self.tables)
        migration.Session.commit()

        for table in self.tables.keys():
            regex = self.tables[table]["regex"]
            if re.match(regex, key_name):
                query_table = self.tables[table]["object"]()
                primary_keys = migration.get_primary_key_value(self.tables[table], value2)
                where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
                                          primary_keys.keys()]
                where_clause_parameter = " ".join(where_clause_parameter)
                stmt = query_table.__table__.select() \
                    .where(where_clause_parameter)
                conn = migration.engine.connect()
                res = conn.execute(stmt, **primary_keys)
                for i in res:
                    print("res")
                    print(i)
                    k = i[1].split(" ")
                    for j in k:
                        value3.add(j)
                print("value3")
                print(value3)
                break
        self.assertEqual(value3, value1)

    # def test_del_cont(self):
    #
    #     key_name = "questions:666666:answers"
    #     value2 = {}
    #     value2[key_name] = "9"
    #     for table in self.tables.keys():
    #         regex = self.tables[table]["regex"]
    #         if re.match(regex, key_name):
    #             query_table = self.tables[table]["object"]()
    #             primary_keys = migration.get_primary_key_value(self.tables[table], value2)
    #             where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
    #                                       primary_keys.keys()]
    #             where_clause_parameter = " ".join(where_clause_parameter)
    #             stmt = query_table.__table__.select() \
    #                 .where(where_clause_parameter)
    #             conn = migration.engine.connect()
    #             res = conn.execute(stmt, **primary_keys)
    #             value1 = set()
    #             for i in res:
    #                 k = i[1].split(" ")
    #                 for j in k:
    #                     value1.add(j)
    #             print(value1)
    #
    #             keys = {}
    #             keys = {key_name: value2[key_name], **keys}
    #             migration.bulk_insertion(keys, self.tables)
    #             migration.Session.commit()
    #
    #             stmt = query_table.__table__.select() \
    #                 .where(where_clause_parameter)
    #             conn = migration.engine.connect()
    #             res = conn.execute(stmt, **primary_keys)
    #             value3 = set()
    #             for i in res:
    #                 k = i[1].split(" ")
    #                 for j in k:
    #                     value3.add(j)
    #             print(value3)
    #             value4=set()
    #             value4.add(value2[key_name])
    #             self.assertEqual(value1-value3,value4)
    #             break

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()




