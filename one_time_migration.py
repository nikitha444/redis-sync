from download_rdb import download_rdb
from jsonpath_ng import parse
from rdbtools import RdbParser, RdbCallback, encodehelpers
from redis import StrictRedis
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import bindparam, create_engine, exc, Integer,DateTime
import migration as mg  # for table_factory
import argparse
import hashlib
import json
import pickle
import re
import time
import sys

try:
    with open("rules", "rb") as f:
        stored_rules = pickle.load(f)
except FileNotFoundError:
    print("Run migration first !")
    sys.exit()

Base = None
target_db = None
engine = None
metadata = None
Session = None

red = StrictRedis(decode_responses=True, encoding='utf-8')
old_hash_values = set()
new_hash_values = set()
old_hash_table = {}
new_hash_table = {}
old_key_name = set()
new_key_name = set()
old_key_value = {}
new_key_value = {}


def target_db_setup(db_type, user, password, host, port, db_name):
    """

    :param db_type:
    :param user:
    :param password:
    :param host:
    :param port:
    :param db_name:
    :return:
    """

    global target_db, engine, metadata, Session

    target_db = db_type+r'://'+user+":"+password+"@"+host+":"+str(port)+r'/'+db_name
    try:
        global Base
        engine = create_engine(target_db, echo=False)
        Base = declarative_base(bind=engine)

        # metadata = MetaData(bind=engine)
        s = sessionmaker(engine)
        Session = s()
    except exc.OperationalError as Error:
        print(Error)


class JSONCallback(RdbCallback):
    def __init__(self, flag, string_escape=None):
        if string_escape is None:
            string_escape = encodehelpers.STRING_ESCAPE_UTF8
        super(JSONCallback, self).__init__(string_escape)
        self._is_first_db = True
        self._has_databases = False
        self._is_first_key_in_db = True
        self._elements_in_key = 0
        self._element_index = 0
        self._flag = flag
        self.hash_value = {}
        self.hash_input = ""

    def encode_key(self, key):
        key = encodehelpers.bytes_to_unicode(key, self._escape, skip_printable=True)
        return key

    def encode_value(self, val):
        val = encodehelpers.bytes_to_unicode(val, self._escape)
        return val

    def _adding_to_set(self, key):
        hash_object = hashlib.md5(self.hash_input.encode('utf-8'))
        key = self.encode_key(key)
        if self._flag == 0:
            # global old_hash_values, old_key_value, old_key_name
            old_hash_values.add(hash_object.hexdigest())
            old_hash_table[hash_object.hexdigest()] = key
            old_key_name.add(key)
            old_key_value[key] = self.hash_value[key]

        else:
            # global new_hash_values
            new_hash_values.add(hash_object.hexdigest())
            new_hash_table[hash_object.hexdigest()] = key
            new_key_name.add(key)
            new_key_value[key] = self.hash_value[key]

    def start_rdb(self):
        pass

    def start_database(self, db_number):
        if not self._is_first_db:
            pass
        self._is_first_db = False
        self._has_databases = True
        self._is_first_key_in_db = True

    def end_database(self, db_number):
        pass

    def end_rdb(self):
        if self._has_databases:
            pass

    def _start_key(self, key, length):
        self.hash_input = ""
        self._is_first_key_in_db = False
        self._elements_in_key = length
        self._element_index = 0
        self.hash_value = {self.encode_key(key): {}}

    def _end_key(self, key):

        pass

    def _write_comma(self):
        if self._element_index > 0:
            if self._element_index < self._elements_in_key:
                self.hash_input = self.hash_input
        self._element_index = self._element_index + 1

    def set(self, key, value, expiry, info):
        self._start_key(key, 0)
        self.hash_input = self.hash_input + self.encode_key(key) + str(':') + self.encode_value(value)

    def start_hash(self, key, length, expiry, info):
        self._start_key(key, length)
        self.hash_input = self.hash_input + self.encode_key(key)

    def hset(self, key, field, value):
        self._write_comma()
        self.hash_input = self.hash_input + self.encode_key(field) + str(':') + self.encode_value(value)
        self.hash_value[self.encode_key(key)][self.encode_key(field)] = self.encode_value(value)

    def end_hash(self, key):
        self._end_key(key)
        self.hash_input = self.hash_input
        self._adding_to_set(key)

    def start_set(self, key, cardinality, expiry, info):
        self._start_key(key, cardinality)
        self.hash_input = self.hash_input + self.encode_key(key)
        self.hash_value[self.encode_key(key)] = []

    def sadd(self, key, member):
        self._write_comma()
        self.hash_input = self.hash_input + self.encode_value(member)
        self.hash_value[self.encode_key(key)].append(self.encode_value(member))

    def end_set(self, key):
        self._end_key(key)
        self.hash_input = self.hash_input
        self._adding_to_set(key)

    def start_list(self, key, expiry, info):
        self._start_key(key, 0)
        self.hash_input = self.hash_input + self.encode_key(key)
        self.hash_value[self.encode_key(key)] = []

    def rpush(self, key, value):
        self._elements_in_key += 1
        self._write_comma()
        self.hash_input = self.hash_input + self.encode_value(value)
        self.hash_value[self.encode_key(key)].append(self.encode_value(value))

    def end_list(self, key, info):
        self._end_key(key)
        self.hash_input = self.hash_input
        self._adding_to_set(key)

    def start_sorted_set(self, key, length, expiry, info):
        self._start_key(key, length)
        self.hash_input = self.hash_input + self.encode_key(key)
        self.hash_value[self.encode_key(key)] = {}

    def zadd(self, key, score, member):
        self._write_comma()
        self.hash_input = self.hash_input + self.encode_key(member) + self.encode_value(score)
        self.hash_value[self.encode_key(key)][self.encode_key(member)] = self.encode_key(score)

    def end_sorted_set(self, key):
        self._end_key(key)
        self.hash_input = self.hash_input
        self._adding_to_set(key)


def get_value_from_join(value, key, key_data, regex):

    join_key = value['key_name']
    type_ = value['key_type']
    field = value['key_field']
    key_parameter = value.get("key_name_parameter", {})

    # pattern_dict = re.match(regex, key).groupdict()
    # for k, _ in sorted(regex.groupindex.items(), key=lambda x: x[1]):
    #     join_key = join_key.replace("<"+k+">",pattern_dict[k])
    # replace regex group with value
    join_key = re.sub(regex, join_key, key)

    join_key_pattern_dict = {}

    for param in key_parameter.keys():
        if key_parameter[param]["source"] == "hash":
            join_key_pattern_dict[param] = key_data[key_parameter[param]['field']]

        else:
            join_key_pattern_dict[param] = key_data
    for k in key_parameter.keys():
        join_key = join_key.replace("<"+k+">", join_key_pattern_dict[k])

    if join_key in new_key_value:
        value = new_key_value[join_key]
    elif join_key in old_key_value:
        value = old_key_value[join_key]
    else:
        return 0
    if type_ == "hash":
        return value[field]
    elif type_ == "sorted_set":
        if field == "score":
            return " ".join([value[key] for key in value.keys()])
        else:
            return " ".join([key for key in value.keys()])
    else:
        return value


def run_lua_script(lua, keys, args_, regex, key_name):

    # match = re.match(regex, key_name)
    # pattern_dict = match.groupdict()
    keys_ = []
    args__ = []
    for index, key in enumerate(keys):
        keys_.append(re.sub(regex, key, key_name))

    for index, arg in enumerate(args_):
        # for k, _ in sorted(regex.groupindex.items(), key=lambda x: x[1]):
        #     args_.append(arg.replace("<"+k+">", pattern_dict[k]))
            # args[index] = arg.replace(k,pattern_dict[k])
        args__.append((re.sub(regex, arg, key_name)))
    value = lua(keys=keys_, args=args__)
    return value


def get_value_from_json_path(data, schema_value):
    values = []
    json_path = schema_value["json_path"]
    field = schema_value["field_name"]
    try:
        json_data = json.loads(data[field])
    except ValueError:
        return ""
    except KeyError:
        return ""

    json_path_expr = parse(json_path)
    value = list(str(match.value) for match in json_path_expr.find(json_data))
    values = values + [v for v in value]
    return " ".join(values)


def create_row(row, table):

    t1 = table(**row)
    Session.merge(t1)


def get_value_from_source(source, key_name, data, regex, value, format_, type_):

    if source == 'key':
        if format_ == "single_row":
            return " ".join(data)
        elif format_ == "multi_row":
            return data
        elif type_ == "hash":
            try:
                return data[value["field"]]
            except KeyError:
                return None
    elif source == 'lua':
        lua_value = run_lua_script(value["script"], value["keys"], value["arguments"], re.compile(regex), key_name)
        if type(lua_value) == list:
            return " ".join(lua_value)
        else:
            return lua_value
    elif source == 'pattern':
        pattern_arg = re.match(regex, key_name).groupdict()
        return pattern_arg[value["group_pattern"]]

    elif source == 'json_path':
        return get_value_from_json_path(data, value)
    elif source == "join":
        return get_value_from_join(value, key_name, data, re.compile(regex))


def calculate_value(key, key_value, table, primary_cols=False, column_name=None):
    type_ = table['key_type']
    columns = table['fields']
    value = {}
    regex = table["regex"]
    format_ = table.get("format", "")
    if primary_cols:
        for column in columns:
            if column['isPrimaryKey']:
                value[column['column_name']] = get_value_from_source(column['source'], key, key_value, regex,
                                                                     column['value'], format_, type_)
                if not (column['column_data_type'] == Integer or column['column_data_type'] == DateTime):
                    data_type = column['column_data_type']
                    value[column['column_name']] = str(value[column['column_name']])
                    value[column['column_name']] = value[column['column_name']][:min(len(value[column['column_name']]),
                                                                                     data_type.length)].encode('utf-8')
    elif column_name is None:
        for column in columns:
            value[column['column_name']] = get_value_from_source(column['source'], key, key_value, regex,
                                                                 column['value'], format_, type_)
            if not (column['column_data_type'] == Integer or column['column_data_type'] == DateTime):
                value[column['column_name']] = str(value[column['column_name']])
                data_type = column['column_data_type']
                value[column['column_name']] = value[column['column_name']][:min(len(value[column['column_name']]),
                                                                                 data_type.length)].encode('utf-8')

    else:
        for column in columns:
            if column_name.get(column['column_name'], False):
                value[column['column_name']] = get_value_from_source(column['source'], key, key_value, regex,
                                                                     column['value'], format_, type_)
                if not (column['column_data_type'] == Integer or column['column_data_type'] == DateTime):
                    value[column['column_name']] = str(value[column['column_name']])
                    data_type = column['column_data_type']
                    value[column['column_name']] = value[column['column_name']][:min(len(value[column['column_name']]),
                                                                                     data_type.length)].encode('utf-8')
    return value


def update(key, table):

    key_name = None
    value = None
    for name in key.keys():
        key_name = name
        value = key[name]

    primary_keys = {}

    update_values = calculate_value(key_name, value, table, False, None)
    for column in table['fields']:
        if column['isPrimaryKey']:
            primary_keys["bind_"+column['column_name']] = update_values[column['column_name']]

    where_clause_parameter = [str(table['object'].__table__.c[k[5:]] == bindparam(k)) for k in primary_keys.keys()]
    where_clause_parameter = " ".join(where_clause_parameter)
    stmt = table['object']().__table__.update() \
        .where(where_clause_parameter) \
        .values(**update_values)
    conn = engine.connect()
    res=conn.execute(stmt, **primary_keys)

    # Session Commit in batch migration


def create_tables_from_stored_rules(check_migration=False):

    tables = {}

    for rule in stored_rules:
        table = rule['table_name']
        tables[table] = {}
        tables[table]['name'] = table
        tables[table]['key_type'] = rule['key_type']
        tables[table]['format'] = rule.get("format", "")
        tables[table]['regex'] = rule['matching_pattern']
        tables[table]['fields'] = rule['columns']
        tables[table]['dependency'] = rule.get('dependency', [])
        for index, column in enumerate(tables[table]["fields"]):
            if column['source'] == "lua":
                tables[table]['fields'][index]["value"]["script"] = red.register_script(column["value"]["script"])
                tables[table]['fields'][index]["value"]["keys"] = column["value"]["keys"]
                tables[table]['fields'][index]["value"]["arguments"] = column["value"]["arguments"]
        tables[table]['object'] = mg.table_factory(table, rule['columns'], Base)

    if check_migration:
        with open("metadata", "wb") as file:
            pickle.dump(Base.metadata, file)
        with open("rules", "wb") as file:
            pickle.dump(stored_rules, file)
        return

    return tables


def bulk_insertion(data, tables):
    duplicate = {}
    for key in data.keys():
        if duplicate.get(key, False):
            print(key)
        else:
            duplicate[key] = True
            insert(key, data[key], tables)


def insert(key, data, tables):
    for rule in stored_rules:
        regex = rule['matching_pattern']
        match = re.match(regex, key)

        if match is not None:

            type_ = rule['key_type']
            table_name = rule['table_name']
            if type_ == "set" or type_ == "sorted_set" or type_ == "list":
                if rule.get("format", "multi_row") == "multi_row":
                    for value in data:
                        row = calculate_value(key, value, tables[table_name], False, None)
                        create_row(row, tables[table_name]["object"])
                else:
                    row = calculate_value(key, data, tables[table_name], False, None)
                    create_row(row, tables[table_name]["object"])
            else:
                row = calculate_value(key, data, tables[table_name], False, None)
                create_row(row, tables[table_name]["object"])

            # break if matching rule found

            break


def delete(key, data, tables):
    for rule in stored_rules:
        regex = rule['matching_pattern']
        if re.match(regex, key):
            try:
                table = rule['table_name']
                query_table = tables[table]["object"]()
                conn = engine.connect()
                primary_keys = {}

                if not rule['key_type'] == "hash":
                    for column in rule['columns']:
                        if column['source'] == "pattern":
                            primary_keys["bind_"+column['column_name']] = get_value_from_source(column['source'],
                                                                                                key, data, regex,
                                                                                                column['value'],
                                                                                                rule['format'],
                                                                                                rule['key_type'])
                else:
                    primary_keys = get_primary_key_value(tables[table], data)

                where_clause_parameter = [str(query_table.__table__.c[k[5:]] == bindparam(k)) for k in
                                          primary_keys.keys()]

                where_clause_parameter = "".join(where_clause_parameter)
                stmt = query_table.__table__.delete().where(where_clause_parameter)
                conn.execute(stmt, **primary_keys)

            except ValueError as E:
                # if can't parse it return
                print(E)
            return
        # Session Commit in batch migration


def bulk_deletion(data, tables):

    res = {}
    for key in data.keys():
        res[key] = data[key]
        delete(key, res, tables)
    Session.commit()


def one_time_migration(file, args_):
    redis_host = args_.redis_host
    redis_port = args_.redis_port
    redis_password = args_.redis_db_password
    print("-----One Time Migration Started---------")
    start_time = time.time()
    print("Dump file download Started")
    download_rdb(file, redis_host, redis_port, redis_password)
    print("Dump file download Completed")
    callback = JSONCallback(0)
    print("Parsing Started")
    parser_ = RdbParser(callback)
    parser_.parse(file)
    print("Parsing Completed")
    print("Table Creation Started")
    tables_ = create_tables_from_stored_rules()
    print("Table Creation Completed")
    print("Querying...")
    data = old_key_value
    bulk_insertion(data, tables_)
    print("Query Completed")
    Session.commit()
    print("Target database commit")
    end_time = time.time()
    print("Execution Time For One Time Migration {0} Seconds" .format(end_time-start_time))
    return tables_


def get_dependency_updates(key_name, tables, update_):

    for table_name in tables.keys():
        table = tables[table_name]
        for dependency in table['dependency']:

            if re.match(dependency, key_name):
                for column in table['fields']:
                    if column['source'] == 'join':
                        join_column = table['object'].__table__.c[column['column_name']]
                        key_field = column['value']['key_field']
                        where_clause_parameter = str(join_column == bindparam('join_col_value'))
                        where_clause_value = {'join_col_value': old_key_value[key_name][key_field]}
                        if update_:
                            update_values = {column['column_name']: new_key_value[key_name][key_field]}
                        else:
                            if type(old_key_value[key_name][key_field]) == str:
                                new_value = None
                            else:
                                new_value = 0
                            update_values = {column['column_name']: new_value}
                        stmt = table['object']().__table__.update() \
                            .where(where_clause_parameter) \
                            .values(**update_values)
                        conn = engine.connect()
                        conn.execute(stmt, **where_clause_value)

    # Session Commit in batch migration


def get_primary_key_value(table, key):

    primary_key_values = {}
    key_name = None
    key_value = None
    for name in key.keys():
        key_name = name
        key_value = key
    values = calculate_value(key_name, key_value, table, True, None)
    for column in values.keys():
        primary_key_values["bind_" + column] = values[column]

    return primary_key_values


if __name__ == "__main__":
    # Getting the arguments from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument("-rh", "--redis_host", help="Redis Host Address", type=str, default="localhost")
    parser.add_argument("-ru", "--redis_db_user", help="Redis User", type=str, default="root")
    parser.add_argument("-rp", "--redis_db_password", help="Redis Password", type=str, default=None)
    parser.add_argument("-rport", "--redis_port", help="Redis Port", type=int, default=6379)
    parser.add_argument("-th", "--target_db_host", help="Target Database Host Address", type=str, default="localhost")
    parser.add_argument("-tu", "--target_db_user", help="Target Database User", type=str, default="root")
    parser.add_argument("-tp", "--target_db_password", help="Target Database Password", type=str, default="root")
    parser.add_argument("-tport", "--target_db_port", help="Target Database Port", type=int, default=3306)
    parser.add_argument("-db", "--target_db", help="Target Database Server", type=str, default="mysql")
    parser.add_argument("-dname", "--db_name", help="Target Database Name", type=str, default="redis")
    args = parser.parse_args()

    # Calling the function with the arguments received
    target_db_setup(args.target_db, args.target_db_user, args.target_db_password, args.target_db_host,
                    args.target_db_port, args.db_name)

    # Target_db_setup()
    one_time_migration("dump.rdb", args)
