import one_time_migration as migration
from redis.connection import Connection
import re
import argparse
from sqlalchemy import bindparam


def download_rdb(out_file_aof, host='localhost', port=6379, password=None):
    """
    Download a rdb dump file from a remote redis server
    out_file is the file name where the rdb will be saved
    host, port, password are used to connect to the remote redis server
    """
    try:
        conn = _MyConnection(host=host, port=port, password=password)
        conn.send_command("sync")

        conn.read_file()
        conn.receive_AOF_stream()
    finally:
        conn.disconnect()


class _MyConnection(Connection):
    def read_file(self):
        rdb_length = self.read_rdb_length()

        buff_length = 16384
        remaining = rdb_length
        while remaining > 0:
            partial = self._sock.recv(min(buff_length, remaining))
            remaining = remaining - len(partial)

    def read_rdb_length(self):
        # Read till we encounter \n in the socket
        data = b''.join(iter(lambda: self._sock.recv(1), b'\n'))
        # The first character is the $ symbol, skip it
        # Everything after that is the length of the file
        if len(data) == 0:
            return self.read_rdb_length()
        length = int(data[1:])
        return length

    def receive_AOF_stream(self):
        buff_length = 16384
        while 1:
            partial = self._sock.recv(buff_length)
            if partial:
                aof_parser(partial.decode('utf-8', 'ignore'))


def convert_to_string(out_file):
    with open(out_file, "r") as out:
        contents = out.read()
        return contents


def check_command(result):
    command = result[0]

    if "SELECT" is command or "PING" is command:
        return

    elif 'h' is command[0]:
        hashes(result)

    elif 's' is command[0]:
        sets(result)

    elif ('l' is command[0]) or ('r' is command[0]) or ('b' is command[0]):
        lists(result)

    elif 'z' is command[0]:
        sorted_sets(result)

    elif 'd' is command[0]:
        delete_keys(result)


def split_list(alist, wanted_parts):
    length = len(alist)
    return [alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


def calculate_value_migrate(key, key_value, table):

    columns = table['fields']
    value = {}
    type_ = table['key_type']
    regex = table["regex"]
    format_ = table.get("format", "")
    for column in columns:
        if column['source'] == "key":
            if column['value']['field'] in key_value.keys():
                value[column['column_name']] = key_value[column['value']['field']]

        if column['isPrimaryKey']:
            value[column['column_name']] = migration.get_value_from_source(column['source'], key, key_value, regex,
                                                                           column['value'], format_, type_)

    return value


def update_cont(key, table):
    key_name = None
    value = None
    for name in key.keys():
        key_name = name
        value = key[name]

    primary_keys = {}

    update_values = calculate_value_migrate(key_name, value, table)
    for column in table['fields']:
        if column['isPrimaryKey']:
            primary_keys["bind_" + column['column_name']] = update_values[column['column_name']]

    where_clause_parameter = [str(table['object'].__table__.c[k[5:]] == bindparam(k)) for k in primary_keys.keys()]
    where_clause_parameter = " ".join(where_clause_parameter)
    stmt = table['object']().__table__.update() \
        .where(where_clause_parameter) \
        .values(**update_values)
    conn = migration.engine.connect()
    conn.execute(stmt, **primary_keys)


def hashes(result):
    key_name = result[1]
    command = result[0]
    value2 = {result[1]: {}}
    if command == 'hdel':
        if len(result) == 3:

            value2[result[1]] = result[2]
            keys_delete = {}
            keys_delete = {result[1]: value2[result[1]], **keys_delete}
            migration.bulk_deletion(keys_delete, tables)
            print("%s key is deleted", key_name)
            return value2

        else:
            result1 = result[2:]
            value1 = set()
            for i in result1:
                value1.add(i)
            value2[key_name] = list(value1)
            keys_delete = {}
            keys_delete = {result[1]: value2[key_name], **keys_delete}
            migration.bulk_deletion(keys_delete, tables)
            print("{} key is deleted".format(key_name))
            return value2

    for table in tables.keys():
        regex = tables[table]["regex"]
        if re.match(regex, key_name):
            query_table = tables[table]["object"]()
            primary_keys = migration.get_primary_key_value(tables[table], value2)
            where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
                                      primary_keys.keys()]
            where_clause_parameter = " ".join(where_clause_parameter)
            stmt = query_table.__table__.select() \
                .where(where_clause_parameter)
            conn = migration.engine.connect()
            res = conn.execute(stmt, **primary_keys)
            for hash_name in res:
                hash_names = hash_name[1].split(" ")
                if set(hash_names) == set():
                    break
                else:
                    if len(result) == 3:
                        value2[result[1]][result[2]] = result[3]
                    else:
                        new = result[2:]
                        eff_length = len(new)
                        wanted_parts = eff_length // 2
                        result1 = split_list(new, wanted_parts)

                        for item in result1:
                            value2[result[1]][str(item[0])] = item[1]
                            item.insert(0, key_name)

                    keys = {}
                    keys = {key_name: value2[key_name], **keys}
                    update_cont(keys, tables[table])
                    print("{} key is updated".format(key_name))
                    return value2

    if len(result) == 4:
            value2[result[1]][result[2]] = result[3]
            keys = {}
            keys = {key_name: value2[result[1]], **keys}
            migration.bulk_insertion(keys, tables)
            print("{} key is inserted".format(key_name))
            return value2

    else:
            new = result[2:]
            eff_length = len(new)
            wanted_parts = eff_length // 2
            result1 = split_list(new, wanted_parts)

            for item in result1:
                value2[result[1]][str(item[0])] = item[1]
                item.insert(0, key_name)

            keys = {}
            keys = {key_name: value2[result[1]], **keys}
            migration.bulk_insertion(keys, tables)
            print("{} key is inserted".format(key_name))
            return value2


def sets(result):
    key_name = result[1]
    value2 = {result[1]: {}}
    value1 = set()
    if result[0] == "srem":
        value2[result[1]] = [result[2]]
        keys_delete = {}
        keys_delete = {result[1]: value2[result[1]], **keys_delete}
        migration.bulk_deletion(keys_delete, tables)
        print("{} key is deleted".format(key_name))
        return value2

    for table in tables.keys():
            regex = tables[table]["regex"]
            if re.match(regex, key_name):
                value2[result[1]] = [result[2]]
                query_table = tables[table]["object"]()
                primary_keys = migration.get_primary_key_value(tables[table], value2)
                where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
                                          primary_keys.keys()]
                where_clause_parameter = " ".join(where_clause_parameter)
                stmt = query_table.__table__.select() \
                    .where(where_clause_parameter)
                conn = migration.engine.connect()
                res = conn.execute(stmt, **primary_keys)
                for set_name in res:
                    set_names = set_name[1].split(" ")
                    for j in set_names:
                        value1.add(j)
                if value1 == set():
                    break
                else:
                    if len(result) == 3:
                        value1.add(result[2])
                    else:
                        new = result[2:]
                        for set_name in new:
                            value1.add(set_name)
                    value2[result[1]] = list(value1)
                    keys = {}
                    keys = {key_name: value2[key_name], **keys}
                    migration.update(keys, tables[table])
                    print("{} key is changed".format(key_name))
                    return value2

    if len(result) == 3:
        value2[result[1]] = [result[2]]
        keys = {}
        keys = {key_name: value2[key_name], **keys}
        migration.bulk_insertion(keys, tables)
        print("{} key is changed".format(key_name))
        return value2

    key_name = result[1]

    new = result[2:]

    value1 = set()
    for i in new:
        value1.add(i)
    value2[key_name] = list(value1)
    keys = {}
    keys = {key_name: value2[key_name], **keys}
    migration.bulk_insertion(keys, tables)
    print("{} key is changed".format(key_name))
    return value2


def lists(result):
    key_name = result[1]
    command = result[0]
    value2 = {result[1]: []}
    for table in tables.keys():
            regex = tables[table]["regex"]
            if re.match(regex, key_name):
                value2[result[1]] = [result[2]]
                query_table = tables[table]["object"]()
                primary_keys = migration.get_primary_key_value(tables[table], value2)
                where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
                                          primary_keys.keys()]
                where_clause_parameter = " ".join(where_clause_parameter)
                stmt = query_table.__table__.select() \
                    .where(where_clause_parameter)
                conn = migration.engine.connect()
                res = conn.execute(stmt, **primary_keys)
                for i in res:
                    k = i[1].split(" ")
                    value1 = set()
                    for j in k:
                        value1.add(j)
                    if value1 == set():
                        break
                    else:
                        if command == "lrem":
                            value = set()
                            number = (int(result[2]))
                            while number <= 0:
                                value.add(result[3])
                                number = number + 1
                            value2[result[1]] = list(value1-value)
                            keys_delete = {}
                            keys_delete = {result[1]: value2[result[1]], **keys_delete}
                            migration.bulk_insertion(keys_delete, tables)
                            print("{} key is changed".format(key_name))
                            return value2

                        if len(result) == 3:
                            value1.add(result[2])
                        else:
                            new = result[2:]
                            for list_name in new:
                                value1.add(list_name)
                        value2[result[1]] = list(value1)
                        keys = {}
                        keys = {key_name: value2[key_name], **keys}
                        migration.update(keys, tables[table])
                        print("{} key is changed".format(key_name))
                        return value2

    if len(result) == 3:
        value2[result[1]] = [result[2]]
        keys = {}
        keys = {result[1]: value2[result[1]], **keys}
        migration.bulk_insertion(keys, tables)
        print("{} key is changed".format(key_name))
        return value2

    key_name = result[1]

    new = result[2:]
    value1 = set()

    for i in new:
        value1.add(i)
    value2[key_name] = list(value1)
    keys = {}
    keys = {key_name: value2[result[1]], **keys}
    migration.bulk_insertion(keys, tables)
    print("{} key is changed".format(key_name))
    return value2


def sorted_sets(result):
    key_name = result[1]
    value2 = {result[1]: {}}
    value1 = set()
    for table in tables.keys():
            regex = tables[table]["regex"]
            if re.match(regex, key_name):
                query_table = tables[table]["object"]()
                primary_keys = migration.get_primary_key_value(tables[table], value2)
                where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
                                          primary_keys.keys()]
                where_clause_parameter = " ".join(where_clause_parameter)
                stmt = query_table.__table__.select() \
                    .where(where_clause_parameter)
                conn = migration.engine.connect()
                res = conn.execute(stmt, **primary_keys)
                for sorted_sets_name in res:
                    k = sorted_sets_name[1].split(" ")
                    value1 = set()
                    for j in k:
                        value1.add(j)
                if value1 == set():
                    break
                else:
                    if len(result) == 4:
                        value1.add(result[2])
                    else:
                        new = result[2:]
                        eff_length = len(new)
                        wanted_parts = eff_length // 2
                        result1 = split_list(new, wanted_parts)
                        for item in result1:
                            item.insert(0, key_name)
                            value1.add(item[1])
                    value2[result[1]] = list(value1)
                    keys = {}
                    keys = {key_name: value2[key_name], **keys}
                    migration.update(keys, tables[table])
                    print("{} key is changed".format(key_name))
                    return value2

    if len(result) == 4:
        value2[result[1]][result[3]] = result[2]
        keys = {}
        keys = {key_name: value2[result[1]], **keys}
        migration.bulk_insertion(keys, tables)
        print("{} key is changed".format(key_name))
        return value2

    else:
        new = result[2:]
        eff_length = len(new)
        wanted_parts = eff_length // 2
        result1 = split_list(new, wanted_parts)

        for item in result1:

            value2[result[1]][str(item[1])] = item[0]
            item.insert(0, key_name)

        keys = {}
        keys = {key_name: value2[result[1]], **keys}
        migration.bulk_insertion(keys, tables)
        migration.Session.commit()
        print("{} key is changed".format(key_name))
        return value2


def delete_keys(result):

    key_name = result[1]
    value1 = set()
    value2 = {result[1]: []}
    for table in tables.keys():
        regex = tables[table]["regex"]
        type_ = tables[table]['key_type']
        format_ = tables[table].get("format", "")
        if re.match(regex, key_name):
            if type_ == "hash":
                value2[result[1]] = []
                query_table = tables[table]["object"]()
                primary_keys = migration.get_primary_key_value(tables[table], value2)
                where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
                                          primary_keys.keys()]
                where_clause_parameter = "".join(where_clause_parameter)
                stmt = query_table.__table__.select() \
                    .where(where_clause_parameter)
                conn = migration.engine.connect()
                res = conn.execute(stmt, **primary_keys)
                for i in res:
                    k = i[1].split(" ")
                    for j in k:
                        value1.add(j)
                value2[key_name] = list(value1)
                keys_delete = {}
                keys_delete = {result[1]: value2[key_name], **keys_delete}
                migration.bulk_deletion(keys_delete, tables)
                print("{} key is deleted".format(key_name))

            else:
                value2[result[1]] = []
                key = result[1]
                key_value = value2[result[1]]
                query_table = tables[table]["object"]()
                values = {}
                for column in tables[table]['fields']:
                    if column['source'] == "pattern":
                        values[column['column_name']] = migration.get_value_from_source(column['source'], key,
                                                                                        key_value, regex,
                                                                                        column['value'], format_, type_)
                primary_keys = {}
                for column in values.keys():
                    primary_keys["bind_" + column] = values[column]
                where_clause_parameter = [str(query_table.__table__.c[k[5:]] == migration.bindparam(k)) for k in
                                          primary_keys.keys()]
                where_clause_parameter = "".join(where_clause_parameter)
                stmt = query_table.__table__.delete().where(where_clause_parameter)
                conn = migration.engine.connect()

                conn.execute(stmt, **primary_keys)
                migration.Session.commit()
                print("{} key is deleted".format(key_name))
                return value2


def aof_parser(contents):
    items = contents.split("\r\n")
    length = len(items)

    for i in range(length-1):
        if '*' in items[i]:
            temp = []
            args = 2 * int(items[i].strip("*"))

            for z in range(args):
                temp.append(items[i + 1 + z])
            result = [x for x in temp if '$' not in x]
            check_command(result)
    migration.Session.commit()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-rh", "--redis_host", help="Redis Host Address", type=str, default="localhost")
    parser.add_argument("-ru", "--redis_db_user", help="Redis User", type=str, default="root")
    parser.add_argument("-rp", "--redis_db_password", help="Redis Password", type=str, default=None)
    parser.add_argument("-rport", "--redis_port", help="Redis Port", type=int, default=6379)
    parser.add_argument("-th", "--target_db_host", help="Target Database Host Address", type=str, default="localhost")
    parser.add_argument("-tu", "--target_db_user", help="Target Database User", type=str, default="root")
    parser.add_argument("-tp", "--target_db_password", help="Target Database Password", type=str, default="root")
    parser.add_argument("-tport", "--target_db_port", help="Target Database Port", type=int, default=6379)
    parser.add_argument("-db", "--target_db", help="Target Database Server", type=str, default="mysql")
    parser.add_argument("-dname", "--db_name", help="Target Database Name", type=str, default="redis")
    args = parser.parse_args()
    migration.target_db_setup(args.target_db, args.target_db_user, args.target_db_password, args.target_db_host,
                              args.target_db_port, args.db_name)
    tables = migration.one_time_migration("dump.rdb", args)

    print("aof parser started")
    download_rdb("AOF.txt")
    aof_parser()

