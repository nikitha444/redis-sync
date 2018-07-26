from rdbtools import RdbParser
import one_time_migration as migration
import re
import time
import argparse


def batch_migration(args_):
    """

    :param args_: command line arguments
    :return: None
    """
    # First Time CallBack
    # do one time migration to create tables
    tables = migration.one_time_migration("dump.rdb", args_)
    # capture current snapshot of database
    migration.download_rdb("./dump.rdb")
    callback = migration.JSONCallback(1)
    while True:
        print("Sleep")
        time.sleep(args_.time_interval)
        print("-------Batch Migrations Started---------------")
        start_time = time.time()
        migration.download_rdb("./dump.rdb", args_.redis_host, args_.redis_port, args_.redis_db_password)
        parser_ = RdbParser(callback)
        parser_.parse("./dump.rdb")
        delete_and_update_keys = migration.old_hash_values - migration.new_hash_values
        insert_and_update_keys = migration.new_hash_values - migration.old_hash_values
        new_key_name = set()
        old_key_name = set()
        for hash_ in delete_and_update_keys:
            key_name = migration.old_hash_table[hash_]
            old_key_name.add(key_name)

        for hash_ in insert_and_update_keys:
            key_name = migration.new_hash_table[hash_]
            new_key_name.add(key_name)

        deleted_keys = old_key_name - new_key_name
        insert_keys = new_key_name - old_key_name
        update_keys = new_key_name & old_key_name
        print("update keys", len(update_keys))
        print("insert keys", len(insert_keys))
        print("deleted keys", len(deleted_keys))
        keys_delete = {}
        for key_name_delete in deleted_keys:
            migration.get_dependency_updates(key_name_delete, tables, False)
            keys_delete = {key_name_delete: migration.old_key_value[key_name_delete], **keys_delete}
        migration.bulk_deletion(keys_delete, tables)
        keys = {}

        for key_name in insert_keys:
            # migration.get_dependency_updates(key_name, tables, True)
            keys = {key_name: migration.new_key_value[key_name], **keys}

        migration.bulk_insertion(keys, tables)

        for key_name in update_keys:
            migration.get_dependency_updates(key_name, tables, True)
            for table in tables.keys():
                regex = tables[table]["regex"]
                if re.match(regex, key_name):
                    if tables[table]["format"] == "multi_row":
                        update_values = set(migration.new_key_value[key_name]) - set(migration.old_key_value[key_name])
                        for update_value in update_values:
                            migration.update({key_name: update_value}, tables[table])
                    else:
                        migration.update({key_name: migration.new_key_value[key_name]}, tables[table])
                    break

        migration.Session.commit()
        migration.old_hash_values = migration.new_hash_values
        migration.old_hash_table = migration.new_hash_table
        migration.old_key_value = migration.new_key_value
        migration.old_key_name = migration.new_key_name
        migration.new_key_name = set()
        migration.new_key_value = {}
        migration.new_hash_values = set()
        migration.new_hash_table = {}
        end_time = time.time()
        print("Batch Migrations Processing Time {0} Seconds" .format(end_time-start_time))


if __name__ == "__main__":
    # Getting the arguments from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument("-ti", "--time_interval", help="Time interval between batch migration", type=int, default=10)
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
    migration.target_db_setup(args.target_db, args.target_db_user, args.target_db_password, args.target_db_host,
                              args.target_db_port, args.db_name)
    batch_migration(args)
