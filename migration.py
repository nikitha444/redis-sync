from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, exc, Column
from alembic.util.exc import CommandError
import alembic.config
import pickle
import argparse
import os
import re
import sys

try:
    import schema
except ImportError:
    print("Schema Not Found")
    sys.exit()

try:
    from schema import rules
except ImportError:
    print("Rules Not Defined")
    sys.exit()

Base = None
target_db = None


def purge(dir_, pattern):
    for f in os.listdir(dir_):
        if os.path.isfile(os.path.join(dir_, f)):
            if re.match(pattern, f):
                os.remove(os.path.join(dir_, f))


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

    global target_db, engine
    target_db = db_type+r'://'+user+":"+password+"@"+host+":"+str(port)+r'/'+db_name
    try:
        global Base
        engine = create_engine(target_db, echo=False)
        Base = declarative_base(bind=engine)
    except exc.OperationalError as Error:
        print(Error)


def schema_verification():
    for rule in rules:
        if ('matching_pattern' in rule) and ('table_name' in rule) and ('key_type' in rule) and ('columns' in rule):
            if rule['key_type'] == 'list' or rule['key_type'] == 'set' or rule['key_type'] == 'sorted_set':
                if not ('format' in rule):
                    print("ERROR: format not included in rule whose key_type is %s" % rule['key_type'])
                    return False

        else:
            print("ERROR: Missing Important keys in rule")
            return False
        for column in rule['columns']:
            if ('source' in column) and ('isPrimaryKey' in column) and ('column_data_type' in column) and \
                    ('column_name' in column) and ('value' in column):
                if column['isPrimaryKey'] and rule['key_type'] == "hash":
                    if not column['source'] == "pattern":
                        print("Source of primary key can be only pattern")
                        print("check {0}.{1}" .format(rule['table_name'], column['column_name']))
                        return False
                if column['source'] == 'pattern':
                    if not ('group_pattern' in column['value']):
                        print("ERROR: Improper Field/s in column['value'] for 'source' = 'pattern' where table_name is "
                              "%s " % rule['table_name'])
                        return False
                elif column['source'] == 'key':
                    if not ('field' in column['value']):
                        print("ERROR: Improper Field/s in column['value'] for 'source' = 'key' where table_name is "
                              "%s " % rule['table_name'])
                        return False
                elif column['source'] == 'lua':
                    if not (('script' in column['value']) and ('keys' in column['value']) and (
                            'arguments' in column['value']) and ('dependency' in rule)):
                        print("ERROR: Improper Field/s in column['value'] for 'source' = 'lua' where table_name is"
                              " %s " % rule['table_name'])
                        return False
                elif column['source'] == 'json_path':
                    if not (('json_path' in column['value']) and ('field_name' in column['value'])):
                        print("ERROR: Improper Field/s in column['value'] for 'source' = 'json_path' where table_name"
                              " is %s " % rule['table_name'])
                        return False
                elif column['source'] == 'join':
                    if not (('key_name' in column['value']) and ('key_type' in column['value']) and
                            ('key_field' in column['value']) and ('dependency' in rule)):
                        print("ERROR: Improper Field/s in column['value'] for 'pattern' = 'join' where table_name is"
                              " %s " % rule['table_name'])
                        return False

            else:
                print("ERROR: Missing important keys in columns whose table name is %s " % rule['table_name'])
                return False
    return True


def table_factory(name, columns, base_class=Base):
    attr = {column['column_name']: Column(column['column_name'], column['column_data_type'],
                                          primary_key=column['isPrimaryKey']) for column in columns}
    table_name = {"__tablename__": name,  "__table_args__": {'extend_existing': True}}

    attr = {**table_name, **attr}
    new_table = type(name, (base_class,), attr)
    return new_table


def create_tables_from_rules(check_migration=False):

    tables = {}
    stored_rules = rules
    if not check_migration:
        try:
            with open("rules", "rb") as f:
                stored_rules = pickle.load(f)
        except FileNotFoundError:
            print("Error : no stored rules found")
            print("Run make_migration first!")
            sys.exit()

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
                # tables[table]['fields'][index]["value"]["script"] = red.register_script(column["value"]["script"])
                tables[table]['fields'][index]["value"]["keys"] = column["value"]["keys"]
                tables[table]['fields'][index]["value"]["arguments"] = column["value"]["arguments"]
        tables[table]['object'] = table_factory(table, rule['columns'], Base)

    if check_migration:
        with open("metadata", "wb") as f:
            pickle.dump(Base.metadata, f)
        with open("rules", "wb") as f:
            pickle.dump(stored_rules, f)
        return

    return tables


def make_migration(args_):

    target_db_setup(args_.target_db, args_.target_db_user, args_.target_db_password, args_.target_db_host,
                    args_.target_db_port, args_.db_name)
    if not schema_verification():
        sys.exit()
    tables_ = create_tables_from_rules(True)

    config = alembic.config.Config("./alembic.ini")
    config.set_main_option("sqlalchemy.url", target_db)
    config.set_main_option("script_location", "./migrate")
    script = ScriptDirectory.from_config(config)
    head_revision = ""
    try:
        command.revision(config, autogenerate=True)
        head_revision = script.get_current_head()
        migrate()

    except FileNotFoundError as Error:

        print("Error Occurred")
        print(Error)
        print("Rollback Migration")
        try:
            file = os.path.join(os.curdir, os.path.join("migrate/versions", (head_revision + "_.py")))
            os.remove(file)
        except FileNotFoundError:
            pass
        finally:
            sys.exit()
    except CommandError as E:
        print(E)
        print("Target Database not in sync")

        print("Re-Run migration")
        try:
            engine.execute("drop table alembic_version")
            purge("./migrate/versions", "\w+.py")
        except exc.ProgrammingError:
            pass
        finally:
            pass
    return tables_


def migrate():

    config = alembic.config.Config("./alembic.ini")
    config.set_main_option("sqlalchemy.url", target_db)
    config.set_main_option("script_location", "./migrate")
    command.upgrade(config, "head")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-th", "--target_db_host", help="Target Database Host Address", type=str, default="localhost")
    parser.add_argument("-tu", "--target_db_user", help="Target Database User", type=str, default="root")
    parser.add_argument("-tp", "--target_db_password", help="Target Database Password", type=str, default="root")
    parser.add_argument("-tport", "--target_db_port", help="Target Database Port", type=int, default=3306)
    parser.add_argument("-db", "--target_db", help="Target Database Server", type=str, default="mysql")
    parser.add_argument("-dname", "--db_name", help="Target Database Name", type=str, default="redis")

    args = parser.parse_args()

    make_migration(args)
