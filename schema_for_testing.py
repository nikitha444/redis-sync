from sqlalchemy import String, Integer, Date, DateTime
from redis import StrictRedis, ConnectionError
import re,random

SKIP_THIS_ROW = "ERROR_SKIP_THIS_ROW"
redis = StrictRedis(decode_responses=True, encoding='utf-8')


def from_hash(field,hash_key=None):
    if hash_key is None:
        return ""
    return hash_key[field]


def from_redis(key, field):

    return redis.hget(key, field)


def from_pattern(arg, key_name=None, pattern=None):
    return re.match(pattern, key_name).group(arg)


def get_value (redis, key_name, pattern_args, key_value, column_name, table_name):
    """

    :param redis: connection to redis database
    :param key_name: name of the key match
    :param pattern_args: arguments retrieve from key name
    :param key_value: value of key
    :param column_name: name of the column for value required
    :return: String or Integer value
    """
    try:
        if table_name == "user_table":
            if column_name == "user_id":
                return int(pattern_args['user_id'])
            elif column_name == "display_name":
                return str(key_value['display_name'])[:50].encode('utf-8')
            elif column_name == 'accountid':
                return int(key_value['accountid'])

        elif table_name == "questions_table":
            if column_name == "question_id":
                return int(pattern_args['question_id'])
            elif column_name == "owneruserid":
                return key_value['owneruserid'].encode('utf-8')
            elif column_name == 'title':
                return str(key_value['title'])
            elif column_name == 'score':
                return int(key_value['score'])
            elif column_name == "body":
                return key_value['body'].encode('utf-8')
            elif column_name == "tags":
                tag_set = redis.smembers("questions:%s:tags" % int(pattern_args['question_id']))
                tag_string = ""
                for tag in tag_set:
                    tag_string = tag_string + str(tag) + ", "
                return tag_string
            elif column_name == "related_questions":
                related_questions_set = redis.smembers("questions:%s:related_questions"
                                         % int(pattern_args['question_id']))
                related_questions_string = ""
                for related_question in related_questions_set:
                    related_questions_string = related_questions_string + str(related_question) + ", "
                return related_questions_string

        elif table_name == "answers_table":
            if column_name == "answer_id":
                return int(pattern_args['answer_id'])
            elif column_name == "score":
                return key_value['score'].encode('utf-8')
            elif column_name == "owneruserid":
                return key_value['owneruserid'].encode('utf-8')
            elif column_name == 'body':
                return str(key_value['body'])

        elif table_name == "comments_table":
            if column_name == "comment_id":
                return int(pattern_args['comment_id'])
            elif column_name == "score":
                return key_value['score'].encode('utf-8')
            elif column_name == "userid":
                return key_value['userid'].encode('utf-8')
            elif column_name == 'text':
                return str(key_value['text'])

        elif table_name == "tags_table":
            if column_name == "tag_name":
                return str(pattern_args['tag_name'])
            elif column_name == "tag_id":
                return key_value['id'].encode('utf-8')

        elif table_name == "question_answer_table":
            if column_name == "question_id":
                return int(pattern_args['question_id'])
            elif column_name == "answer_id":
                return int(key_value)
            elif column_name == "title":
                return redis.hmget("question:%s" % int(pattern_args['question_id']), 'title')

        elif table_name == "user_badges_table":
            if column_name == "user_id":
                return int(pattern_args['user_id'])
            elif column_name == "badge_name":
                return str(key_value)
            elif column_name == "user_name":
                return redis.hmget("users:%s" % int(pattern_args['user_id']), "display_name")

        elif table_name == "posts_comments_table":
            if column_name == "post_id":
                return int(pattern_args['post_id'])
            elif column_name == "comment_id":
                return int(key_value)
            elif column_name == "comment_text":
                return redis.hmget("comments:%s" % int(key_value), "text")


        elif table_name == "users_comments_table":
            if column_name == "user_id":
                return int(pattern_args['user_id'])
            elif column_name == "comment_id":
                return int(key_value)
            elif column_name == "comment_text":
                return redis.hmget("comments:%s" % int(key_value), "text")

        elif table_name == "users_questions_table":
            if column_name == "user_id":
                return int(pattern_args['user_id'])
            elif column_name == "question_id":
                return int(key_value)
            elif column_name == "question_title":
                return redis.hmget("questions:%s" % int(key_value), "title")
            elif column_name == "user_name":
                return redis.hmget("users:%s" % int(pattern_args['user_id']), "display_name")
    except KeyError:
        print("Key Error")
        return SKIP_THIS_ROW

    print("Error")
    return SKIP_THIS_ROW


rules = [
    # Hashes Structure
    {
        "matching_pattern": "users:(?P<user_id>\d+)$",
        "table_name": "user_table",
        "key_type": "hash",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "user_id",
                "data_type": Integer,
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "display_name",
                "data_type": String(500)
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "accountid",
                "data_type": Integer
            }
        ]
    },
    {
        "matching_pattern": "questions:(?P<question_id>\d+)$",
        "table_name": "questions_table",
        "key_type": "hash",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "question_id",
                "data_type": Integer,
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "owneruserid",
                "data_type": Integer
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "title",
                "data_type": String(500)
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "score",
                "data_type": Integer
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "related_questions",
                "data_type": String(500)
            }
        ]
    },
    {
        "matching_pattern": "answers:(?P<answer_id>\d+)$",
        "table_name": "answers_table",
        "key_type": "hash",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "answer_id",
                "data_type": Integer,
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "score",
                "data_type": Integer
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "owneruserid",
                "data_type": Integer
            }
        ]
    },
    {
        "matching_pattern": "comments:(?P<comment_id>\d+)$",
        "table_name": "comments_table",
        "key_type": "hash",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "comment_id",
                "data_type": Integer,
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "score",
                "data_type": Integer
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "userid",
                "data_type": Integer
            },
            {

                "source": get_value,
                "isPrimaryKey": False,
                "name": "text",
                "data_type": String(1500)
            }
        ]
    },
    {
        "matching_pattern": "tags:(?P<tag_name>\w+)$",
        "table_name": "tags_table",
        "key_type": "hash",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "tag_name",
                "data_type": String(70),
            },
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "tag_id",
                "data_type": Integer,
            },
        ]
    },

    # Sets structure
    {
        "matching_pattern": "questions:(?P<question_id>\d+):answers$",
        "table_name": "question_answer_table",
        "key_type": "set",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "answer_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "question_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "title",
                "data_type": String(500),
            }
        ]
    },
    {
        "matching_pattern": "users:(?P<user_id>\d+):badges$",
        "table_name": "user_badges_table",
        "key_type": "set",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "user_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "badge_name",
                "data_type": String(50),
            },
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "user_name",
                "data_type": String(80),
            }
        ]
    },
    # lists structures
    {
        "matching_pattern": "posts:(?P<post_id>\d+):comments$",
        "table_name": "posts_comments_table",
        "key_type": "list",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "post_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "comment_id",
                "data_type": Integer,
            },
            # {
            #     "source": get_value,
            #     "isPrimaryKey": False,
            #     "name": "comment_text",
            #     "data_type": String(1500),
            # }
        ]
    },

    # sorted sets structures

    {
        "matching_pattern": "users:(?P<user_id>\d+):comments$",
        "table_name": "users_comments_table",
        "key_type": "sorted_set",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "user_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "comment_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "comment_text",
                "data_type": String(1500),
            }
        ]
    },
    {
        "matching_pattern": "users:(?P<user_id>\d+):questions_by_views$",
        "table_name": "users_questions_table",
        "key_type": "sorted_set",
        "columns": [
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "user_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": True,
                "name": "question_id",
                "data_type": Integer,
            },
            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "question_title",
                "data_type": String(700),
            },

            {
                "source": get_value,
                "isPrimaryKey": False,
                "name": "user_name",
                "data_type": String(70),
            }

        ]
    }

]
