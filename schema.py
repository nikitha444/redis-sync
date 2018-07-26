from sqlalchemy import String, Integer,DateTime

rules = [
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
                "column_data_type": String(700),
                "value": {
                    "field": "display_name"
                }
            },
            {
                "source":"key",
                "isPrimaryKey": False,
                "column_name": "reputation",
                "column_data_type": Integer,
                "value":{
                    "field": "reputation"
                }
            },
            {
                "source":"key",
                "isPrimaryKey": False,
                "column_name": "upvotes",
                "column_data_type": Integer,
                "value":{
                    "field": "upvotes"
                }

            },
            {
                "source": "key",
                "isPrimaryKey": False,
                "column_name": "downvotes",
                "column_data_type": Integer,
                "value": {
                    "field": "downvotes"
                }

            },
            {
                "source": "key",
                "isPrimaryKey": False,
                "column_name": "creation_date",
                "column_data_type": DateTime,
                "value": {
                    "field": "creation_date"
                }

            },
            {
                "source": "key",
                "isPrimaryKey": False,
                "column_name": "last_access_date",
                "column_data_type": DateTime,
                "value": {
                    "field": "last_access_date"
                }
            },

            # {
            #     "source": "lua",
            #     "isPrimaryKey": False,
            #     "column_name": "comment id from lua",
            #     "column_data_type": String(500),
            #     "value": {
            #         "script": "return redis.call('zrange',KEYS[1],0,-1)",
            #         "keys": ["users:\g<user_id>:comments"],
            #         "arguments": []
            #     }
            # },
            # {
            #     "source": "lua",
            #     "isPrimaryKey": False,
            #     "column_name": "question_answered",
            #     "column_data_type": Integer,
            #     "value": {
            #         "script": "local len = redis.call('zcard',KEYS[1]) return len ",
            #         "keys": ["users:\g<user_id>:answers_by_score"],
            #         "arguments": []
            #     }
            # },
            # {
            #     "source": "json_path",
            #     "isPrimaryKey": False,
            #     "column_name": "test_json_path",
            #     "column_data_type": String(50),
            #     "value": {
            #         "json_path": "$.data",
            #         "field_name": "test"
            #     }
            # },
            # {
            #     "source": "join",
            #     "isPrimaryKey": False,
            #     "column_name": "comment id from join",
            #     "column_data_type": String(100),
            #     "value": {
            #         "key_name": "users:\g<user_id>:comments",
            #         "key_type": "sorted_set",
            #         "key_field": "value"
            #     }
            #
            # }
            ]
    },
    # {
    #     "matching_pattern": "posts:(?P<post_id>\d+):comments$",
    #     "table_name": "posts_comments_table",
    #     "key_type": "list",
    #     "format": "multi_row",  # single_row or multi_row
    #     "columns": [
    #         {
    #             "source": "pattern",
    #             "isPrimaryKey": False,
    #             "column_name": "post_id",
    #             "column_data_type": Integer,
    #             "value": {
    #                 "group_pattern": "post_id"
    #             }
    #         },
    #         {
    #             "source": "key",
    #             "isPrimaryKey": True,
    #             "column_name": "comment_id",
    #             "column_data_type": String(100),
    #             "value": {
    #                 "field": "comment_id"
    #             }
    #         }
    #     ]
    # },
    # {m
    #     "matching_pattern": "questions:(?P<question_id>\d+):answers$",
    #     "table_name": "question_answer_table",
    #     "key_type": "set",
    #     "format": "multi_row",
    #     "columns": [
    #         {
    #             "source": "pattern",
    #             "isPrimaryKey": False,
    #             "column_name": "question_id",
    #             "column_data_type": Integer,
    #             "value": {
    #                 "group_pattern": "question_id",
    #             }
    #         },
    #         {
    #             "source": "key",
    #             "isPrimaryKey": True,
    #             "column_name": "answer_id",
    #             "column_data_type": Integer,
    #             "value": {
    #                 "field": "answer_id",  # No Need
    #             }
    #         },
    #
    #     ]
    # },
    # {
    #
    #     "matching_pattern": "users:(?P<user_id>\d+):questions_by_views$",
    #     "table_name": "users_questions_table",
    #     "key_type": "sorted_set",
    #     "format": "multi_row",
    #     "columns": [
    #         {
    #             "source": "pattern",
    #             "isPrimaryKey": False,
    #             "column_name": "user_id",
    #             "column_data_type": Integer,
    #             "value": {
    #                 "group_pattern": "user_id",
    #             }
    #         },
    #         {
    #             "source": "key",
    #             "isPrimaryKey": True,
    #             "column_name": "question_id",
    #             "column_data_type": String(100),
    #             "value": {
    #                 "field": "question_id",
    #             }
    #         },
    #
    #     ]
    #
    # },
    {
        "matching_pattern": 'questions:(?P<question_id>\d+):tags',
        "table_name": "questions_tags",
        "key_type": 'set',
        "format": "multi_row",
        "dependency": ["tags:(?P<tag>[\w-]+)$"],
        "columns": [
            {
                "source": "pattern",
                "isPrimaryKey": True,
                "column_name": "Question_id",
                "column_data_type": Integer,
                "value": {
                    "group_pattern": "question_id",
                }
            },
            {
                "source": "key",
                "isPrimaryKey": True,
                "column_name": "tags",
                "column_data_type": String(2500),
                "value": {
                    "field": "data"
                }
            },
            {

                # Join work with sorted set, set or list only when format is "multi_row"
                "source": "join",
                "isPrimaryKey": False,
                "column_name": "tag_id",
                "column_data_type": Integer,
                "value":
                    {
                        "key_name": "tags:<tag_name>",
                        "key_type": "hash",
                        "key_field": "id",
                        "key_name_parameter":
                            {
                                "tag_name": {
                                    "source": "set",
                                    "field": "data"
                                }
                                # For Hash Type
                                # "tags_name":{
                                #     "source": "key",
                                #     "field": "tag_name"
                                # }
                                # For List Type
                                # "tags_name":{
                                #   "source": "key",
                                #   "field": "index"
                                # }

                            }
                    }
            }
        ]
    },


]
