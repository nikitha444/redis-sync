from __future__ import print_function
from redis import StrictRedis, ConnectionError
from redis.sentinel import Sentinel
import datetime as dt
import pprint

pp = pprint.PrettyPrinter(indent=4)


def get_connection():
    # This is the object we will use to connect to redis server
    # You can pass host, port and password to StrictRedis...
    # ... but for this assigment, we will stick to defaults
    # so that it connects to redis server on localhost:6379
    return StrictRedis(decode_responses=True, encoding='utf-8')



def update(red,count):

    red.hset("users:828","reputation",count)
    


if __name__ == "__main__":

    red = get_connection()
    for i in range(1,10001):
        update(red,i)

        print(red.hget("users:828","reputation"))
    
