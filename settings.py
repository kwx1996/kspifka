import pymysql

NAME = 'dytt'

REDIS_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'encoding': 'utf-8',
    'db': 7
}

CURRENT_REQUSET = 0.03  # request for per second( 1 / 0.03 ~= 33)

conn_kwargs = {
            'host': '127.0.0.1',
            'user': 'root',
            'password': '',
            'database': '',
            'cursorclass': pymysql.cursors.DictCursor
        }