import redis
import time
import threading
from functools import wraps
import redis
import os
import random


def rate_limiter(name, calls, period, host='localhost', port=6379, db=0, decode_responses=True):
    """
    Decorator for limiting the frequency of function calls. If necessary to limit the number of
    function calls from different threads or other scripts.
    @rate_limiter(name="test_limiter", calls=2, period=1)

    !! If function calls are too frequent, for example in a while loop, it creates a heavy load
    !! on the Redis server and CPU. Function calls should have minimal delay (e.g. 0.05 s).
    !! To achieve this, use @ensure_delay(0.05)

    :param name: The name of the counter in the Redis database
    :param calls: The maximum number of calls
    :param period: Time period in seconds
    :param host: The address of the installed and running Redis server (host='localhost')
    :param port: The port of the Redis server (port=637)
    :param db: The database number
    :param decode_responses: Whether to decode strings (decode_responses=True)
    """

    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            current_count = redis_client.incr(name)  # Увеличиваем счётчик вызовов
            if current_count == 1:
                milliseconds_period = period*1000
                redis_client.pexpire(name, milliseconds_period)  # Устанавливаем время жизни ключа

            if current_count > calls:
                return None
            return func(*args, **kwargs)

        return wrapper

    return decorator

def ensure_delay(seconds):
    """
    Decorator for creating a delay between function calls.
    :param seconds: Time in seconds
    """

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            time.sleep(seconds)

            return result

        return wrapper

    return decorator


# Test
if __name__ == '__main__':

    @rate_limiter('test', 1, 2)
    @ensure_delay(0.05)
    def test_func():
        print(f"random val {random.random()}!")

    for i in range(1000000):
        test_func()