import redis
import time
from functools import wraps


def rate_limiter(thread_name, calls, period, host='localhost', port=6379, db=0, decode_responses=True):
    """
    Decorator for limiting the frequency of function calls. If necessary to limit the number of
    function calls from different threads or other scripts.
    @rate_limiter(name="user1", calls=1, period=2)

    :param thread_name: The name of the counter in the Redis database
    :param calls: The maximum number of calls
    :param period: Time period in seconds
    :param host: The address of the installed and running Redis server (host='localhost')
    :param port: The port of the Redis server (port=637)
    :param db: The database number
    :param decode_responses: Whether to decode strings (decode_responses=True)

    !! If function calls are too frequent, for example in a while loop, it creates a heavy load
    !! on the Redis server and CPU. Function calls should have minimal delay (e.g. 0.05 s).
    !! To achieve this, use @ensure_delay(0.05)
    """

    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            call_cnt_name = thread_name
            current_count = redis_client.incr(call_cnt_name)  # Увеличиваем счётчик вызовов
            if current_count == 1:
                milliseconds_period = round(period*1000)
                if milliseconds_period <= 0 or milliseconds_period > 1000*60*5:
                    raise Exception(f"{milliseconds_period}ms must be greater than 0 and less than 5 min")
                redis_client.pexpire(call_cnt_name, milliseconds_period)  # Устанавливаем время жизни ключа

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

# Global rate protection example
def global_protection_test():
    """
    Limit the rate of calls of a function from any thread or script
    """
    import random

    @rate_limiter('testName', 1, 2)
    @ensure_delay(0.05)
    def print_random():
        print(f"rnd: {random.random()}")

    for i in range(100000):
        print_random()

# User rate limiter example
input_counter = 0
def user_protection_test():
    """
    Protects code section for each user using user_id as part of redis key
    """
    import threading

    def run_protected_code_section_as(user_id):
        @rate_limiter(user_id, 1, 0.5)
        @ensure_delay(0.05)
        def protected_code_section():
            # Some logic
            global input_counter
            print(f"{user_id} input {input_counter}!")
            input_counter += 1
        return protected_code_section()

    def emulate_user1_code_section_exec():
        for i in range(1000000):
            run_protected_code_section_as("user_1")

    def emulate_user2_code_section_exec():
        for i in range(1000000):
            run_protected_code_section_as("user_2")

    user1_thread = threading.Thread(target=emulate_user1_code_section_exec, daemon=True)
    user2_thread = threading.Thread(target=emulate_user2_code_section_exec, daemon=True)
    user1_thread.start()
    user2_thread.start()
    time.sleep(10)


if __name__ == '__main__':
    user_protection_test()

    # redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    # user_1_call_counter = redis_client.get("user_1_call_counter")
    # ttl = redis_client.ttl("user_1_call_counter")
    # print(ttl)
    # redis_client.delete("user_1_call_counter")
    # redis_client.delete("user_2_call_counter")