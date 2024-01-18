import time

# A simple decorator to measure the time a function takes to execute
def time_it(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Time before function execution
        result = func(*args, **kwargs)  # Call the actual function
        end_time = time.time()  # Time after function execution
        print(f"Executing {func.__name__} took {end_time - start_time} seconds.")
        return result
    return wrapper