import logging

# Set up the logging infrastructure - this is the "surveillance system"
def setup_logger():
    """
    Creates a dedicated logger for the decorator.
    Think of this as setting up the recording equipment.
    """
    logger = logging.getLogger(__name__ + "_parameter_log")
    logger.setLevel(logging.INFO)
    
    # Only add handler if it doesn't exist (prevents duplicate logs)
    if not logger.handlers:
        handler = logging.FileHandler("./decorator.log", "a")
        logger.addHandler(handler)
    
    return logger


def logger_decorator(func):
    """
    This is the main decorator - the 'surveillance wrapper' that monitors function calls.
    
    The decorator pattern here follows a classic three-layer structure:
    1. Outer function: Receives the function to be decorated
    2. Wrapper function: Intercepts calls to the original function
    3. Return mechanism: Preserves the original function's behavior while adding logging
    """
    logger = setup_logger()
    
    def wrapper(*args, **kwargs):
        # Capture the input parameters - what's coming into the function
        pos_params = list(args) if args else "none"
        keyword_params = dict(kwargs) if kwargs else "none"
        
        # Execute the original function and capture its return value
        result = func(*args, **kwargs)
        
        # Create the log entry - this is the "surveillance report"
        log_message = (
            f"function: {func.__name__} "
            f"positional parameters: {pos_params} "
            f"keyword parameters: {keyword_params} "
            f"return: {result}"
        )
        
        # Write to the log file
        logger.log(logging.INFO, log_message)
        
        # Return the original result - the function should behave normally
        return result
    
    return wrapper


# Test Function 1: Simple function with no parameters
@logger_decorator
def simple_hello():
    """
    This tests the decorator's ability to handle functions with no parameters.
    It's the simplest case - no input, no return value.
    """
    print("Hello, World!")
    return None  # Explicitly return None for clarity in logs

# Test Function 2: Function with variable positional arguments
@logger_decorator
def accept_many_args(*args):
    """
    This tests the decorator with functions that accept any number of positional arguments.
    This is like a function that can handle 1, 5, or 50 arguments flexibly.
    """
    print(f"Received {len(args)} arguments: {args}")
    return True

# Test Function 3: Function with variable keyword arguments
@logger_decorator
def accept_keyword_args(**kwargs):
    """
    This tests the decorator with functions that accept keyword arguments.
    This function also returns the decorator itself - testing complex return values.
    """
    print(f"Received keyword arguments: {kwargs}")
    return logger_decorator  # Returns the decorator function itself


if __name__ == "__main__":
    print("Testing the logger decorator with different function types...\n")
    
    # Test 1: Function with no parameters
    print("=== Testing simple function (no parameters) ===")
    simple_hello()
    
    # Test 2: Function with positional arguments
    print("\n=== Testing function with positional arguments ===")
    accept_many_args(1, 2, 3, "hello", [1, 2, 3])
    accept_many_args("single argument")
    
    # Test 3: Function with keyword arguments
    print("\n=== Testing function with keyword arguments ===")
    accept_keyword_args(name="Alice", age=30, city="New York")
    accept_keyword_args(debug=True, level="INFO")
    
    print("\n=== All tests completed! Check decorator.log for results ===")


    