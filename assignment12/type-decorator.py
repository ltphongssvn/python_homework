def type_converter(type_of_output):
    """
    This is the decorator factory - the outermost layer.
    
    Think of this as a factory that creates customized type converters.
    When we call type_converter(str), it creates a decorator that converts 
    everything to strings. When we call type_converter(int), it creates 
    a decorator that converts everything to integers.
    
    Args:
        type_of_output: The type to convert the function's result to (str, int, float, etc.)
    
    Returns:
        A decorator function that will convert results to the specified type
    """
    
    def decorator(func):
        """
        This is the middle layer - the actual decorator.
        
        This function receives the function that's being decorated.
        It creates and returns the wrapper that will actually do the work.
        
        Args:
            func: The function being decorated
            
        Returns:
            The wrapper function that converts the original function's result
        """
        
        def wrapper(*args, **kwargs):
            """
            This is the innermost layer - the wrapper that does the actual work.
            
            This function:
            1. Calls the original function with its arguments
            2. Takes whatever the function returns
            3. Converts it to the type specified in the outer layer
            4. Returns the converted result
            
            Args:
                *args: Positional arguments passed to the original function
                **kwargs: Keyword arguments passed to the original function
                
            Returns:
                The original function's result, converted to the specified type
            """
            # Call the original function and get its result
            x = func(*args, **kwargs)
            
            # Convert the result to the specified type
            # This is where the magic happens - type_of_output is "remembered"
            # from the outermost function call thanks to Python's closure mechanism
            return type_of_output(x)
        
        return wrapper
    return decorator


# Test Function 1: Converting an integer to a string
@type_converter(str)  # This creates a decorator that converts results to strings
def return_int():
    """
    This function normally returns an integer (5), but the decorator
    will convert it to a string before returning it to the caller.
    
    This demonstrates successful type conversion - integers can easily
    become strings ("5").
    """
    return 5

# Test Function 2: Attempting to convert a non-numeric string to an integer
@type_converter(int)  # This creates a decorator that tries to convert results to integers
def return_string():
    """
    This function returns a string that cannot be converted to an integer.
    
    This will demonstrate error handling - when Python tries to convert
    "not a number" to an integer, it will raise a ValueError because
    there's no meaningful way to turn those words into a number.
    """
    return "not a number"


if __name__ == "__main__":
    print("Testing the type converter decorator...\n")
    
    # Test 1: Successful type conversion (int to str)
    print("=== Test 1: Converting integer result to string ===")
    y = return_int()
    print(f"return_int() returned: {repr(y)}")  # repr() shows quotes around strings
    print(f"Type of result: {type(y).__name__}")  # Should print "str"
    print(f"Expected: str | Actual: {type(y).__name__} | Success: {type(y).__name__ == 'str'}\n")
    
    # Test 2: Failed type conversion with error handling
    print("=== Test 2: Attempting to convert non-numeric string to integer ===")
    try:
        y = return_string()
        print("shouldn't get here!")  # This line should never execute
    except ValueError as e:
        print("can't convert that string to an integer!")  # This is what should happen
        print(f"Detailed error: {e}")  # Show the actual error message
    
    print("\n=== Demonstration Complete ===")
    print("This shows how decorators can modify function behavior,")
    print("and how Python's type system handles conversion errors gracefully.")


