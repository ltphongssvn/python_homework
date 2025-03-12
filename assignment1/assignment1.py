# Write your code here.

# Task 1: Hello
def hello():
    """
    A simple function that takes no arguments and returns the greeting 'Hello!'.

    Returns:
        str: The string 'Hello!'
    """
    return "Hello!"


# Task 2: Greet with a Formatted String
def greet(name):
    """
    Greets a person by name using a formatted string.

    Args:
        name (str): The name of the person to greet

    Returns:
        str: A personalized greeting in the format "Hello, Name!"
    """
    return f"Hello, {name}!"


# Task 3: Calculator
def calc(a, b, operation="multiply"):
    """
    Performs various arithmetic operations on two values.

    Args:
        a: First value
        b: Second value
        operation (str): The operation to perform. Default is "multiply".
            Valid operations: add, subtract, multiply, divide, modulo, int_divide, power

    Returns:
        The result of the operation, or an error message if the operation fails
    """
    try:
        match operation:
            case "add":
                return a + b
            case "subtract":
                return a - b
            case "multiply":
                return a * b
            case "divide":
                return a / b
            case "modulo":
                return a % b
            case "int_divide":
                return a // b
            case "power":
                return a ** b
            case _:
                return "Unknown operation"
    except ZeroDivisionError:
        return "You can't divide by 0!"
    except TypeError:
        return f"You can't {operation} those values!"


# Task 4: Data Type Conversion
def data_type_conversion(value, type_name):
    """
    Converts a value to the specified data type.

    Args:
        value: The value to convert
        type_name (str): The data type to convert to (float, str, or int)

    Returns:
        The converted value, or an error message if conversion fails
    """
    try:
        if type_name == "float":
            return float(value)
        elif type_name == "str":
            return str(value)
        elif type_name == "int":
            return int(value)
        else:
            return f"Unsupported conversion type: {type_name}"
    except (ValueError, TypeError):
        return f"You can't convert {value} into a {type_name}."


# Task 5: Grading System, Using *args
def grade(*args):
    """
    Calculates a letter grade based on the average of numerical scores.

    Args:
        *args: Variable number of numerical scores

    Returns:
        str: A letter grade based on the average score:
            A: 90 and above
            B: 80-89
            C: 70-79
            D: 60-69
            F: Below 60
        Or an error message if invalid data is provided
    """
    try:
        # Calculate the average of all scores
        average = sum(args) / len(args)

        # Determine the letter grade based on the average
        if average >= 90:
            return "A"
        elif average >= 80:
            return "B"
        elif average >= 70:
            return "C"
        elif average >= 60:
            return "D"
        else:
            return "F"
    except (TypeError, ZeroDivisionError):
        return "Invalid data was provided."


# Task 6: Use a For Loop with a Range
def repeat(string, count):
    """
    Repeats a string a specified number of times using a for loop.

    Args:
        string (str): The string to repeat
        count (int): The number of times to repeat the string

    Returns:
        str: The input string repeated count times
    """
    result = ""
    for i in range(count):
        result += string
    return result


# Task 7: Student Scores, Using **kwargs
def student_scores(operation, **kwargs):
    """
    Processes student scores based on the requested operation.

    Args:
        operation (str): Either "best" or "mean"
        **kwargs: Keyword arguments with student names as keys and scores as values

    Returns:
        str or float: The name of the best student if operation is "best",
                     or the mean score if operation is "mean"
    """
    if not kwargs:
        return 0

    if operation == "best":
        # Find the student with the highest score
        best_student = ""
        best_score = -1

        for student, score in kwargs.items():
            if score > best_score:
                best_score = score
                best_student = student

        return best_student

    elif operation == "mean":
        # Calculate the average of all scores
        total = sum(kwargs.values())
        count = len(kwargs)
        return total / count


# Task 8: Titleize, with String and List Operations
def titleize(text):
    """
    Capitalizes a string as if it were a book title.

    Rules:
    1. The first word is always capitalized
    2. The last word is always capitalized
    3. All other words are capitalized, except for certain small words

    Args:
        text (str): The input string to titleize

    Returns:
        str: The titleized string
    """
    # List of small words that aren't capitalized (except as first or last word)
    little_words = ["a", "on", "an", "the", "of", "and", "is", "in"]

    # Split the input string into words
    words = text.split()

    # Process each word according to the rules
    for i, word in enumerate(words):
        # Capitalize first word, last word, or words not in the little_words list
        if i == 0 or i == len(words) - 1 or word not in little_words:
            words[i] = word.capitalize()
        else:
            words[i] = word.lower()

    # Join the words back into a string and return
    return " ".join(words)


# Task 9: Hangman, with more String Operations
def hangman(secret, guess):
    """
    Implements a simple hangman game function.

    Args:
        secret (str): The word to guess
        guess (str): A string containing all the letters that have been guessed

    Returns:
        str: A string where characters in secret that are also in guess remain,
             and all other characters are replaced with underscores
    """
    result = ""

    # Process each character in the secret word
    for char in secret:
        if char in guess:
            # If the character has been guessed, keep it
            result += char
        else:
            # Otherwise, replace it with an underscore
            result += "_"

    return result


# Task 10: Pig Latin, Another String Manipulation Exercise
def pig_latin(text):
    """
    Converts English text to Pig Latin.

    Rules:
    1. If a word starts with a vowel, add "ay" to the end
    2. If a word starts with consonants, move them to the end and add "ay"
    3. "qu" is treated as a single consonant unit

    Args:
        text (str): The English text to convert

    Returns:
        str: The Pig Latin translation
    """
    vowels = "aeiou"
    words = text.split()
    pig_latin_words = []

    for word in words:
        # Case 1: Word starts with a vowel
        if word[0] in vowels:
            pig_latin_words.append(word + "ay")
        # Case 3: Word starts with "qu"
        elif word.startswith("qu"):
            pig_latin_words.append(word[2:] + "quay")
        # Case 2: Word starts with consonants
        else:
            # Find the index of the first vowel
            vowel_index = 0
            for i, char in enumerate(word):
                if char in vowels:
                    # Special case for "qu"
                    if char == 'u' and i > 0 and word[i-1] == 'q':
                        continue
                    vowel_index = i
                    break

            # Move consonants to the end and add "ay"
            if vowel_index > 0:
                pig_latin_words.append(word[vowel_index:] + word[:vowel_index] + "ay")
            else:
                # If no vowels found, just add "ay"
                pig_latin_words.append(word + "ay")

    # Join the translated words and return
    return " ".join(pig_latin_words)


# Test print statements for debugging purposes
if __name__ == "__main__":
    print("Task 1 - Hello function:")
    print(hello())

    print("\nTask 2 - Greet function:")
    print(greet("John"))

    print("\nTask 3 - Calculator function:")
    print(calc(5, 3))  # Default multiply
    print(calc(10, 2, "divide"))
    print(calc(10, 0, "divide"))  # Division by zero
    print(calc("hello", 3, "multiply"))  # Type error

    print("\nTask 4 - Data Type Conversion function:")
    print(data_type_conversion("123", "int"))
    print(data_type_conversion("3.14", "float"))
    print(data_type_conversion("hello", "float"))  # Error case

    print("\nTask 5 - Grading function:")
    print(grade(85, 90, 78, 92))
    print(grade("A", 90))  # Error case

    print("\nTask 6 - Repeat function:")
    print(repeat("abc", 3))

    print("\nTask 7 - Student Scores function:")
    print(student_scores("best", Alice=85, Bob=92, Charlie=78))
    print(student_scores("mean", Alice=85, Bob=92, Charlie=78))

    print("\nTask 8 - Titleize function:")
    print(titleize("the quick brown fox jumps over the lazy dog"))

    print("\nTask 9 - Hangman function:")
    print(hangman("alphabet", "ab"))

    print("\nTask 10 - Pig Latin function:")
    print(pig_latin("hello world"))
    print(pig_latin("apple banana"))
    print(pig_latin("quick question"))