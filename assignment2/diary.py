"""A simple diary application that allows users to record
and save daily entries to a text file."""

# !/usr/bin/env python3
# Task 1: Diary
import traceback

try:
    # Open the diary.txt file for appending
    with open("diary.txt", "a", encoding="utf-8") as diary_file:
        # Initialize the first prompt
        PROMPT = "What happened today? "

        while True:
            try:
                # Get input from the user
                entry = input(PROMPT)

                # Check for exit condition
                if entry == "done for now":
                    diary_file.write(entry + "\n")
                    break

                # Write entry to the file
                diary_file.write(entry + "\n")

                # Update prompt for subsequent entries
                PROMPT = "What else? "

            except EOFError:
                # Handle the EOF signal (Ctrl+D on Unix, Ctrl+Z on Windows)
                print("\nEnd of input detected. Saving diary and exiting.")
                break

except Exception as e:
    trace_back = traceback.extract_tb(e.__traceback__)
    stack_trace = list()
    for trace in trace_back:
        # Split the long line into multiple lines using implicit continuation
        stack_trace.append(
            f'File : {trace[0]}, '
            f'Line : {trace[1]}, '
            f'Func.Name : {trace[2]}, '
            f'Message : {trace[3]}'
        )
    print(f"Exception type: {type(e).__name__}")
    MESSAGE = str(e)
    if MESSAGE:
        print(f"Exception message: {MESSAGE}")
    print(f"Stack trace: {stack_trace}")
