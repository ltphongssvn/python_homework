#!/usr/bin/env python3
# Task 1: Diary
import traceback

try:
    # Open the diary.txt file for appending
    with open("diary.txt", "a") as diary_file:
        # Initialize the first prompt
        prompt = "What happened today? "

        while True:
            # Get input from the user
            entry = input(prompt)

            # Check for exit condition
            if entry == "done for now":
                diary_file.write(entry + "\n")
                break

            # Write entry to the file
            diary_file.write(entry + "\n")

            # Update prompt for subsequent entries
            prompt = "What else? "

except Exception as e:
    trace_back = traceback.extract_tb(e.__traceback__)
    stack_trace = list()
    for trace in trace_back:
        stack_trace.append(f'File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}')
    print(f"Exception type: {type(e).__name__}")
    message = str(e)
    if message:
        print(f"Exception message: {message}")
    print(f"Stack trace: {stack_trace}")