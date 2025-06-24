import pandas as pd
import os

# Let's first verify the data file exists and understand its structure
def explore_employee_data():
    """
    This function helps us understand the data before we start processing it.
    A seasoned developer always examines their data first!
    """
    csv_path = "../csv/employees.csv"
    
    # Check if the file exists
    if not os.path.exists(csv_path):
        print(f"Warning: {csv_path} not found!")
        print("Make sure we have the csv folder with employees.csv in the project structure")
        return None
    
    # Load and examine the data
    df = pd.read_csv(csv_path)
    print("Employee data loaded successfully!")
    print(f"Dataset contains {len(df)} employees")
    print(f"Columns available: {list(df.columns)}")
    print("\nFirst few rows:")
    print(df.head())
    print("\n" + "="*60)
    
    return df

# Load the employee data
print("Loading and exploring employee data...")
df = explore_employee_data()

if df is not None:
    print("\nUnderstanding df.iterrows() - the foundation of the list comprehensions:")
    print("Each iteration gives us a tuple: (index, row_data)")
    print("Let's examine the first few iterations:\n")
    
    # Look at the first 3 rows to understand the structure
    for i, (index, row) in enumerate(df.iterrows()):
        if i >= 3:  # Only show first 3 for clarity
            break
        print(f"Iteration {i}:")
        print(f"  Index: {index}")
        print(f"  Row type: {type(row)}")
        print(f"  First name: {row['first_name'] if 'first_name' in row else 'N/A'}")
        print(f"  Last name: {row['last_name'] if 'last_name' in row else 'N/A'}")
        print(f"  Full row data: {dict(row)}")
        print()

    print("="*60)
    print("TASK 1: Creating full names using list comprehension")
    print("="*60)
    
    # The list comprehension magic happens here!
    # Let's break down the syntax: [expression for item in iterable]
    # In this case: [row['first_name'] + ' ' + row['last_name'] for index, row in df.iterrows()]
    
    employee_full_names = [
        row['first_name'] + ' ' + row['last_name'] 
        for index, row in df.iterrows()
    ]
    
    print(f"Generated {len(employee_full_names)} full names:")
    print("Full employee names list:")
    for i, name in enumerate(employee_full_names, 1):
        print(f"  {i:2d}. {name}")
    
    print(f"\nList comprehension result: {employee_full_names}")

    print("\n" + "="*60)
    print("TASK 2: Filtering names containing the letter 'e'")
    print("="*60)
    
    # This list comprehension includes a condition (filter)
    # Syntax: [expression for item in iterable if condition]
    # We're filtering the full names we just created, keeping only those containing 'e'
    
    names_with_e = [
        name 
        for name in employee_full_names 
        if 'e' in name.lower()  # Using .lower() to catch both 'e' and 'E'
    ]
    
    print(f"Found {len(names_with_e)} names containing the letter 'e':")
    print("Names with 'e' (case-insensitive):")
    for i, name in enumerate(names_with_e, 1):
        print(f"  {i:2d}. {name}")
    
    print(f"\nFiltered list result: {names_with_e}")
    
    # Let's also show which names were filtered OUT for educational purposes
    names_without_e = [
        name 
        for name in employee_full_names 
        if 'e' not in name.lower()
    ]
    
    print(f"\nFor comparison, {len(names_without_e)} names do NOT contain 'e':")
    for i, name in enumerate(names_without_e, 1):
        print(f"  {i:2d}. {name}")

    print("\n" + "="*60)
    print("EDUCATIONAL COMPARISON: List Comprehensions vs Traditional Loops")
    print("="*60)
    
    # Traditional loop approach for creating full names
    traditional_full_names = []
    for index, row in df.iterrows():
        full_name = row['first_name'] + ' ' + row['last_name']
        traditional_full_names.append(full_name)
    
    # Traditional loop approach for filtering names with 'e'
    traditional_filtered_names = []
    for name in traditional_full_names:
        if 'e' in name.lower():
            traditional_filtered_names.append(name)
    
    # Verify both approaches give identical results
    print("Verification that both approaches produce identical results:")
    print(f"List comprehension full names == Traditional loop: {employee_full_names == traditional_full_names}")
    print(f"List comprehension filtered names == Traditional loop: {names_with_e == traditional_filtered_names}")
    
    print("\nWhy list comprehensions are preferred:")
    print("• More concise: 1 line vs 3-4 lines")
    print("• More readable: reads like natural language")
    print("• Often faster: optimized at the C level in Python")
    print("• Less error-prone: fewer opportunities for bugs")
    print("• More Pythonic: follows Python's principle of elegant simplicity")

else:
    print("Cannot proceed with list comprehensions due to missing data file.")
    print("Please ensure ../csv/employees.csv exists and contains the required columns:")
    print("- first_name")
    print("- last_name")
    
print("\n" + "="*60)
print("ADVANCED INSIGHTS: List Comprehension Best Practices")
print("="*60)

if df is not None:
    # Demonstrate error-handling in list comprehensions
    print("Professional tip: Handling potential missing data gracefully")
    
    # This comprehension handles cases where names might be missing (NaN values)
    robust_full_names = [
        f"{row.get('first_name', 'Unknown')} {row.get('last_name', 'Unknown')}"
        for index, row in df.iterrows()
        if pd.notna(row.get('first_name')) and pd.notna(row.get('last_name'))
    ]
    
    print(f"Robust name generation handled {len(robust_full_names)} valid records")
    
    # Demonstrate nested conditions for complex filtering
    print("\nAdvanced filtering: Names with 'e' that are also longer than 10 characters")
    complex_filtered_names = [
        name 
        for name in employee_full_names 
        if 'e' in name.lower() and len(name) > 10
    ]
    
    print(f"Found {len(complex_filtered_names)} names with 'e' and length > 10:")
    for name in complex_filtered_names:
        print(f"  - {name} (length: {len(name)})")

print("\nList comprehension mastery achieved! 🎉")