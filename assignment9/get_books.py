# Import required libraries
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
import json
import time

# Task 3.1: Import libraries
# Task 3.2: Load web page
def get_books():
    # Setup Chrome driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    
    # Navigate to the Durham County Library search page
    url = "https://durhamcounty.bibliocommons.com/v2/search?query=learning%20spanish&searchType=smart"
    driver.get(url)
    
    # Task 3.3: Find all li elements with class for search results
    search_results = driver.find_elements(By.CSS_SELECTOR, "div.cp-search-result-item-content")
    print(f"Found {len(search_results)} search results")
    
    # Task 3.4: Create empty results list
    results = []
    
    # Task 3.5: Main loop - iterate through search results
    for result in search_results:
        # Get title
        title_element = result.find_element(By.CSS_SELECTOR, "h2.cp-title span.title-content")
        title = title_element.text
        print(f"Title: {title}")
        
        # Get author(s)
        author_elements = result.find_elements(By.CSS_SELECTOR, "span.cp-by-author-block a")
        authors = [author.text for author in author_elements]
        author = "; ".join(authors)
        print(f"Author: {author}")
        
        # Get format and year
        try:
            format_year_element = result.find_element(By.CSS_SELECTOR, "span.cp-screen-reader-message")
            format_year = format_year_element.text
            print(f"Format-Year: {format_year}")
        except:
            format_year = "Not available"
            print("Format-Year not found")
        
        # Create dict and append to results
        book_dict = {
            "Title": title,
            "Author": author,
            "Format-Year": format_year
        }
        results.append(book_dict)
    
    # Create DataFrame and print
    df = pd.DataFrame(results)
    print("\nDataFrame of all results:")
    print(df)
    
    # Task 4.1: Write DataFrame to CSV
    csv_path = 'get_books.csv'
    df.to_csv(csv_path, index=False)
    print(f"Saved data to {csv_path}")
    
    # Task 4.2: Write results list to JSON
    json_path = 'get_books.json'
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=4)
    print(f"Saved data to {json_path}")
    
    # Clean up
    driver.quit()
    
    return results, df

# Run the function
if __name__ == "__main__":
    results, df = get_books()