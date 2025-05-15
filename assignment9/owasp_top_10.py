from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import csv
import time

def get_owasp_top_10():
    # Setup Chrome driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    
    # Navigate to the OWASP Top 10 page
    url = "https://owasp.org/www-project-top-ten/"
    driver.get(url)
    
    # Wait for page to load
    time.sleep(3)
    
    # Find the vulnerability list items using XPath
    # The vulnerabilities are in <li> elements with links inside them
    vulnerability_elements = driver.find_elements(By.XPATH, "//ul/li/a[contains(@href, '/Top10/A')]")
    
    vulnerabilities = []
    
    # Process each vulnerability element
    for element in vulnerability_elements:
        title = element.text.strip()
        href = element.get_attribute("href")
        
        # Only include items that match the A01-A10 pattern
        if title and href and any(f"A0{i}" in title or f"A{i}" in title for i in range(1, 11)):
            vuln_dict = {
                "Title": title,
                "Link": href
            }
            vulnerabilities.append(vuln_dict)
            print(f"Found: {title} - {href}")
    
    # Sort vulnerabilities by their number (A01, A02, etc.)
    vulnerabilities.sort(key=lambda x: x["Title"])
    
    # Print the list to verify
    print("\nComplete list of vulnerabilities:")
    for i, vuln in enumerate(vulnerabilities, 1):
        print(f"{i}. {vuln['Title']} - {vuln['Link']}")
    
    # Write to CSV file
    csv_file = 'owasp_top_10.csv'
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["Title", "Link"])
        writer.writeheader()
        writer.writerows(vulnerabilities)
    
    print(f"\nData written to {csv_file}")
    
    # Close the browser
    driver.quit()
    
    return vulnerabilities

if __name__ == "__main__":
    get_owasp_top_10()