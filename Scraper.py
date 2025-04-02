from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re

def extract_email_from_text(text):
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text, re.IGNORECASE)
    return emails[0] if emails else 'N/A'

def scrape_website_for_email(driver, url):
    try:
        driver.get(url)
        time.sleep(random.uniform(1, 3))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # Look for "Contact Us" or similar links
        contact_link = soup.find('a', string=re.compile('contact|about|reach', re.I))
        if contact_link and 'href' in contact_link.attrs:
            contact_url = contact_link['href']
            if not contact_url.startswith('http'):
                contact_url = url.rstrip('/') + '/' + contact_url.lstrip('/')
            driver.get(contact_url)
            time.sleep(random.uniform(1, 2))
            soup = BeautifulSoup(driver.page_source, 'html.parser')
        return extract_email_from_text(soup.get_text(separator=' '))
    except Exception:
        return 'N/A'

def scrape_yellow_pages(search_term, location, max_pages=5):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--disable-webgl")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver_path = r"C:\Users\Home2\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
    service = Service(executable_path=driver_path)
    
    business_names = []
    phone_numbers = []
    addresses = []
    websites = []
    emails = []
    
    base_url = "https://www.yellowpages.com/search?search_terms={}&geo_location_terms={}&page={}"
    
    for page in range(1, max_pages + 1):
        url = base_url.format(search_term, location.replace(" ", "+"), page)
        print(f"Attempting to scrape: {url}")
        
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            driver.get(url)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "result"))
            )
            time.sleep(random.uniform(1, 3))
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            listings = soup.find_all('div', class_='result')
            
            if not listings or "Sorry, you have been blocked" in soup.text:
                print(f"Blocked or no results on page {page}. Saving source.")
                with open(f"page_{page}_source.html", "w", encoding="utf-8") as f:
                    f.write(soup.prettify())
                driver.quit()
                continue
                
            print(f"Found {len(listings)} listings on page {page}")
            for listing in listings:
                name = listing.find('a', class_='business-name') or listing.find('h2')
                business_names.append(name.text.strip() if name else 'N/A')

                phone = listing.find('div', class_='phones') or listing.find('div', class_='phone')
                phone_numbers.append(phone.text.strip() if phone else 'N/A')

                address = listing.find('div', class_='street-address') or listing.find('div', class_='adr')
                addresses.append(address.text.strip() if address else 'N/A')

                website = listing.find('a', class_='track-visit-website') or listing.find('a', {'data-analytics': re.compile('website')})
                website_url = website['href'] if website else 'N/A'
                websites.append(website_url)

                listing_text = listing.get_text(separator=' ')
                email = extract_email_from_text(listing_text)
                if email == 'N/A' and website_url != 'N/A':
                    email = scrape_website_for_email(driver, website_url)
                emails.append(email)

            print(f"Scraped page {page} successfully")
            driver.quit()
            
        except Exception as e:
            print(f"Error on page {page}: {e}")
            with open(f"page_{page}_error.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source if 'driver' in locals() else "No page source available")
            if 'driver' in locals():
                driver.quit()
            continue
    
    if 'driver' in locals():
        driver.quit()
    
    data = {
        'Business Name': business_names,
        'Phone Number': phone_numbers,
        'Address': addresses,
        'Website': websites,
        'Email': emails
    }
    df = pd.DataFrame(data)
    
    filename = f"yellow_pages_{search_term}_{location}.csv"
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    print(f"Total businesses scraped: {len(df)}")
    
    return df

if __name__ == "__main__":
    # Read local CSV
    df = pd.read_csv("cities.csv")  # Adjust path if needed
    search_term = "Home Builders"
    
    for _, row in df.iterrows():
        location = f"{row['City']}, {row['State']}"
        max_pages = get_max_pages(row['Population'])
        print(f"Scraping {location} (Population: {row['Population']}, Pages: {max_pages})")
        results = scrape_yellow_pages(search_term, location, max_pages)