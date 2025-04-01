import requests
from bs4 import BeautifulSoup
import time
import csv
import pandas as pd # Optional: for easier data handling and CSV export

# --- Configuration ---
# WARNING: Use hypothetical structure/selectors. Actual YP selectors will differ and change.
# Scraping YP directly likely violates ToS and may not work due to anti-scraping measures.
BASE_URL = "https://www.yellowpages.com/search" # Replace with actual base URL if proceeding responsibly
SEARCH_TERM = "restaurants"
LOCATION = "San Antonio, TX"
# --- MODIFIED HEADERS ---
# Added common browser headers. This might help, but is NO guarantee against blocking.
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br', # requests handles decompression
    'Upgrade-Insecure-Requests': '1', # Signal preference for HTTPS
    'Connection': 'keep-alive', # Often default for requests, but doesn't hurt
     'Referer': 'https://www.yellowpages.com/', # Optional: Sometimes helps, set to base domain or previous page
}
# --- END MODIFIED HEADERS ---
MAX_PAGES = 3 # Limit the number of pages to scrape for this example
OUTPUT_FILE = 'scraped_data.csv'

# --- Main Scraping Logic ---
results = []
print(f"Attempting to scrape: {SEARCH_TERM} in {LOCATION}")
for page_num in range(1, MAX_PAGES + 1):
    print(f"Scraping page {page_num}...")

    # Construct URL for the current page (adjust params based on actual site)
    params = {
        'search_terms': SEARCH_TERM,
        'geo_location_terms': LOCATION,
        'page': page_num
    }

    try:
   # Make the HTTP GET request with updated headers and parameters
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=20) # Increased timeout slightly

        # Check the status code IMMEDIATELY
        print(f"Page {page_num} status code: {response.status_code}")

        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'lxml') # Using lxml pars

        # --- IMPORTANT: UPDATE THESE SELECTORS ---
        # Find all listing containers (adjust selector based on inspecting the *actual* YP site)
        listings = soup.find_all('div', class_='result') # Example: YP often uses 'result' or similar

        if not listings:
            print(f"No listings found using the specified selector on page {page_num}. Structure might have changed or selectors are wrong.")
            # Check response.text here to see the raw HTML YP returned - maybe it's a CAPTCHA page?
            # print(response.text[:1000]) # Print first 1000 chars of HTML for debugging
            break # Stop if no listings are found

        print(f"Found {len(listings)} potential listings on page {page_num}.")

        # Extract data from each listing (UPDATE ALL SELECTORS)
        for listing in listings:
            name = listing.find('a', class_='business-name') # Hypothetical - UPDATE
            phone = listing.find('div', class_='phones phone primary') # Hypothetical - UPDATE
            address_locality = listing.find('span', itemprop='addressLocality') # Hypothetical - UPDATE
            address_street = listing.find('span', itemprop='streetAddress') # Hypothetical - UPDATE
            website_tag = listing.find('a', class_='track-visit-website') # Hypothetical - UPDATE

            # Get text safely, use .strip() to remove leading/trailing whitespace
            name_text = name.text.strip() if name else 'N/A'
            phone_text = phone.text.strip() if phone else 'N/A'

            # Combine address parts if found separately
            address_text = 'N/A'
            if address_street and address_locality:
                 address_text = f"{address_street.text.strip()}, {address_locality.text.strip().replace(',', '')}" # Basic combination
            elif address_street:
                 address_text = address_street.text.strip()
            elif address_locality:
                 address_text = address_locality.text.strip()


            # Get URL from 'href' attribute
            website_url = website_tag['href'].strip() if website_tag and website_tag.has_attr('href') else 'N/A'
            # YP links might be relative or tracked, may need cleaning/joining with base URL

            print(f"  - Found: {name_text} | {phone_text} | {address_text} | {website_url}") # Debug print

            # Append data to results list
            results.append({
                'Name': name_text,
                'Phone': phone_text,
                'Address': address_text,
                'Website': website_url
            })
        # --- END SELECTOR UPDATES NEEDED ---

        # --- Be Polite: Wait before scraping the next page ---
        print(f"Finished page {page_num}. Waiting...")
        time.sleep(7) # Increased delay slightly (be respectful of server load)

    # Catch specific HTTP errors like 403
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching page {page_num}: {e}")
        print("This often means the website blocked the request (e.g., 403 Forbidden).")
        print("Consider checking Terms of Service or using an official API if available.")
        # Print some of the response content if it's an error page
        if e.response is not None:
            print("Response content snippet:")
            print(e.response.text[:1000]) # Print first 1000 chars
        break # Stop processing on significant HTTP errors
    # Catch other request exceptions (network issues, timeouts, DNS errors)
    except requests.exceptions.RequestException as e:
        print(f"Network/Request Error fetching page {page_num}: {e}")
        break # Stop processing on network errors
    except Exception as e:
        print(f"An unexpected error occurred while processing page {page_num}: {e}")
        # Log the error, maybe save the HTML for debugging
        break # Stop on unexpected parsing errors

# --- Save Data ---
if results:
    print(f"\nTotal listings scraped: {len(results)}")
    try:
        df = pd.DataFrame(results)
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"Data saved successfully to {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")
else:
    print("\nNo data was successfully scraped.")
