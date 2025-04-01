import asyncio
import logging
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import aiohttp
import pandas as pd
from lxml import etree, html  # Use lxml directly for parsing

# Assuming these tools exist and work as expected:
from tools.functionalities import (
    userAgents,      # Function returning a random User-Agent string
    randomTime,      # Function returning a random float (seconds) for sleep
    # verify_yellow, # Commented out as its usage was unclear/commented
    yaml_by_select,  # Function to load selectors from YAML based on a key
    yp_lists,        # Function taking search URL -> returns list of page URLs
    create_path      # Function to create directory if it doesn't exist
)

# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Limit concurrent requests to avoid overwhelming the server or getting blocked
CONCURRENT_REQUEST_LIMIT = 10
# Load selectors once
SELECTORS = yaml_by_select('selectors')
OUTPUT_DIR = Path('Yellowpage_database')

# --- Helper Function for Fetching and Parsing ---

async def fetch_and_parse(session: aiohttp.ClientSession, url: str) -> Optional[etree._Element]:
    """Fetches a URL and parses it into an lxml HTML element tree."""
    try:
        headers = {'User-Agent': userAgents()}
        async with session.get(url, headers=headers, timeout=20) as response:
            response.raise_for_status()  # Raise exception for bad status codes (4xx or 5xx)
            content = await response.text()
            if not content:
                logging.warning(f"Empty content received from {url}")
                return None
            # Use lxml.html.fromstring for efficient parsing
            tree = html.fromstring(content)
            return tree
    except aiohttp.ClientError as e:
        logging.error(f"Network error fetching {url}: {e}")
        return None
    except asyncio.TimeoutError:
        logging.error(f"Timeout fetching {url}")
        return None
    except Exception as e:
        logging.error(f"Error parsing {url}: {e}")
        return None

# --- Core Scraping Functions ---

async def get_business_urls_from_page(
    session: aiohttp.ClientSession,
    search_page_url: str,
    semaphore: asyncio.Semaphore
) -> Tuple[List[str], Optional[str]]:
    """Fetches a search results page and extracts business URLs and category."""
    async with semaphore: # Acquire semaphore before making request
        logging.info(f"Fetching business URLs from: {search_page_url}")
        tree = await fetch_and_parse(session, search_page_url)
        if tree is None:
            return [], None

        business_links = []
        category = None

        try:
            # Extract category (adjust selector as needed)
            # Using ''.join() handles cases where xpath returns multiple elements
            category_elements = tree.xpath(SELECTORS['categories'])
            if category_elements:
                 # Join text content of all found category elements
                category = ' '.join(el.text_content() for el in category_elements).strip()

            # Extract business URLs
            raw_links = tree.xpath(SELECTORS['business_urls'])
            business_links = [f"https://www.yellowpages.com{link}" for link in raw_links if isinstance(link, str)]

            # Check for "No results" (adjust selector as needed)
            page_content_elements = tree.xpath(SELECTORS['page_content'])
            page_content = ' '.join(el.text_content() for el in page_content_elements).strip()
            if re.search("^No results found for.*", page_content, re.IGNORECASE):
                logging.info(f"'No results found' detected on {search_page_url}")
                # Return empty list but potentially valid category if found
                return [], category

            # Add a small delay after processing a page
            await asyncio.sleep(randomTime(0.5, 1.5)) # Example: sleep 0.5-1.5s

        except etree.XPathEvalError as e:
            logging.error(f"XPath error on {search_page_url}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error processing {search_page_url}: {e}")

        return business_links, category


async def scrape_business_details(
    session: aiohttp.ClientSession,
    business_url: str,
    semaphore: asyncio.Semaphore
) -> Optional[Dict[str, Any]]:
    """Fetches a business details page and extracts information."""
    async with semaphore: # Acquire semaphore
        bizz_name_print = ' '.join(business_url.split("/")[-1].split("?")[0].split("-")[:-1])
        logging.info(f"Scraping details for: {bizz_name_print} ({business_url})")

        tree = await fetch_and_parse(session, business_url)
        if tree is None:
            return None

        details = {}
        try:
            # Helper to extract text safely
            def safe_xpath_get_text(xpath_selector: str, default: str = "") -> str:
                elements = tree.xpath(xpath_selector)
                # Join text content of all matching elements, strip whitespace
                return ' '.join(el.text_content() for el in elements).strip() if elements else default

            # Helper to extract attribute safely
            def safe_xpath_get_attrib(xpath_selector: str, attrib: str, default: str = "") -> str:
                 elements = tree.xpath(xpath_selector)
                 return elements[0].get(attrib, default).strip() if elements else default


            details = {
                "Business": safe_xpath_get_text(SELECTORS['business_name']),
                "Contact": safe_xpath_get_text(SELECTORS['contact']),
                # Email often needs specific handling (e.g., might be obfuscated)
                "Email": safe_xpath_get_text(SELECTORS['email']).replace("mailto:", ""),
                "Address": safe_xpath_get_text(SELECTORS['address']),
                "Map and direction": f"https://www.yellowpages.com{safe_xpath_get_attrib(SELECTORS['map_and_direction'], 'href')}",
                "Review": safe_xpath_get_attrib(SELECTORS['review'], 'class').replace("rating-stars ", ""), # Example if rating is in class
                "Review count": re.sub(r"[()]", "", safe_xpath_get_text(SELECTORS['review_count'])),
                "Hyperlink": business_url,
                "Images": safe_xpath_get_attrib(SELECTORS['images'], 'src'), # Assuming single image src
                "Website": safe_xpath_get_attrib(SELECTORS['website'], 'href'),
            }
            # Add a small delay
            await asyncio.sleep(randomTime(0.3, 1.0)) # Shorter delay for detail pages?

        except etree.XPathEvalError as e:
            logging.error(f"XPath error scraping {business_url}: {e}")
            return None # Indicate failure
        except Exception as e:
            logging.error(f"Unexpected error scraping {business_url}: {e}")
            return None # Indicate failure

        # Return None if essential info (like name) is missing
        return details if details.get("Business") else None


# --- Main Orchestration Function ---

async def run_scraper(start_url: str):
    """Main function to orchestrate the scraping process."""
    logging.info(f"Starting Yellow Pages scrape for: {start_url}")

    # Use a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)

    # Use a single session for connection pooling
    async with aiohttp.ClientSession() as session:

        # 1. Get all search result page URLs (assuming yp_lists is synchronous)
        try:
            search_page_urls = yp_lists(start_url)
            if not search_page_urls:
                logging.error(f"Could not find any search result pages for {start_url}. Exiting.")
                return
            logging.info(f"Found {len(search_page_urls)} search result pages.")
        except Exception as e:
            logging.error(f"Error getting search page list from yp_lists: {e}")
            return

        # 2. Fetch all business URLs concurrently from search pages
        tasks_get_urls = [
            asyncio.create_task(get_business_urls_from_page(session, page_url, semaphore))
            for page_url in search_page_urls
        ]
        results_urls = await asyncio.gather(*tasks_get_urls)

        all_business_urls = set() # Use set to avoid duplicates
        categories_found = []
        for business_urls, category in results_urls:
            if business_urls: # Only add if list is not empty
                 all_business_urls.update(business_urls)
            if category:
                categories_found.append(category)

        if not all_business_urls:
            logging.warning("No business URLs found after checking all search pages.")
            return

        # Determine category name (e.g., use the first one found, or the most common)
        category_name = categories_found[0] if categories_found else "Unknown_Category"
        # Sanitize category name for filename
        sanitized_category = re.sub(r'[\\/*?:"<>|]', "", category_name).strip()
        if not sanitized_category:
            sanitized_category = "General_Scrape"
        logging.info(f"Determined category: {sanitized_category}")
        logging.info(f"Found {len(all_business_urls)} unique business URLs to scrape.")


        # 3. Scrape details for each business URL concurrently
        tasks_scrape_details = [
            asyncio.create_task(scrape_business_details(session, biz_url, semaphore))
            for biz_url in all_business_urls
        ]
        results_details = await asyncio.gather(*tasks_scrape_details)

        # Filter out None results (errors during scraping)
        final_data = [details for details in results_details if details is not None]

        if not final_data:
            logging.warning("No business details could be scraped successfully.")
            return

        logging.info(f"Successfully scraped details for {len(final_data)} businesses.")

        # 4. Save results to Excel
        try:
            create_path(OUTPUT_DIR) # Ensure output directory exists
            output_file = OUTPUT_DIR / f"{sanitized_category}.xlsx"
            df = pd.DataFrame(final_data)
            df.to_excel(output_file, index=False)
            logging.info(f"Scraping complete. Data saved to: {output_file}")
        except Exception as e:
            logging.error(f"Failed to save data to Excel: {e}")


# --- Entry Point ---

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python your_script_name.py <yellow_pages_start_url>")
        # Example default for testing:
        start_url = "https://www.yellowpages.com/search?search_terms=pizza&geo_location_terms=New+York%2C+NY"
        print(f"No URL provided, using default: {start_url}")
    else:
        start_url = sys.argv[1]

    # Run the main async function
    try:
        asyncio.run(run_scraper(start_url))
    except KeyboardInterrupt:
        logging.info("Scraping interrupted by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in main execution: {e}")