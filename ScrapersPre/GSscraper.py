import asyncio
import logging
import random # Added
import re
import sys
from pathlib import Path # Ensured Path is imported
from typing import Optional, Dict, Any, List, Tuple

# Third-Party Imports
import aiohttp
import pandas as pd
import yaml       # Added (requires PyYAML: pip install PyYAML)
from lxml import etree, html

# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Limit concurrent requests to avoid overwhelming the server or getting blocked
CONCURRENT_REQUEST_LIMIT = 2
OUTPUT_DIR = Path('Yellowpage_database') # Output directory Path object

# --- Integrated Utility Functions ---

def yp_lists(yp_url: str) -> list[str]:
    """Generates a list of Yellow Pages search result page URLs."""
    # Note: Hardcoding 101 pages might be too many or too few.
    # A more robust scraper might determine the actual number of pages first.
    total_page_urls = [f"{yp_url}&page={num}" for num in range(1, 10)]
    return total_page_urls

def randomTime(min_sec: float, max_sec: float) -> float:
    """Generates a random float sleep time between min_sec and max_sec."""
    # Replaced the original index-based function with a more typical float generator
    return random.uniform(min_sec, max_sec)

def userAgents() -> str:
    """Selects a random User-Agent string from a file."""
    # CRITICAL: Requires 'user-agents.txt' file in the script's running directory (CWD).
    # Download a list from https://github.com/tamimibrahim17/List-of-user-agents if needed.
    ua_file = Path.cwd() / 'user-agents.txt'
    try:
        with open(ua_file) as f:
            agents = f.read().splitlines() # Use splitlines() to avoid empty strings from trailing newline
            return random.choice([agent for agent in agents if agent]) # Ensure non-empty agent
    except FileNotFoundError:
        logging.error(f"CRITICAL: User agents file not found at {ua_file}. Please create 'user-agents.txt'.")
        # Return a default or raise an error
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" # Example default
    except Exception as e:
        logging.error(f"Error reading user agents file {ua_file}: {e}")
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" # Example default


def verify_yellow(yp_url: str) -> bool:
    """Checks if a URL appears to be a valid Yellow Pages URL format."""
    # Returns True if INVALID, False if potentially VALID
    yp_pattern = re.search(r"""^(https://|www\.|)yellowpages\.com/.+""", yp_url, re.IGNORECASE)
    return yp_pattern is None # Simpler return

def yaml_by_select(selectors_filename_key: str) -> Optional[Dict[str, Any]]:
    """Loads YAML data, finding the file relative to the script's CWD."""
    # CRITICAL: Expects 'scrapers/{selectors_filename_key}.yml' relative to where the script is run.
    # e.g., run `python your_script.py` from `C:\Project`, it looks for `C:\Project\scrapers\selectors.yml`
    base_dir = Path.cwd() # Current Working Directory
    yaml_path = base_dir / "scrapers" / f"{selectors_filename_key}.yml"
    logging.info(f"Attempting to load selectors from: {yaml_path}")
    try:
        with open(yaml_path) as file:
            data = yaml.safe_load(file) # Use safe_load
            return data # Return the entire loaded dictionary
    except FileNotFoundError:
        logging.error(f"CRITICAL: YAML file not found at {yaml_path}. Ensure 'scrapers/{selectors_filename_key}.yml' exists.")
        return None # Return None to indicate failure
    except yaml.YAMLError as e:
        logging.error(f"CRITICAL: Error parsing YAML file {yaml_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"CRITICAL: An unexpected error occurred loading YAML: {e}")
        return None

def create_path(dir_path: Path):
    """Creates a directory including parent directories if it doesn't exist."""
    try:
        dir_path.mkdir(parents=True, exist_ok=True) # Create parents, ignore if exists
        logging.info(f"Ensured output directory exists: {dir_path}")
    except OSError as e:
        logging.error(f"Failed to create directory {dir_path}: {e}")
        raise # Re-raise error as this might be critical

# --- Load Selectors ---
# Load selectors once using the integrated function
SELECTORS_DATA = yaml_by_select('selectors')
if SELECTORS_DATA is None:
     logging.critical("Failed to load selectors YAML. Exiting.")
     sys.exit(1) # Stop execution if selectors aren't loaded

# Assuming the YAML structure is {'selectors': { 'key1': 'xpath1', ...}}
# Or perhaps just { 'key1': 'xpath1', ... } directly in the file.
# Adjust this based on your actual selectors.yml structure:
if 'selectors' in SELECTORS_DATA:
    SELECTORS = SELECTORS_DATA['selectors']
else:
    # If the file *directly* contains the selectors map:
    SELECTORS = SELECTORS_DATA

# --- Helper Function for Fetching and Parsing ---

async def fetch_and_parse(session: aiohttp.ClientSession, url: str) -> Optional[etree._Element]:
    """Fetches a URL and parses it into an lxml HTML element tree."""
    # --- Add delay BEFORE the request ---
    sleep_time = randomTime(2.0, 5.0) # Sleep for 2-5 seconds BEFORE request
    logging.debug(f"Sleeping for {sleep_time:.2f}s before fetching {url}")
    await asyncio.sleep(sleep_time)
    try:
        headers = {'User-Agent': userAgents()}
        async with session.get(url, headers=headers, timeout=20) as response:
            response.raise_for_status()
            content = await response.text()
            if not content:
                logging.warning(f"Empty content received from {url}")
                return None
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
    async with semaphore:
        logging.info(f"Fetching business URLs from: {search_page_url}")
        tree = await fetch_and_parse(session, search_page_url)
        if tree is None:
            return [], None

        business_links = []
        category = None

        try:
            category_elements = tree.xpath(SELECTORS['categories'])
            if category_elements:
                category = ' '.join(el.text_content() for el in category_elements).strip()

            raw_links = tree.xpath(SELECTORS['business_urls'])
            business_links = [f"https://www.yellowpages.com{link}" for link in raw_links if isinstance(link, str)]

            page_content_elements = tree.xpath(SELECTORS['page_content'])
            page_content = ' '.join(el.text_content() for el in page_content_elements).strip()
            if re.search("^No results found for.*", page_content, re.IGNORECASE):
                logging.info(f"'No results found' detected on {search_page_url}")
                return [], category

            await asyncio.sleep(randomTime(0.5, 1.5)) # Use integrated randomTime

        except KeyError as e:
             logging.error(f"Selector key missing: {e} on {search_page_url}. Check selectors.yml.")
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
    async with semaphore:
        bizz_name_print = ' '.join(business_url.split("/")[-1].split("?")[0].split("-")[:-1])
        logging.info(f"Scraping details for: {bizz_name_print} ({business_url})")

        tree = await fetch_and_parse(session, business_url)
        if tree is None:
            return None

        details = {}
        try:
            def safe_xpath_get_text(xpath_selector: str, default: str = "") -> str:
                elements = tree.xpath(xpath_selector)
                return ' '.join(el.text_content() for el in elements).strip() if elements else default

            def safe_xpath_get_attrib(xpath_selector: str, attrib: str, default: str = "") -> str:
                 elements = tree.xpath(xpath_selector)
                 return elements[0].get(attrib, default).strip() if elements else default

            details = {
                "Business": safe_xpath_get_text(SELECTORS['business_name']),
                "Contact": safe_xpath_get_text(SELECTORS['contact']),
                "Email": safe_xpath_get_text(SELECTORS['email']).replace("mailto:", ""),
                "Address": safe_xpath_get_text(SELECTORS['address']),
                "Map and direction": f"https://www.yellowpages.com{safe_xpath_get_attrib(SELECTORS['map_and_direction'], 'href')}",
                "Review": safe_xpath_get_attrib(SELECTORS['review'], 'class').replace("rating-stars ", ""),
                "Review count": re.sub(r"[()]", "", safe_xpath_get_text(SELECTORS['review_count'])),
                "Hyperlink": business_url,
                "Images": safe_xpath_get_attrib(SELECTORS['images'], 'src'),
                "Website": safe_xpath_get_attrib(SELECTORS['website'], 'href'),
            }
            await asyncio.sleep(randomTime(0.3, 1.0)) # Use integrated randomTime

        except KeyError as e:
             logging.error(f"Selector key missing: {e} scraping {business_url}. Check selectors.yml.")
             return None
        except etree.XPathEvalError as e:
            logging.error(f"XPath error scraping {business_url}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error scraping {business_url}: {e}")
            return None

        return details if details.get("Business") else None


# --- Main Orchestration Function ---

async def run_scraper(start_url: str):
    """Main function to orchestrate the scraping process."""
    logging.info(f"Starting Yellow Pages scrape for: {start_url}")

    # Optional: Verify start URL format (if needed)
    # if verify_yellow(start_url):
    #     logging.error(f"Invalid start URL format: {start_url}")
    #     return

    semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)
    async with aiohttp.ClientSession() as session:

        # 1. Get search result page URLs
        try:
            search_page_urls = yp_lists(start_url)
            if not search_page_urls:
                logging.error(f"Could not generate any search result pages for {start_url}. Exiting.")
                return
            logging.info(f"Generated {len(search_page_urls)} search result page URLs to check.")
        except Exception as e:
            logging.error(f"Error generating search page list: {e}")
            return

        # 2. Fetch all business URLs concurrently
        tasks_get_urls = [
            asyncio.create_task(get_business_urls_from_page(session, page_url, semaphore))
            for page_url in search_page_urls
        ]
        results_urls = await asyncio.gather(*tasks_get_urls)

        all_business_urls = set()
        categories_found = []
        for business_urls, category in results_urls:
            if business_urls:
                 all_business_urls.update(business_urls)
            if category and category not in categories_found: # Collect unique categories
                categories_found.append(category)

        if not all_business_urls:
            logging.warning("No business URLs found after checking all search pages.")
            return

        # Determine category name
        category_name = categories_found[0] if categories_found else "Unknown_Category"
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

        final_data = [details for details in results_details if details is not None]

        if not final_data:
            logging.warning("No business details could be scraped successfully.")
            return

        logging.info(f"Successfully scraped details for {len(final_data)} businesses.")

        # 4. Save results to Excel
        try:
            create_path(OUTPUT_DIR) # Ensure output directory exists using integrated function
            output_file = OUTPUT_DIR / f"{sanitized_category}.xlsx"
            df = pd.DataFrame(final_data)
            # Ensure all columns expected by Excel exist, fill missing with empty string or NaN
            # This prevents potential errors if some scrapes failed partially
            # Example: df = df.reindex(columns=["Business", "Contact", "Email", ...], fill_value="")
            df.to_excel(output_file, index=False, engine='openpyxl') # Specify engine
            logging.info(f"Scraping complete. Data saved to: {output_file}")
        except ImportError:
             logging.error("`openpyxl` library not found. Please install it: pip install openpyxl")
        except Exception as e:
            logging.error(f"Failed to save data to Excel: {e}")


# --- Entry Point ---

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python your_script_name.py <yellow_pages_start_url>")
        start_url = "https://www.yellowpages.com/search?search_terms=pizza&geo_location_terms=New+York%2C+NY" # Example Default
        print(f"No URL provided, using default: {start_url}")
    else:
        start_url = sys.argv[1]

    try:
        asyncio.run(run_scraper(start_url))
    except KeyboardInterrupt:
        logging.info("Scraping interrupted by user.")
    except Exception as e:
        logging.critical(f"An unhandled error occurred in main execution: {e}", exc_info=True) # Log traceback