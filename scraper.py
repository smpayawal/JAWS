# important imports
import requests
from urllib3.util import retry
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import time, random
import mysql.connector
import logging
import concurrent.futures

# Setup logger
logger = logging.getLogger('scraper')
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('scraper.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

# Retry with backoff on connection errors
retries = retry.Retry(total=2, backoff_factor=0.1)
http = HTTPAdapter(max_retries=retries)
https = HTTPAdapter(max_retries=retries)

session = requests.Session()
session.mount("http://", http)
session.mount("https://", https)

MAX_PAGES = 1000

# Set user agent
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0'}

def fetch_page(url, page_number, max_pages):
    """Fetches the HTML content of a specific job listing page.

    Args:
        url (str): The base URL of the job listings.
        page_number (int): The page number to fetch.
        max_pages (int): The maximum number of pages to scrape.

    Returns:
        bytes: The HTML content of the page, or None if an error occurs.
    """

    if page_number > max_pages:
        return None

    request_url = f"{url}?page={page_number}"

    try:
        # Add random delay
        time.sleep(random.randint(1, 2))

        response = session.get(request_url, headers=headers)

        if response.status_code == 404:
            logger.info(f"Page not found: {request_url}")
            return None

        return response.content

    except ConnectionError:
        logger.error(f"Connection error fetching page: {request_url}")
        # Retry on connection error with backoff
        time.sleep(5)
        return fetch_page(url, page_number, max_pages)


def extract_jobs(html_content):
    """Parses a given HTML page to extract job listings.

    Args:
        html_content (bytes): The HTML content of the page.

    Returns:
        list: A list of extracted job dictionaries, or None if parsing fails.
    """

    soup = BeautifulSoup(html_content, 'html.parser')

    try:
        # Find all job article elements
        job_articles = soup.find_all('article')

        # Extract job details from each article
        extracted_jobs = []
        for article in job_articles:
            job_data = extract_job_details(article)
            if job_data:
                extracted_jobs.append(job_data)

        return extracted_jobs

    except Exception as e:
        logger.error(f"Error parsing page: {e}")
        return None

def extract_job_details(article):
    """Extracts job information from a given article element.

    Args:
        article (bs4.element.Tag): The HTML article element representing a job listing.

    Returns:
        dict: A dictionary containing extracted job details, or None if parsing fails.
    """

    try:
        job_title = article.find('h3').text.strip()
        company = article.find('a', {'data-automation': 'jobCompany'})
        company_name = company.text.strip() if company else "N/A"

        location = article.find('a', {'data-automation': 'jobLocation'}).text.strip()

        salary = article.find('span', {'data-automation': 'jobSalary'})
        salary = salary.text.strip() if salary else "N/A"

        category = article.find('a', {'data-automation': 'jobClassification'})
        category = category.text.strip() if category else "N/A"

        subcategory = article.find('a', {'data-automation': 'jobSubClassification'})
        subcategory = subcategory.text.strip() if subcategory else "N/A"

        # Extract job description (handle potential missing element)
        description_ul = article.find('ul', class_='_1wkzzau0 _1wkzzau3 szurmz0 szurmz4')
        if description_ul:
            description_items = description_ul.find_all('li')
            description = ". ".join([item.find('span').text.strip() for item in description_items])
        else:
            description = "N/A"

        # Extract and format posted date
        posted = article.find('span', {'data-automation': 'jobListingDate'}).text.strip()
        posted_date = relative_date(posted)
        posted_formatted = posted_date.strftime("%m/%d/%Y") if posted_date else None

        # Only return data if a title is found
        if not job_title:
            return None

        return {
            'job_title': job_title,
            'company_name': company_name,
            'location': location,
            'category': category,
            'subcategory': subcategory,
            'salary': salary,
            'description': description,
            'posted': posted,
            'posted_date': posted_formatted
        }

    except Exception as e:
        logger.error(f"Error parsing job article: {e}")
        return None

def relative_date(posted_string):
    """Parses a relative posted date string (e.g., "5 days ago").

    Args:
        posted_string (str): The relative posted date string.

    Returns:
        datetime: The parsed date, or None if the string is invalid.
    """

    # Regular expression pattern to match relative date format
    pattern = r"(\d+)\s?(d?|h?|m?) ago"

    # Match the pattern and extract components
    match = re.search(pattern, posted_string)
    if not match:
        return None

    value, unit = match.groups()
    value = int(value)

    # Convert unit to timedelta and subtract from current date
    delta = {
        "d": timedelta(days=value),
        "h": timedelta(hours=value),
        "m": timedelta(minutes=value),
    }.get(unit.lower(), None)

    if not delta:
        logger.warning(f"Invalid time unit: {unit}")
        return None

    return datetime.now() - delta

def store_jobs(jobs):
    """Saves extracted jobs to the specified database table.

    Args:
        jobs (list): A list of job dictionaries containing extracted data.
    """

    # Connect to the database
    try:
        db = mysql.connector.connect(
            user='root',
            password='N4p4k4p0g1k0',  # Mask password for security
            port=3306,
            host='localhost',
            database='data_warehouse2024'
        )
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return

    with db:
        cursor = db.cursor()

        # Prepare batch insert query for efficiency
        insert_query = """
            INSERT INTO jaws
            (ExtractionDate, JobTitle, CompanyName, Location, Category, SubCategory, Salary, JobDescription, Posted, DatePosted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Convert job data to tuples for batch insertion
        job_data = []
        for job in jobs:
            job_data.append((
                datetime.now().strftime("%m/%d/%Y"),
                job['job_title'],
                job['company_name'],
                job['location'],
                job['category'],
                job['subcategory'],
                job['salary'],
                job['description'],
                job['posted'],
                job['posted_date']
            ))

        # Execute the batch insert query
        try:
            cursor.executemany(insert_query, job_data)
            db.commit()
            logger.info(f"Successfully saved {len(job_data)} jobs to database.")
        except Exception as e:
            logger.error(f"Error saving jobs to database: {e}")

    # Close the database connection
    db.close()

def parallelism(url, page_number):
    """Scrapes job listings from a specific page URL and saves them asynchronously.

    Args:
        url (str): The base URL of the job listings.
        page_number (int): The page number to scrape.
    """

    try:
        # Fetch page content and handle potential errors
        page_content = fetch_page(url, page_number, MAX_PAGES)
        if not page_content:
            logger.info(f"Page {page_number} not found.")
            return

        # Extract job details from the page
        jobs = extract_jobs(page_content)

        # Offload job saving to a separate thread for parallelism
        with concurrent.futures.ThreadPoolExecutor() as executor:
            logger.info("Scraping page %s", page_number)
            future = executor.submit(store_jobs, jobs)
            future.result()  # Wait for saving to finish

    except Exception as e:
        logger.error(f"Error scraping page {page_number}: {e}")

def main():

    logger.info('Starting scrape...')

    url = "https://www.jobstreet.com.ph/jobs"

    # Use ThreadPoolExecutor for parallelization
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(parallelism, url, page) for page in range(1, MAX_PAGES + 1)]

        # Wait for all pages to be scraped
        for future in futures:
            future.result()

    logger.info("Completed scrape!")

if __name__ == '__main__':
    main()
