import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import mysql.connector
import time, random
from urllib3.util import retry
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
import logging

# Setup logger
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('logger.log')
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




def get_page(url, page, max_pages):


  if page > max_pages:
    return None

  request_url = f"{url}?page={page}"

  try:
    # Add random delay
    time.sleep(random.randint(1,2))

    response = session.get(request_url, headers=headers)

    if response.status_code == 404:
      return None

    return response.content

  except ConnectionError:
    # Retry on connection error
    print("Connection error, retrying...")
    time.sleep(5)
    return get_page(url, page)

def parse_page(page):
    soup = BeautifulSoup(page, 'html.parser')
    articles = soup.find_all('article')

    jobs = []
    for article in articles:
        job = parse_article(article)
        if job:
            jobs.append(job)

    return jobs

def parse_article(article):
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

    description_ul = article.find('ul', class_='_1wkzzau0 _1wkzzau3 szurmz0 szurmz4')
    if description_ul:
        description_items = description_ul.find_all('li')
        description = ". ".join([item.find('span').text.strip() for item in description_items])
    else:
        description = "N/A"

    posted = article.find('span', {'data-automation': 'jobListingDate'}).text.strip()
    posted_date = parse_posted_date(posted)
    posted_formatted = posted_date.strftime("%m/%d/%Y") if posted_date else None

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

def parse_posted_date(posted):
    pattern = r"(\d+)\s?(d|h|m) ago"
    parts = re.search(pattern, posted)
    if not parts:
        return None

    num, unit = parts.groups()
    num = int(num)

    if unit == "d":
        delta = timedelta(days=num)
    elif unit == "h":
        delta = timedelta(hours=num)
    else:
        delta = timedelta(minutes=num)

    return datetime.now() - delta

def save_jobs(jobs):
    db = mysql.connector.connect(
        user='root',
        password='*******',
        port=3306,
        host='localhost',
        database='data_warehouse2024'
    )

    cursor = db.cursor()

    query = """
        INSERT INTO jaws
        (ExtractionDate, JobTitle, CompanyName, Location, Category, SubCategory, Salary, JobDescription, Posted, DatePosted)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for job in jobs:
        values = (
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
        )

        cursor.execute(query, values)

    db.commit()
    db.close()

def main():
    logger.info('Starting scrape...')

    url = "https://www.jobstreet.com.ph/jobs"
    page = 1

    while True:
        logger.info("Scraping page %s", page)

        page_content = get_page(url, page, MAX_PAGES)
        if not page_content:
            break

        jobs = parse_page(page_content)

        logger.info("Extracted %s jobs", len(jobs))

        save_jobs(jobs)

        logger.info("Inserted %s jobs into database", len(jobs))

        page += 1

    logger.info("Completed scrape!")

if __name__ == '__main__':
    main()
