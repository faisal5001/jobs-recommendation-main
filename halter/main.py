import os
import sys
import time
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from utils import generate, parse_llm_response
from model import connect_to_mysql, create_tables, insert_job, get_all_job_links

from dotenv import load_dotenv

# Ensure .env is loaded before using environment variables
load_dotenv()




def scrape_job_description(driver, url: str) -> str:
    try:
        driver.get(url)
        time.sleep(2)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(8)
        text_content = driver.find_element(By.CSS_SELECTOR, "section#beschreibung").text
        return text_content
    except Exception as e:
        print(f"✗ Failed to scrape {url}: {e}")
        return ""


def main():
    print("=" * 60)
    print("Halter JOB SCRAPER - Database Version")
    print("=" * 60)


    # Connect to database
    connection = connect_to_mysql()
    
    if not connection:
        print("✗ Failed to connect to database. Exiting.")
        return

    # Create table if not exists
    create_tables(connection)

    done_job_links = get_all_job_links(connection)
    # Initialize Selenium WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Fetch jobs from API
        print("\n Fetching jobs from API...")
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://jobs.halter.ch',
            'priority': 'u=1, i',
            'referer': 'https://jobs.halter.ch/?lang=en&f=20%3A1432271%2C1432272%2C1432273%2C1432274',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

        response = requests.get(
            'https://ohws.prospective.ch/public/v1/medium/1005677/jobs?lang=en&offset=0&limit=15&f=20:1432271,1432272,1432273,1432274',
            headers=headers)
        json_response = json.loads(response.text)
        total_jobs = json_response.get("total", 0)
        data_response = json_response["jobs"]
        if total_jobs > 15:
            limit = 15
            total_pages = (total_jobs + limit - 1) // limit
            print(f"Total pages: {total_pages}")
            print(f"⚠ API returned total {total_jobs} jobs, but only 15 are fetched due to limit parameter.")
            for page in range(2, total_pages + 1):
                offset = (page - 1) * limit
                response = requests.get(
                f'https://ohws.prospective.ch/public/v1/medium/1005677/jobs?lang=en&offset={offset}&limit=15&f=20:1432271,1432272,1432273,1432274',
                    headers=headers)
                json_response = json.loads(response.text)
                data_response = data_response + json_response["jobs"]

        print(f"✓ Found {len(data_response)} jobs from API")

        # Process each job
        processed = 0
        skipped = 0
        failed = 0

        for idx, job_data in enumerate(data_response, 1):
            job_title = job_data.get('title')
            job_id = job_data.get('id')
            job_url = job_data.get('links')["directlink"]
            job_source = "Halter"

            # skip already done jobs
            if job_url in done_job_links:
                print(f"⊘ [{idx}/{len(data_response)}] Skipping already processed job: {job_title}")
                skipped += 1
                continue
            if not job_title or not job_id or not job_url:
                print(f"⊘ [{idx}/{len(data_response)}] Skipping job with missing data")
                skipped += 1
                continue

            print(f"\n[{idx}/{len(data_response)}] Processing: {job_title}")

            # Scrape job description
            print(f"  → Scraping job page...")
            job_description = scrape_job_description(driver, job_url)
            print("page data",job_url, job_description)

            if not job_description:
                print(f"  ✗ Failed to scrape job description")
                failed += 1
                continue

            # Parse with LLM
            print(f"  → Parsing with LLM...")
            try:
                llm_response = generate(job_description)
                parsed_data = parse_llm_response(llm_response)

                if parsed_data:
                    print(f"  ✓ LLM parsing successful")
                else:
                    print(f"  ⚠ LLM parsing returned empty data")

            except Exception as e:
                print(f"  ✗ LLM parsing failed: {e}")
                parsed_data = {}

            # Insert into database
            print(f"  → Inserting into database...")
            success = insert_job(
                connection=connection,
                job_title=job_title,
                job_id=job_id,
                job_link=job_url,
                job_source=job_source,
                job_description=job_description,
                parsed_data=parsed_data
            )

            if success:
                processed += 1
            else:
                skipped += 1

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"✓ Successfully processed: {processed}")
        print(f"⊘ Skipped (duplicates):   {skipped}")
        print(f"✗ Failed:                 {failed}")
        print("=" * 60)

    finally:
        # Cleanup
        driver.quit()
        if connection and connection.is_connected():
            connection.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()
