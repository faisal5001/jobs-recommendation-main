import os
import sys
import time
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from utils import generate, parse_llm_response
from model import connect_to_mysql, create_tables, insert_job, get_all_job_links


def scrape_job_description(driver, url: str) -> str:
    try:
        driver.get(url)
        time.sleep(2)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(8)
        text_content = driver.find_element("tag name", "body").text
        return text_content
    except Exception as e:
        print(f"✗ Failed to scrape {url}: {e}")
        return ""

def main():
    
    print("=" * 60)
    print("TANNE JOB SCRAPER - Database Version")
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
            'Accept': '*/*',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Connection': 'keep-alive',
            'Origin': 'https://tanne.ch',
            'Referer': 'https://tanne.ch/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }
        
        response = requests.get(
            'https://api.jobportal.abaservices.ch/api/extern/v1/job-portal/bf00e8ff-86d1-4249-a66c-afc48dc55baf/publications',
            headers=headers,
        )
        
        data_response = json.loads(response.text)
        print(f"✓ Found {len(data_response)} jobs from API")
        
        # Process each job
        processed = 0
        skipped = 0
        failed = 0
        
        for idx, job_data in enumerate(data_response, 1):
            job_title = job_data.get('JobTitle')
            job_id = job_data.get('JobId')
            job_url = job_data.get('PublicationUrlAbacusJobPortal')
            if job_url in done_job_links:
                print(f"⊘ [{idx}/{len(data_response)}] Skipping already processed job: {job_title}")
                skipped += 1
                continue
            job_source = "Tanne"
            
            if not job_title or not job_id or not job_url:
                print(f"⊘ [{idx}/{len(data_response)}] Skipping job with missing data")
                skipped += 1
                continue
            
            print(f"\n[{idx}/{len(data_response)}] Processing: {job_title}")
            
            # Scrape job description
            print(f"  → Scraping job page...")
            job_description = scrape_job_description(driver, job_url)
            
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
                job_id=job_id,
                job_title=job_title,
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
