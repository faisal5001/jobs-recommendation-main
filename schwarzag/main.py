import os
import sys
import time
import requests
import scrapy
from io import BytesIO
from pypdf import PdfReader
# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from utils import generate, parse_llm_response
from model import connect_to_mysql, create_tables, insert_job, get_all_job_links


def pdf_url_to_text(pdf_url: str) -> str:
    # Fetch PDF into memory
    response = requests.get(pdf_url)
    time.sleep(2)
    response.raise_for_status()

    # Load PDF from bytes
    pdf_file = BytesIO(response.content)
    reader = PdfReader(pdf_file)

    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"

    return text


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

    done_job_links =  get_all_job_links(connection)
    try:
        # Fetch jobs from API
        print("\n Fetching jobs from API...")
        headers = {
            'Referer': 'https://www.schwarzag.ch/kontakt/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        response = requests.get('https://www.schwarzag.ch/ueber-uns/jobs/', headers=headers)
        data_response = scrapy.Selector(text=response.text)
        jobs_list = []

        for loop in data_response.css("a[href$='.pdf']"):
            jobs_list.append({
                "title": loop.css(" ::text").extract(),
                "url": loop.css("::attr(href)").get()
            })

        # Process each job
        processed = 0
        skipped = 0
        failed = 0

        for idx, job_data in enumerate(jobs_list, 1):
            job_title = " ".join(s.strip() for s in job_data.get('title') if s.strip())
            job_id = job_data.get('id', '')
            job_url = job_data.get("url")
            job_source = "Schwarzag"

            if not job_title or not job_url:
                # print(f"⊘ Skipping job with missing data")
                skipped += 1
                continue

            if job_url in done_job_links:
                print(f"⊘ [{idx}/{len(jobs_list)}] Skipping already processed job: {job_title}")
                skipped += 1
                continue
            # Scrape job description
            print(f"  → Scraping job page...")
            job_description = pdf_url_to_text(job_url)
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
        if connection and connection.is_connected():
            connection.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()
