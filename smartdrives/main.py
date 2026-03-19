import sys
import os
import time
import uuid
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

load_dotenv()

from model import connect_to_mysql, insert_job, get_all_job_links
from utils import call_llm_with_retry, parse_llm_response

# -----------------------------
# Configuration
# -----------------------------
CAREER_URL = "https://www.smartdrives.ch/ueber-uns/karriere"
BASE_URL = "https://www.smartdrives.ch"
JOB_LINKS_XPATH = '//*[@id="mainContent"]/div/div/div[2]/div/div/div/div[1]/h3/a'
PDF_XPATH = '/html/body/main/div/section/div/div[1]/div/ul/li[7]/p[2]/a'

def scrape_smartdrives():

    # -----------------------------
    # Database connection
    # -----------------------------
    db_connection = connect_to_mysql()
    if not db_connection:
        print("✗ Cannot connect to DB, exiting")
        return

    # -----------------------------
    # Get already scraped jobs
    # -----------------------------
    done_job_links = get_all_job_links(db_connection)

    jobs_found = 0
    jobs_inserted = 0
    jobs_skipped = 0
    llm_errors = 0

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        print("Opening SmartDrives career page...")
        page.goto(CAREER_URL, timeout=60000)
        time.sleep(3)

        jobs = page.locator(f"xpath={JOB_LINKS_XPATH}")
        total_jobs = jobs.count()
        print(f"📝 Total SmartDrives jobs found: {total_jobs}")

        for j in range(total_jobs):
            job_elem = jobs.nth(j)
            title = job_elem.inner_text().strip() if job_elem.count() else "No Title"
            job_url = job_elem.get_attribute("href") if job_elem.count() else None
            if job_url and not job_url.startswith("http"):
                job_url = BASE_URL + job_url

            jobs_found += 1

            # -----------------------------
            # Skip already scraped jobs
            # -----------------------------
            if job_url and job_url in done_job_links:
                print(f"⏭ Skipping already scraped job: {title}")
                jobs_skipped += 1
                continue

            # -----------------------------
            # Generate unique job ID
            # -----------------------------
            job_id = job_url if job_url else str(uuid.uuid4())

            full_text = ""

            # -----------------------------
            # Scrape job description page
            # -----------------------------
            if job_url:
                try:
                    new_page = context.new_page()
                    new_page.goto(job_url, timeout=60000)
                    time.sleep(2)
                    full_text = new_page.inner_text("body")
                    new_page.close()
                except Exception as e:
                    print("⚠ External page error:", e)

            # -----------------------------
            # LLM Parsing with retry logic
            # -----------------------------
            parsed_data = {}

            if full_text:
                try:
                    llm_response = call_llm_with_retry(full_text)
                    if not llm_response:
                        print("❌ LLM failed after retries. Stopping scraper.")
                        browser.close()
                        return
                    parsed_data = parse_llm_response(llm_response)
                except Exception as e:
                    print("❌ LLM error:", e)
                    llm_errors += 1
                    if llm_errors >= 3:
                        print("❌ Too many LLM failures. Stopping scraper.")
                        browser.close()
                        return

            # -----------------------------
            # Insert job into database
            # -----------------------------
            try:
                insert_job(
                    db_connection,
                    job_id=job_id,
                    job_title=title,
                    job_link=job_url,
                    job_source="smartdrives.ch",
                    job_description=full_text,
                    parsed_data=parsed_data
                )
                print(f"✓ Job inserted: {title}")
                jobs_inserted += 1
            except Exception as e:
                print(f"⚠ Job skipped: {title}")
                print("DB insert error:", e)
                jobs_skipped += 1

        # -----------------------------
        # Cleanup
        # -----------------------------
        browser.close()
        if db_connection and db_connection.is_connected():
            db_connection.close()
            print("✓ Database connection closed")

        # -----------------------------
        # Summary
        # -----------------------------
        print("\n✅ SmartDrives scraping completed successfully!")
        print(f"📝 Total jobs found: {jobs_found}")
        print(f"💾 Jobs inserted: {jobs_inserted}")
        print(f"⏭ Jobs skipped: {jobs_skipped}")
        print(f"⚠ LLM errors: {llm_errors}")


if __name__ == "__main__":
    scrape_smartdrives()