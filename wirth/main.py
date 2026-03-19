import sys
import os
import time
import uuid
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
# -----------------------------
# Fix import paths
# -----------------------------
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
# -----------------------------
# Local imports
# -----------------------------
from model import connect_to_mysql, insert_job, get_all_job_links
from utils import call_llm_with_retry, parse_llm_response

# -----------------------------
# Configuration
# -----------------------------
CAREER_URL = "https://www.wirth-ag.ch/de/karriere/wirth-als-arbeitgeber"


def scrape_wirth():

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

        print("Opening Karriere page...")
        page.goto(CAREER_URL, timeout=60000)
        time.sleep(3)

        # -----------------------------
        # Scroll to Lehrstellen
        # -----------------------------
        print("Scrolling to Lehrstellen link...")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

        try:
            lehrstellen_el = page.query_selector(
                'a[href="/de/karriere/berufsbildung"]'
            )

            if lehrstellen_el:
                page.evaluate("(el) => el.click()", lehrstellen_el)
                time.sleep(3)
                print("✅ Lehrstellen page opened")
            else:
                print("❌ Lehrstellen link not found")
                browser.close()
                return

        except Exception as e:
            print("❌ Error clicking Lehrstellen:", e)
            browser.close()
            return

        # -----------------------------
        # Scraping loop
        # -----------------------------
        while True:

            try:
                page.wait_for_selector("div.entry", timeout=5000)
            except:
                break

            jobs = page.query_selector_all("div.entry")
            jobs_found += len(jobs)

            for job in jobs:

                title_el = job.query_selector(".entry-title")
                title = title_el.inner_text().strip() if title_el else "No Title"

                external_el = job.query_selector("a.entry-link-external")
                job_url = external_el.get_attribute("href") if external_el else None

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
                if job_url:
                    job_id = job_url
                else:
                    job_id = str(uuid.uuid4())

                full_text = ""

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

                        # Stop scraper if LLM fails repeatedly
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
                        job_source="wirth-ag.ch",
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
            # Pagination
            # -----------------------------
            next_btn = page.locator("a[rel='next']")

            if next_btn.count() > 0:
                page.evaluate(
                    "(el) => el.scrollIntoView({behavior:'smooth'})",
                    next_btn.first
                )
                time.sleep(1)

                page.evaluate("(el) => el.click()", next_btn.first)
                time.sleep(3)

            else:
                break

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
        print("\n✅ Scraping completed successfully!")
        print(f"📝 Total jobs found: {jobs_found}")
        print(f"💾 Jobs inserted: {jobs_inserted}")
        print(f"⏭ Jobs skipped: {jobs_skipped}")
        print(f"⚠ LLM errors: {llm_errors}")


if __name__ == "__main__":
    scrape_wirth()