import sys
import os

# -----------------------------
# Fix import path to use local model.py
# -----------------------------
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# -----------------------------
# Now imports will work
# -----------------------------
import logging
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from dotenv import load_dotenv
import requests
import pdfplumber
import os

# Load environment variables
load_dotenv()

from model import connect_to_mysql, insert_job
from utils import generate, parse_llm_response

BASE_URL = "https://haeusermann.ch"
AKTUELL_URL = "https://haeusermann.ch/de/aktuell/"
KARRIERE_URL = "https://haeusermann.ch/de/firma/karriere/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# -----------------------------
# Helper functions (same as before)
# -----------------------------
def auto_scroll(page):
    logging.info("Scrolling page to load dynamic content...")
    previous_height = 0
    while True:
        current_height = page.evaluate("document.body.scrollHeight")
        if current_height == previous_height:
            break
        previous_height = current_height
        page.mouse.wheel(0, 2000)
        page.wait_for_timeout(1500)


def click_learn_more(page):
    try:
        button = page.locator("text=Mehr laden, text=Learn more").first
        if button.is_visible():
            logging.info("Clicking Learn More button...")
            button.click()
            page.wait_for_timeout(3000)
    except Exception:
        logging.info("No Learn More button found.")


def extract_job_links(page, selector):
    jobs = []
    elements = page.locator(selector)
    count = elements.count()
    logging.info(f"Found {count} potential jobs")
    for i in range(count):
        try:
            el = elements.nth(i)
            title = el.inner_text().strip()
            link = el.get_attribute("href")
            if not link:
                continue
            full_url = link if link.startswith("http") else urljoin(BASE_URL, link)
            jobs.append({"title": title, "url": full_url})
        except Exception as e:
            logging.warning(f"Error extracting job {i}: {e}")
    return jobs


def safe_goto(page, url):
    try:
        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)
        logging.info(f"Opened page: {url}")
        return True
    except PlaywrightTimeoutError:
        logging.error(f"Timeout loading {url}")
        return False
    except Exception as e:
        logging.error(f"Error loading {url}: {e}")
        return False


def extract_pdf_text(pdf_url):
    logging.info(f"Downloading PDF: {pdf_url}")
    try:
        resp = requests.get(pdf_url, timeout=30)
        resp.raise_for_status()
        with open("temp.pdf", "wb") as f:
            f.write(resp.content)
        text = ""
        with pdfplumber.open("temp.pdf") as pdf:
            for page in pdf.pages:
                text += page.extract_text() + "\n"
        os.remove("temp.pdf")
        return text.strip()
    except Exception as e:
        logging.error(f"Error extracting PDF text: {e}")
        return ""


def get_job_description(page, job_url):
    if job_url.lower().endswith(".pdf"):
        return extract_pdf_text(job_url)
    if not safe_goto(page, job_url):
        return ""
    page.wait_for_timeout(2000)
    try:
        desc_el = page.locator("article, .job-description, .content").first
        if desc_el.is_visible():
            return desc_el.inner_text().strip()
    except Exception:
        pass
    return page.content()


# -----------------------------
# Main
# -----------------------------
def main():
    conn = connect_to_mysql()
    if not conn:
        logging.error("Cannot connect to MySQL. Exiting.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )

        all_job_links = []

        for url, selector, source in [
            (AKTUELL_URL, "h5 a", "aktuell"),
            (KARRIERE_URL, "a[href*='karriere'], h6 a, h5 a", "karriere")
        ]:
            if safe_goto(page, url):
                auto_scroll(page)
                click_learn_more(page)
                page.wait_for_timeout(2000)
                jobs = extract_job_links(page, selector)
                for job in jobs:
                    job["source"] = source
                all_job_links.extend(jobs)

        logging.info(f"Total jobs found: {len(all_job_links)}")

        for idx, job in enumerate(all_job_links, 1):
            logging.info(f"[{idx}/{len(all_job_links)}] Processing job: {job['title']}")
            job_description = get_job_description(page, job['url'])

            if not job_description.strip():
                logging.warning("Job description empty, skipping job.")
                continue

            parsed_data = {}
            try:
                llm_resp = generate(job_description)
                parsed_data = parse_llm_response(llm_resp)
            except Exception as e:
                logging.error(f"LLM parse failed: {e}")
                parsed_data = {}

            # ---- FIXED: truncate to prevent MySQL 1406 error ----
            job_external_id = job['url'][:1000]  # ensure URL fits
            description = job_description[:4294967295]  # LONGTEXT max safe limit

            try:
                insert_job(
                    connection=conn,
                    job_id=job_external_id,
                    job_title=job['title'],
                    job_link=job['url'],
                    job_source=job['source'],
                    job_description=description,
                    parsed_data=parsed_data
                )
                logging.info(f"✓ Job inserted successfully: {job['title']}")
            except Exception as e:
                logging.warning(f"⊘ Job skipped or duplicate: {job['title']}")
                logging.error(f"DB insert error: {e}")

        browser.close()
    conn.close()
    logging.info("Done! All jobs processed and saved.")


if __name__ == "__main__":
    main()