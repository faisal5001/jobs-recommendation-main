import os
import sys
import time
import requests
import math
import scrapy
# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from utils import generate, parse_llm_response
from model import connect_to_mysql, create_tables, insert_job, get_all_job_links


def main():
    print("=" * 60)
    print("JOB AGENT SCRAPER - Database Version")
    print("=" * 60)
    
    # Connect to database
    connection = connect_to_mysql()
    if not connection:
        print("✗ Failed to connect to database. Exiting.")
        return
    
    # Create table if not exists
    create_tables(connection)
    done_job_links = get_all_job_links(connection)
    
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=0, i',
            'referer': 'https://www.jobagent.ch/kaufm%C3%A4nnische-jobs',
            'sec-ch-device-memory': '8',
            'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-full-version-list': '"Not:A-Brand";v="99.0.0.0", "Google Chrome";v="145.0.7632.116", "Chromium";v="145.0.7632.116"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
            # 'cookie': '_uc=ad_storage=granted:analytics_storage=granted; _ga=GA1.1.1926029684.1771957016; _clck=1up36li%5E2%5Eg3u%5E0%5E2246; _gcl_au=1.1.150182279.1771957045; lang=de-CH; _uetsid=0df3deb011ad11f1b3a33325b65283ef; _uetvid=0df4699011ad11f1b87c91493185928a; _clsk=1nl88w8%5E1771959547752%5E9%5E1%5Ez.clarity.ms%2Fcollect; datadome=gRDrJm~AIbf2M7~HQcBaNZ5OXrvo_oZ08Llv4NwkXEmCURnp0HXyP_HOS6CiDctZ57X86xNY2eNHF4rznCFUhTsRckTtCjTPgqbKEKBKzrxL9vukGQ3eaqbyMtYZEqmQ; _ga_T0E2JNNRW2=GS2.1.s1771959217$o2$g1$t1771959573$j50$l0$h1679622999',
        }

        response = requests.get('https://www.jobagent.ch/', headers=headers)
        time.sleep(2)
        response_data = scrapy.Selector(text=response.text)
        for loop in response_data.css("div.categories ul.dropdown-menu"):
            category_link = loop.css("li a ::attr(href)").extract()[-1]
            category_response = requests.get(category_link, headers=headers)
            time.sleep(2)
            category_response_data = scrapy.Selector(text=category_response.text)
            jobs_count = category_response_data.css("div.spacer-bottom15 >h2 ::text").get().split(" ")[0].strip()
            page_count = math.ceil(int(jobs_count.replace('’', "")) / 117)
            for page in range(1, page_count + 1):
                page_url = category_link + "?page=" + str(page)
                page_response = requests.get(page_url, headers=headers)
                page_response_data = scrapy.Selector(text=page_response.text)
                each_page_jobs = page_response_data.css("div.results ul.resultlist li.item a.title")
                for inner_loop in each_page_jobs:
                    if inner_loop in done_job_links:
                        continue
                    job_title = "".join(inner_loop.css(" ::text").extract()).strip()
                    job_link = inner_loop.css(" ::attr(href)").get()
                    job_response = requests.get(job_link, headers=headers)
                    time.sleep(1)
                    job_response_data = scrapy.Selector(text=job_response.text)
                    job_description = " ".join(job_response_data.css("div.plain-content ::text").extract())
                    job_id = job_link.split("/")[-1]
                    if not job_description:
                        print(f"  ✗ Failed to scrape job description")
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
                    job_source = "jobAgent"
                    print(f"  → Inserting into database...")
                    success = insert_job(
                        connection=connection,
                        job_id=job_id,
                        job_title=job_title,
                        job_link=job_link,
                        job_source=job_source,
                        job_description=job_description,
                        parsed_data=parsed_data
                    )
                    done_job_links.append(job_link)
        
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\n✓ Database connection closed")

if __name__ == "__main__":
    main()
