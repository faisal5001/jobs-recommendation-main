import os
import sys
import time
import requests
import scrapy
from model import connect_to_mysql, create_tables, get_all_job_links, insert_job

# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

# from utils import generate, parse_llm_response
# from model import connect_to_mysql, create_tables, insert_job, get_all_job_links
from utils import generate, parse_llm_response


def main():
    print("=" * 60)
    print("JOB UP SCRAPER - Database Version")
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
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
            # 'cookie': 'ab_experiments=%5B%7B%22key%22%3A%22CHI-2258-survey%22%2C%22variant%22%3A1%7D%2C%7B%22key%22%3A%22CHI-2258-survey-srp%22%2C%22variant%22%3A1%7D%2C%7B%22key%22%3A%22CNT-1215-search-salary-lock%22%2C%22variant%22%3A0%7D%5D; session_id=40221c7c-58c3-4d86-9447-61d7cdf666fa; CONSENTMGR=c1:1%7Cc4:1%7Cc2:0%7Cc3:0%7Cc5:0%7Cc6:0%7Cc7:0%7Cc8:0%7Cc9:0%7Cc10:0%7Cc11:0%7Cc12:0%7Cc13:0%7Cc14:0%7Cc15:0%7Cc16:0%7Cts:1771876592791%7Cconsent:true%7Cid:019c8c131913000ad007549cf5da0506f001806700876; AMCVS_EB4C1D9B672BAE480A495FB6%40AdobeOrg=1; _clck=m7rq16%5E2%5Eg3t%5E1%5E2245; _gcl_au=1.1.1267453894.1771876594; tealium_ga=GA1.1.019c8c131913000ad007549cf5da0506f001806700876; _pin_unauth=dWlkPU0yTmxZelZoWkdRdE1XUmhNaTAwTnpsbExUazBNakF0WTJNeU1UUXdOak13TkRSbA; _tt_enable_cookie=1; _ttp=01KJ616DK8KZFC51HE7EERKHTD_.tt.1; s_ecid=MCMID%7C46938505740337722794125820812814674047; AMCV_EB4C1D9B672BAE480A495FB6%40AdobeOrg=179643557%7CMCIDTS%7C20508%7CMCMID%7C46938505740337722794125820812814674047%7CMCAAMLH-1772481393%7C3%7CMCAAMB-1772481393%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1771883795s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.5.0; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22hrHwPNiqHitaX3hsUeyY%22%2C%22expiryDate%22%3A%222027-02-23T19%3A58%3A28.121Z%22%7D; cto_bundle=yqPOm19xYjVFNGRqJTJCOVhnRHNKSDlZZFdYVUJOa2hkT2NmUSUyQngwaGxmbnBoQ0daNGNLaGxDbTUybGVHcXRFZm9iTTdFMCUyQldDMFpSRG00UkRkV1pBZ09QV3JWY3hCTU0lMkJscEtTR1VuYkt1NlEyU3laUmRoaUJxV29KNFhQcXRjRzglMkJSYnh2UXpKQWpLWGNpSWJwRGdQRzFZQVFBJTNEJTNE; AWSALB=4GRpGjtWOvaCDGP8ruZikstbh49AlWoY6j64Fov2A6+1MVzziF6whwKBQ8JiMjFOyFGds91f4Vuq2BZ/3w2C+Wc1Gal3d46fzJT3neiJozPIeswV/1jpvubj7jVL; AWSALBCORS=4GRpGjtWOvaCDGP8ruZikstbh49AlWoY6j64Fov2A6+1MVzziF6whwKBQ8JiMjFOyFGds91f4Vuq2BZ/3w2C+Wc1Gal3d46fzJT3neiJozPIeswV/1jpvubj7jVL; _clsk=14jkh6u%5E1771877230154%5E6%5E1%5Ez.clarity.ms%2Fcollect; _teal_prevPage=JTdCJTIydXJsJTIyJTNBJTIyaHR0cHMlM0ElMkYlMkZ3d3cuam9idXAuY2glMkZlbiUyRiUyMiUyQyUyMnBhZ2VfY2F0ZWdvcnklMjIlM0ElMjJob21lJTIyJTdE; _uetsid=c152d1e010f111f1b838ffbac89ce9d5; _uetvid=c152f09010f111f1956b55bffde513af; tealium_ga_GCBHJJGL65=GS2.1.s1771876587807$o1$g1$t1771877230$j60$l0$h0; utag_main=v_id:019c8c131913000ad007549cf5da0506f001806700876$_sn:1$_se:24%3Bexp-session$_ss:0%3Bexp-session$_st:1771879030222%3Bexp-session$ses_id:1771876587807%3Bexp-session$_pn:2%3Bexp-session$vapi_domain:jobup.ch$adobe_ecid:46938505740337722794125820812814674047%3Bexp-1834949230000$dc_visit:1$dc_event:10%3Bexp-session$dc_region:eu-central-1%3Bexp-session; ttcsid_CA3PJNJC77UDR638BOE0=1771876595314::wQizA5iq-YQCxJIxA96h.1.1771877230628.1; ttcsid=1771876595317::Io8Hsbu2UGTXIQvF-9wj.1.1771877230628.0::1.634931.112958::484926.6.466.4184::633642.137.3885',
        }
        response = requests.get('https://www.jobup.ch/en/', headers=headers)
        time.sleep(2)
        response_data = scrapy.Selector(text=response.text)
        categories_list = []

        for v in response_data.css("section#tab-category ul li"):
            job = {
                "href": "https://www.jobup.ch" + v.css("a ::attr(href)").get(),
                "title": v.css("a ::attr(title)").get()
            }
            categories_list.append(job)
        for loop in categories_list:
            job_category_response = requests.get(loop["href"], headers=headers)
            time.sleep(2)
            job_response_data = scrapy.Selector(text=job_category_response.text)
            jobs_pages = max(
                int(v.css("::text").get())
                for v in job_response_data.css('[data-cy="paginator"] span[title]')
                if v.css("::text").get()
            )
            for page in range(1, jobs_pages + 1):
                page_url = loop["href"] + "?page=" + str(page)
                page_response = requests.get(page_url, headers=headers)
                time.sleep(2)
                page_response_data = scrapy.Selector(text=page_response.text)
                jobs = []

                for v in page_response_data.css('div[aria-label="Job list"] div[data-cy="serp-item"]'):
                    job = {
                        "href": "https://www.jobup.ch" + v.css("a ::attr(href)").get(),
                        "title": v.css("a ::attr(title)").get()
                    }
                    jobs.append(job)
                for inner_loop in jobs:
                    if inner_loop["href"] in done_job_links:
                        continue
                    each_job_response = requests.get(inner_loop["href"], headers=headers)
                    time.sleep(2)
                    each_job_response_data = scrapy.Selector(text=each_job_response.text)
                    job_description = " ".join([v.strip() for v in each_job_response_data.css("div[data-cy='vacancy-description'] ::text").extract()])
                    job_id = inner_loop["href"].split("detail/")[-1].replace("/","")
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
                    job_source = "jobup"
                    print(f"  → Inserting into database...")
                    success = insert_job(
                        connection=connection,
                        job_id=job_id,
                        job_title=inner_loop["title"],
                        job_link=inner_loop["href"],
                        job_source=job_source,
                        job_description=job_description,
                        parsed_data=parsed_data
                    )
                    done_job_links.append(inner_loop["href"])
        
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\n✓ Database connection closed")

if __name__ == "__main__":
    main()