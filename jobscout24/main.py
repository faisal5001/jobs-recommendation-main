import os
import sys
import time
import requests
import scrapy
import math
# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from utils import generate, parse_llm_response
from model import connect_to_mysql, create_tables, insert_job, get_all_job_links


def main():
    print("=" * 60)
    print("JOB SCOUT 24 SCRAPER - Database Version")
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
        cookies = {
            '_JS24A': '0',
            'ARRAffinity': '0ee95c059457b7dd0d50c934378435e1385d119a3d2ba3ce1fc95f504d3abf0a',
            'ARRAffinitySameSite': '0ee95c059457b7dd0d50c934378435e1385d119a3d2ba3ce1fc95f504d3abf0a',
            'ASP.NET_SessionId': '13veicativ5ofn3tbtvu5lbc',
            'ASID': '5363ccce-a338-487b-bcab-723a45ca465b|20260223|8',
            '__RequestVerificationToken': 'm5CBkMB47tiOsaGp4PSVoXoRQlCYzfZyqZCx7xqMKhbORnNuCNKzxidgZNE3bg4SPU4GibEnZCcTbjo_bzUuJAFC31Q-CotWnUpesy1afUI1',
            'JS24_CONSENT': 'sn|ff|mr|1',
            'CONSENTMGR': 'c1:1%7Cc4:1%7Cc2:0%7Cc3:0%7Cc5:0%7Cc6:0%7Cc7:0%7Cc8:0%7Cc9:0%7Cc10:0%7Cc11:0%7Cc12:0%7Cc13:0%7Cc14:0%7Cc15:0%7Cc16:0%7Cts:1771868066466%7Cconsent:true%7Cid:019c8b91128000081df998ea2b700506f001d06700876',
            'AMCVS_EB4C1D9B672BAE480A495FB6%40AdobeOrg': '1',
            's_ecid': 'MCMID%7C46938505740337722794125820812814674047',
            'tealium_ga': 'GA1.1.019c8b91128000081df998ea2b700506f001d06700876',
            '_tt_enable_cookie': '1',
            '_ttp': '01KJ5S29GKPBM8GBDJVNG21GN4_.tt.1',
            'AMCV_EB4C1D9B672BAE480A495FB6%40AdobeOrg': '179643557%7CMCIDTS%7C20508%7CMCMID%7C46938505740337722794125820812814674047%7CMCAAMLH-1772472869%7C3%7CMCAAMB-1772472869%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1771875271s%7CNONE%7CMCAID%7CNONE%7CMCSYNCSOP%7C411-20515%7CvVersion%7C5.5.0',
            '_gcl_au': '1.1.59367943.1771868071.493639327.1771868159.1771868159',
            '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22jcRZWJReo9PWSmBu3uZL%22%2C%22expiryDate%22%3A%222027-02-23T17%3A37%3A47.296Z%22%7D',
            'cto_bundle': '7QXiHl90aEglMkIyZURGWkNQYkhMUFRBQ1YwYWQwNkZseTElMkJtaTNUZzVWc2tQckI4eW5IQlA0NXlWUlU5ZzZpVml5dTdBTk80cm9KVXpuUFVCVWtEUjN5REV4WEpBZE9hcFolMkZKb1hTUUNMVzEwNVhhM2J6eFJ3VzRqRUYlMkZaYTJpNSUyQkZJQzlaU3RxdVplUjVCUUMyREFQNjdXNzR3JTNEJTNE',
            '_teal_prevPage': 'JTdCJTIydXJsJTIyJTNBJTIyaHR0cHMlM0ElMkYlMkZ3d3cuam9ic2NvdXQyNC5jaCUyRmVuJTJGam9icyUyRmFjY291bnRpbmclMkYlMjIlMkMlMjJwYWdlX2NhdGVnb3J5JTIyJTNBJTIyam9iJTIyJTdE',
            '_uetsid': 'e8622eb010dd11f1806e9f86a8682fe9',
            '_uetvid': 'e862aac010dd11f195ec7771a20919eb',
            'utag_main': 'v_id:019c8b91128000081df998ea2b700506f001d06700876$_sn:1$_se:54%3Bexp-session$_ss:0%3Bexp-session$_st:1771870272756%3Bexp-session$ses_id:1771868066435%3Bexp-session$_pn:5%3Bexp-session$vapi_domain:jobscout24.ch$adobe_ecid:46938505740337722794125820812814674047%3Bexp-1834940473000$dc_visit:1$dc_event:36%3Bexp-session$dc_region:eu-central-1%3Bexp-session',
            'ttcsid_CVE267BC77UANTV9LTFG': '1771868071448::NwAlNFC3IJcjj0kxFq6x.1.1771868473176.1',
            'ttcsid': '1771868071449::22MeIFDyMZ-KdJfJI214.1.1771868265261.0::1.402659.45072::193789.7.260.422::188784.17.88',
            'tealium_ga_3EKC6G4KSB': 'GS2.1.s1771868066435$o1$g1$t1771868474$j52$l0$h0',
        }

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
             'sec-fetch-site': 'cross-site',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
            # 'cookie': '_JS24A=0; ARRAffinity=0ee95c059457b7dd0d50c934378435e1385d119a3d2ba3ce1fc95f504d3abf0a; ARRAffinitySameSite=0ee95c059457b7dd0d50c934378435e1385d119a3d2ba3ce1fc95f504d3abf0a; ASP.NET_SessionId=13veicativ5ofn3tbtvu5lbc; ASID=5363ccce-a338-487b-bcab-723a45ca465b|20260223|8; __RequestVerificationToken=m5CBkMB47tiOsaGp4PSVoXoRQlCYzfZyqZCx7xqMKhbORnNuCNKzxidgZNE3bg4SPU4GibEnZCcTbjo_bzUuJAFC31Q-CotWnUpesy1afUI1; JS24_CONSENT=sn|ff|mr|1; CONSENTMGR=c1:1%7Cc4:1%7Cc2:0%7Cc3:0%7Cc5:0%7Cc6:0%7Cc7:0%7Cc8:0%7Cc9:0%7Cc10:0%7Cc11:0%7Cc12:0%7Cc13:0%7Cc14:0%7Cc15:0%7Cc16:0%7Cts:1771868066466%7Cconsent:true%7Cid:019c8b91128000081df998ea2b700506f001d06700876; AMCVS_EB4C1D9B672BAE480A495FB6%40AdobeOrg=1; s_ecid=MCMID%7C46938505740337722794125820812814674047; tealium_ga=GA1.1.019c8b91128000081df998ea2b700506f001d06700876; _tt_enable_cookie=1; _ttp=01KJ5S29GKPBM8GBDJVNG21GN4_.tt.1; AMCV_EB4C1D9B672BAE480A495FB6%40AdobeOrg=179643557%7CMCIDTS%7C20508%7CMCMID%7C46938505740337722794125820812814674047%7CMCAAMLH-1772472869%7C3%7CMCAAMB-1772472869%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1771875271s%7CNONE%7CMCAID%7CNONE%7CMCSYNCSOP%7C411-20515%7CvVersion%7C5.5.0; _gcl_au=1.1.59367943.1771868071.493639327.1771868159.1771868159; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22jcRZWJReo9PWSmBu3uZL%22%2C%22expiryDate%22%3A%222027-02-23T17%3A37%3A47.296Z%22%7D; cto_bundle=7QXiHl90aEglMkIyZURGWkNQYkhMUFRBQ1YwYWQwNkZseTElMkJtaTNUZzVWc2tQckI4eW5IQlA0NXlWUlU5ZzZpVml5dTdBTk80cm9KVXpuUFVCVWtEUjN5REV4WEpBZE9hcFolMkZKb1hTUUNMVzEwNVhhM2J6eFJ3VzRqRUYlMkZaYTJpNSUyQkZJQzlaU3RxdVplUjVCUUMyREFQNjdXNzR3JTNEJTNE; _teal_prevPage=JTdCJTIydXJsJTIyJTNBJTIyaHR0cHMlM0ElMkYlMkZ3d3cuam9ic2NvdXQyNC5jaCUyRmVuJTJGam9icyUyRmFjY291bnRpbmclMkYlMjIlMkMlMjJwYWdlX2NhdGVnb3J5JTIyJTNBJTIyam9iJTIyJTdE; _uetsid=e8622eb010dd11f1806e9f86a8682fe9; _uetvid=e862aac010dd11f195ec7771a20919eb; utag_main=v_id:019c8b91128000081df998ea2b700506f001d06700876$_sn:1$_se:54%3Bexp-session$_ss:0%3Bexp-session$_st:1771870272756%3Bexp-session$ses_id:1771868066435%3Bexp-session$_pn:5%3Bexp-session$vapi_domain:jobscout24.ch$adobe_ecid:46938505740337722794125820812814674047%3Bexp-1834940473000$dc_visit:1$dc_event:36%3Bexp-session$dc_region:eu-central-1%3Bexp-session; ttcsid_CVE267BC77UANTV9LTFG=1771868071448::NwAlNFC3IJcjj0kxFq6x.1.1771868473176.1; ttcsid=1771868071449::22MeIFDyMZ-KdJfJI214.1.1771868265261.0::1.402659.45072::193789.7.260.422::188784.17.88; tealium_ga_3EKC6G4KSB=GS2.1.s1771868066435$o1$g1$t1771868474$j52$l0$h0',
        }

        response = requests.get('https://www.jobscout24.ch/en/', headers=headers)
        time.sleep(2)
        data_resp = scrapy.Selector(text=response.text)
        jobs_links_list = ["https://www.jobscout24.ch"+v for v in data_resp.css("section.home-middle ul li a::attr(href)").getall()]
        for loop in jobs_links_list:
            job_category_response = requests.get(loop, headers=headers)
            time.sleep(2)
            job_response_data = scrapy.Selector(text=job_category_response.text)
            jobs_in_page = math.ceil(int(job_response_data.css(" h1 span.number ::text").get()) / 25)
            for page in range(1, jobs_in_page + 1):
                page_url = loop + "&p=" + str(page)
                page_response = requests.get(page_url, headers=headers)
                time.sleep(2)
                page_response_data = scrapy.Selector(text=page_response.text)
                jobs = []

                for v in page_response_data.css("div.jobs-list ul li.job-list-item"):
                    job = {
                        "job_id": v.attrib.get("data-job-id"),
                        "href": "https://www.jobscout24.ch" + v.css("a.job-link-detail::attr(href)").get(),
                        "title": v.css("a.job-link-detail::attr(title)").get()
                    }
                    jobs.append(job)
                for inner_loop in jobs:
                    if inner_loop['href'] in done_job_links:
                        print("This job is already scraped")
                        continue
                    each_job_response = requests.get(inner_loop["href"], headers=headers)
                    time.sleep(2)
                    each_job_response_data = scrapy.Selector(text=each_job_response.text)
                    job_description = " ".join([v.strip() for v in each_job_response_data.css("article.job-details ::text").extract()])
                    inner_loop["job_description"] = job_description
                    print(f"  → Parsing with LLM...")
                    if not job_description:
                        print(f"  ✗ Failed to scrape job description")
                        continue
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
                    job_source = "jobscout24"
                    print(f"  → Inserting into database...")
                    success = insert_job(
                        connection=connection,
                        job_id=inner_loop["job_id"],
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
