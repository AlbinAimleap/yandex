import requests
import json
from bs4 import BeautifulSoup
from input_files.utils import load_items
from urllib.parse import urlparse
import pandas as pd
import asyncio
import aiohttp
import logging
import csv
from datetime import datetime, timezone
import argparse
from pathlib import Path

# Set up logging configuration
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

monitor = {}

class YandexMarketReviews:
    def __init__(self):
        logging.info("Starting YandexMarketReviews")
        self.data = []
        self.missing = []
        self.items = load_items()
        self.url = "https://market.yandex.ru/api/render-lazy?w=%40card%2FReviewsLayout"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'cookie': 'yandexuid=1704044661726667882; yashr=1570000711726667882; cmp-merge=true; reviews-merge=true; skid=833951061726805537; nec=0; yuidss=1704044661726667882; ymex=2042165542.yrts.1726805542; receive-cookie-deprecation=1; _ym_uid=1726805541493289859; _ym_d=1726805542; yandexmarket=48%2CRUR%2C1%2C%2C%2C%2C2%2C0%2C0%2C213%2C0%2C0%2C12%2C0%2C0; is_gdpr=0; is_gdpr_b=CLmcHRCKlAI=; oq_last_shown_date=1727159318330; oq_shown_onboardings=%5B%5D; i=0ZCEwmoWq/cxAJG29go2kniZJaEtOVQoKiC9zAMJJVGGNXuQkrPCHqPBBXqIwqyck5F2BI1HVUSx5CNZ7wf/wP9MwpY=; muid=1152921512419772384%3A4NeHeaNYTl%2FVg4lLt2koIU0ij6obYj%2BA; gdpr=0; global_delivery_point_skeleton={%22regionName%22:%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%22%2C%22addressLineWidth%22:49.400001525878906}; server_request_id_market:index=1728966528343%2Fa2970639366e06f3bf1ec0637c240600%2F1%2F1; visits=1726805537-1728964824-1729077329; parent_reqid_seq=1728964828486%2F4021ee00e95bd3ee9f596efe7b240600%2F1%2F1%2C1728966528343%2Fa2970639366e06f3bf1ec0637c240600%2F1%2F1%2C1728966533632%2Ff599fd53ed80354b05d210647c240600%2F1%2F1%2C1728966551328%2Fa3280a19ac11b2dfe2d81e657c240600%2F1%2F1%2C1729077329315%2F2f809a7f0e50663caec6003096240600%2F1%2F1; rcrr=true; _ym_isad=2; bh=EkAiR29vZ2xlIENocm9tZSI7dj0iMTI5IiwgIk5vdD1BP0JyYW5kIjt2PSI4IiwgIkNocm9taXVtIjt2PSIxMjkiKgI/MDoJIldpbmRvd3MiYNXAvrgGah7cyuH/CJLYobEDn8/h6gP7+vDnDev//fYPtZbNhwg=; _yasc=Xbk85W+mVcW5j86T2+irA2EIXwwM+VKROLp4LoWJP9ohWhrjSRa/Fbu6wj7gdQByACv/B58SCLJIEg==',
            'origin': 'https://market.yandex.ru',
            'priority': 'u=1, i',
            'referer': 'https://market.yandex.ru/product--televizor-philips-50pus8507-60/1808094064/reviews?sku=101903223819&uniqueId=34099948&do-waremd5=Ba2XyJdVHwBsRT99DU3FBQ',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sk': 's8bb5657057c00ed669fca1fd59a1f7b2',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'x-market-app-version': '2024.10.9.3-desktop.t2622855680',
            'x-market-core-service': 'default',
            'x-market-first-req-id': '1729077329315/2f809a7f0e50663caec6003096240600/1/1',
            'x-market-front-glue': '1729077329329967',
            'x-market-page-id': 'market:product-reviews'
            }

        self.csv_file = 'scraped_reviews_test.csv'
        self.initialize_csv()

    def initialize_csv(self):
        """Initialize CSV with headers if not exists."""
        headers = ["product_sku", "adminproductname", "scope", "source_code", "product_link", 
                   "data_author", "date_published", "data_content", "data_score", "url_status"]
        df = pd.DataFrame(columns=headers)
        df.to_csv(self.csv_file, index=False)

    def set_payload(self, path, page=0):
        return{
    "widgets": [
        {
            "lazyId": "cardReviewsLayout43",
            "widgetName": "@card/ReviewsLayout",
            "options": {
                "resolverParams": {},
                "widgetName": "@card/ReviewsLayout",
                "id": "ReviewsLayoutRenderer",
                "slotOptions": {"dynamic": True},
                "className": "",
                "needToProvideData": False,
                "wrapperProps": {},
                "layoutOptions": {
                    "entityWrapperProps": {"paddings": {"top": "5", "bottom": "5"}}
                },
                "ignoreRemixGrid": False,
                "forceCountInRow": False,
                "isChefRemixExp": False,
                "extraProps": {
                    "page": str(page),
                    "params": {"reviewPage": str(page), "customConfigName": "all_product_reviews_web_next_page"}
                },
                "widgetSource": "default"
            },
            "slotOptions": {"dynamic": True}
        }
    ],
    "path": f"{path}/reviews",
    "widgetsSource": "default",
    "experimental": {}
}

    def handle_missing(self, item, status):
        review_data = {
                "product_sku": item.get('product_sku', ""),
                "adminproductname": item.get('adminproductname', ""),
                "scope": item.get('scope', ""),
                "source_code": item.get('source_code', ""),
                "product_link": item.get('product_link'),
                "data_author": "",
                "date_published": "", 
                "data_content": "",
                "data_score": "",
                "url_status": status,
                "date_published_parsed": datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            }
        self.data.append(review_data)

    async def fetch_reviews(self, product_link, item, session):
        page = 0
        while True:
            if not product_link:
                break
            parsed_url = urlparse(product_link.split("?")[0])
            path = parsed_url.path.replace('reviews/', '').replace('reviews', '').rstrip('/')
            payload = self.set_payload(path, page)
            payload = json.dumps(payload)
            async with session.post(self.url, headers=self.headers, data=payload) as response:
                logging.info(f"Response status: {response.status} for {product_link}")
                if response.status == 200:
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    reviews = soup.find_all('div', attrs={'data-apiary-widget-name': '@card/Review'})

                    if not reviews:
                        logger.info(f"No reviews found for {product_link}")
                        self.handle_missing(item, "No Reviews Found")
                        break
                    else:
                        for review in reviews:
                            self.parse_review(review, item)
                    page += 1
                else:
                    self.missing.append(item)
                    self.handle_missing(item, "Not Found")
                    

    def parse_review(self, review, item):
        widgets = json.loads(review.find("noframes", class_="apiary-patch").text)["widgets"]
        
        header = widgets.get("@card/ReviewHeader")
        
        if not header:
            return
        
        for key in header.keys():
            review_item = header[key]["reviewItem"]
            
            author = review_item.get("author", {}).get("nickname")
            rating = review_item.get("rating")
            published_date = review_item.get("reviewDate")
            review_text = review_item.get("advantages", "") + review_item.get("disadvantages", "") + review_item.get("commonText", "")

            review_data = {
                "product_sku": item.get('product_sku', "N/A"),
                "adminproductname": item.get('adminproductname', "N/A"),
                "scope": item.get('scope', "N/A"),
                "source_code": item.get('source_code', "N/A"),
                "product_link": item.get('product_link'),
                "data_author": author.strip() if author else "",
                "date_published": published_date, 
                "data_content": review_text or "",
                "data_score": rating if rating else "",
                "url_status": "Data Extracted",
                "date_published_parsed": datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            }
            # print(review_data)
            self.data.append(review_data)

    def save_to_csv(self, item):
        df = pd.DataFrame([item])
        df.to_csv(self.csv_file, mode='a', header=False, index=False)
        logging.info(f"Saved data to CSV: {item}")

    async def run(self, output_file: Path):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_reviews(item['product_link'], item, session) for item in self.items]
            await asyncio.gather(*tasks)
            
            for item in self.data:
                if item["date_published"]:
                    date = datetime.fromtimestamp(int(item["date_published"]) / 1000, tz=timezone.utc)
                    date_published = date.strftime("%Y-%m-%d %H:%M:%S")
                    item["date_published"] = date_published
            
            if output_file.suffix.lower() == ".csv":
                pd.DataFrame(self.data).to_csv(output_file, index=False)
            elif output_file.suffix.lower() == ".json":
                with open(output_file, "w") as f:
                    json.dump(self.data, f, indent=4)
            elif output_file.suffix.lower() == ".xlsx":
                pd.DataFrame(self.data).to_excel(output_file, index=False, engine="openpyxl")
            else:
                raise ValueError("Unsupported file type. Please use .csv, .json, or .xlsx.")
            
            with open("yandex_missing.json", "w") as f:
                json.dump(self.missing, f, indent=4)
            
            logger.info(f"Out of {len(self.items)} items, {len(self.missing)} items were missing")
            logger.info(f"Total reviews extracted: {len(self.data)}")

def main():
    parser = argparse.ArgumentParser(description="Extract Yandex Market reviews")
    parser.add_argument("output_file", help="Output file name")
    args = parser.parse_args()
    path = Path(args.output_file)
    
    if not path.suffix.lower() in [".csv", ".json", ".xlsx"]:
        raise ValueError("Unsupported file type. Please use .csv, .json, or .xlsx.")
    
    yandex_reviews = YandexMarketReviews()
    asyncio.run(yandex_reviews.run(path))

if __name__ == "__main__":
    main()
