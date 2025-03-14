
num_of_pages=2   #crawling links of first 2 pages
from botasaurus.request import request, Request
from botasaurus.soupify import soupify
import csv
from urllib.parse import urljoin
import os
import threading
import logging
from datetime import datetime
import time
import random

delay = random.uniform(2, 5)
time.sleep(delay)

os.makedirs("logs", exist_ok=True)
os.makedirs("output", exist_ok=True)

log_filename = f"logs/crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


write_lock = threading.Lock()
output_file = "output/rings_urls.csv"
base_url = "https://www.therealreal.com/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

proxy_data = {
    "http": "http://a91ac6e8a76376a0:RNW78Fm5@res.proxy-seller.com:10000",
    "https": "http://a91ac6e8a76376a0:RNW78Fm5@res.proxy-seller.com:10000",
}

def write_to_csv(link):
    with write_lock:
        with open(output_file, mode="a", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow([link])


# Initialize CSV file with header
with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["RINGS_URLS"])




@request(cache=False,
    parallel=10,
    max_retry=20,
    close_on_crash=True,
    raise_exception=True,
    create_error_logs=False,
    output=None,)
def crawl_rings(request: Request, data):
    page_number=data["page_number"]
    ring_links=[]
    try:
        delay = random.uniform(3, 8)
        time.sleep(delay)
        response = request.get(
            f"https://www.therealreal.com/sales/designer-jewelry?after=YXJyYXljb25uZWN0aW9uOjIzNQ%3D%3D&first=120&page={page_number}&taxons%5B%5D=1099",proxies=proxy_data)
        delay = random.uniform(1, 3)
        time.sleep(delay)
        if response.status_code == 200:
            soup = soupify(response)
            rings_links = soup.find_all('a', class_='product-card__description product-card__link js-product-card-link')
            print(len(rings_links))
            for ring_url in rings_links:
                links = ring_url.get("href")
                ring_links.append(links)
                links = str(links)
                full_url = urljoin(base_url, links)
                write_to_csv(full_url)
            logger.info(f"Page {page_number}: Found {len(rings_links)} links")
        else:
            print("[ERROR]",response.status_code)
    except Exception as e:
        logger.error(f"Page {page_number} error: {str(e)}")

    return ring_links

if __name__ == "__main__":
    pages_data = [{"page_number": i} for i in range(1, num_of_pages + 1)]
    logger.info(f"Starting crawl for {len(pages_data)} pages")
    result = crawl_rings(pages_data)
    logger.info(f"Crawling complete. Total links collected: {len(result)}")











