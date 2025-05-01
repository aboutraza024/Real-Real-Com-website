import random
from botasaurus.browser import browser, Driver
import csv
from urllib.parse import urljoin
import os
import threading
import logging
from datetime import datetime
import time

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
output_file = "output/crawled_urls.csv"
base_url = "https://www.therealreal.com/"

proxy_data = {
    'http': 'http://VCrI40bs0ca4Jhy0:AGCuQwc4rHWltmlJ@geo.iproyal.com:12321',
    'https': 'http://VCrI40bs0ca4Jhy0:AGCuQwc4rHWltmlJ@geo.iproyal.com:12321'
}


def write_to_csv(link):
    with write_lock:
        with open(output_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([link])


with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["RINGS_URLS"])


@browser
def scrape_data(driver: Driver, data):
    try:
        ring_links = []
        try:
            driver.get("https://www.therealreal.com/")
            driver.click("div[class='sign-in-sign-up-footer'] a[tabindex='0']:nth-of-type(1)")
            user_name = input("Enter User Name: ")
            password = input("Enter Password:")
            driver.type("input[id='user_email']:nth-of-type(1)", user_name)
            driver.type('li[aria-label="Password"] input[tabindex="0"]', password)
            driver.click('li[id="user_submit_action"] input[tabindex="0"]')
            time.sleep(2)
        except Exception as e:
            print("Error ", e)
        input_query = input("Enter What You Want To Search : ")
        num_of_pages = int(input("Enter Number of pages you want to crawl : "))
        driver.type("input[aria-owns='search-listbox-mobile']", input_query)
        driver.click('[aria-label="Submit search"]')
        time.sleep(random.uniform(1, 2))
        for i in range(1, num_of_pages + 1):

            print("Page NO", i)

            all_links = driver.select_all(
                "a[class='product-card__description product-card__link js-product-card-link']")

            print("Crawled Links", len(all_links))

            for j in all_links:
                link = j.get_attribute('href')
                link = str(link)
                full_url = urljoin(base_url, link)
                write_to_csv(full_url)
            pagination = driver.select_all(
                'a[class="pagination__number js-pagination__number plp-pagination__number--suffix"]')

            pagination[1].click()
            time.sleep(random.uniform(1, 3))
    except Exception as e:
        print("ERROR ", e)

    driver.close()


if __name__ == "__main__":
    scrape_data()
