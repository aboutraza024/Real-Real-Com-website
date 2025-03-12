import csv
from botasaurus.request import request, Request
from botasaurus.soupify import soupify
from datetime import datetime
from datetime import date

input_file="output/rings_urls.csv"
output_file="output/rings_data.csv"
data_ring=[]
details_list=[]

header=["BRAND","PRODUCT NAME","SIZE","RETAIL PRICE / PERCENTAGE OF DISCOUNT","DISCOUNTED PRICE","DESCRIPTION","DETAILS","ITEM_ID","DATE"]

proxy_data = {
    'http': 'http://ue3a39fc857ea05cb-zone-custom-region-tr:ue3a39fc857ea05cb@118.193.59.102:2333',
    'https': 'http://ue3a39fc857ea05cb-zone-custom-region-tr:ue3a39fc857ea05cb@118.193.59.102:2333'
}

def read_url_from_csv():
    with open(input_file, mode="r", newline="", encoding='utf-8') as file:
        reader = csv.reader(file)
        crawler_urls = [row[0] for row in reader if row]
        return crawler_urls[1:]


with open(output_file, mode="w", newline="", encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(header)

def write_to_csv(link):
    with open(output_file,mode="a",newline="",encoding='utf-8')as file:
        writer=csv.writer(file)
        writer.writerow(link)

@request
def scrape_data(request: Request, link):
    try:
        print(link)
        response = request.get(link,proxies=proxy_data)
        if response.status_code == 200:
            soup = soupify(response)

            data_ring.clear()
            details_list.clear()

            brand = soup.find('div', class_="brand")
            product_name = soup.find('div', class_="product-name")
            size = soup.find('div', class_="pdp-title__size")
            retail_price = soup.find('div', class_="price-info__msrp")
            final_price = soup.find('div', class_="price-info-values__final-price")
            description = soup.find('div', class_="pdp-description")
            details_info = soup.find_all('ul', class_='product-details-group')

            # Extract text safely (avoiding NoneType errors)
            data_ring.append(brand.text if brand else "N/A")
            data_ring.append(product_name.text if product_name else "N/A")
            data_ring.append(size.text if size else "N/A")
            data_ring.append(retail_price.text if retail_price else "N/A")
            data_ring.append(final_price.text if final_price else "N/A")
            data_ring.append(description.text if description else "N/A")

            for d in details_info:
                details_list.append(d.text.strip())

            data_ring.append("|".join(details_list))  # Join list to avoid nested lists
            item_id = details_info[-1].text if details_info else "N/A"
            data_ring.append(item_id)
            data_ring.append(date.today())  # Append current date

            print(data_ring)
            write_to_csv(data_ring)
        else:
            print("ERROR", response.status_code)

    except Exception as e:
        print(f"EXCEPTION: {str(e)}")




result=read_url_from_csv()[0:100]   #scrape 1 50 urls


scrape_data(result)



