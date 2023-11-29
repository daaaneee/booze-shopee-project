import sqlite3
import requests
from time import sleep
import requests.exceptions
import csv
from fake_useragent import UserAgent
from datetime import datetime

# Read shop ID from a csv file
try:
    with open('booze_shop_id.csv', 'r') as file:
        shop_ids = [line.strip() for line in file]
except FileNotFoundError:
    print('Invalid file name.')
except Exception as e:
    print(f'An error occurred: {e}')


# Date of extraction process
extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Database file_name
db_filename = f"product_raw_data_{datetime.now().strftime('%Y-%m-%d')}.db"

# CSV File name and Header
csv_filename = f"product_raw_data_{datetime.now().strftime('%Y-%m-%d')}.csv"
csv_header = ["product_name", "current_stock", "shop_name", "shop_id", "brand",
                    "unique_item_id", "sold_per_month", "historical_sold", "liked_count",
                    "variation_type", "variation", "current_price", "min_price", "max_price",
                    "lowest_price_guarantee", "current_discount_percentage", "rating_star", "shopee_verified",
                    "official_shop", "cc_installment", "none_cc_installment", "preferred_seller",
                    "shop_location", "shop_rating", "cod", "extraction_date"]

# Initialize an instance for user agent
user_agent = UserAgent()


# Rate limit in seconds(adjust as needed)
sleep_in_seconds = 5


# Make API request
def make_api_request(shop_ids, headers):
    url = f'https://shopee.ph/api/v4/recommend/recommend?bundle=shop_page_product_tab_main&limit=999&offset=0&section=shop_page_product_tab_main_sec&shopid={shop_ids}'

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception if the status code is not 200

        if response.status_code == 200:
            data = response.json().get('data', {})
            sections = data.get('sections', [])
            if sections:
                items = sections[0].get('data', {}).get('item', [])
                if not items:
                    print(f"No items found for seller {shop_ids}. Skipping.\n")
                    return None  # return none when there are no available products of the shop
                return items
            else:
                print(f"No data found for seller {shop_ids}. Skipping.")
                return None  # return none when there is no data
        else:
            print(f"Request for seller {shop_ids} was not successful. Status Code: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Request error {str(e)}. Skipping.")
        return None  # Return none when a request error occurs.


# Extract the data from the JSON Response
def extract_data(item):
    tier_variations = item.get('tier_variations') or None
    variation_name = tier_variations[0].get('name') if tier_variations else None
    variation_options = tier_variations[0].get('options') if tier_variations else None

    discount = item['discount'] if ('discount' in item and item['discount'] is not None) else 0

    data_row = [
        item.get('name'), item.get('stock'), item.get('shop_name'), item.get('shopid'),
        item.get('brand'),
        item.get('itemid'), item.get('sold'), item.get('historical_sold'), item.get('liked_count'),
        variation_name, str(variation_options),
        item.get('price') / 100000 if item.get('price') else None,
        item.get('price_min') / 100000 if item.get('price_min') else None,
        item.get('price_max') / 100000 if item.get('price_max') else None,
        item.get('has_lowest_price_guarantee'), discount,
        item.get('item_rating').get('rating_star'),
        item.get('shopee_verified'),
        item.get('is_official_shop'),
        item.get('is_cc_installment_payment_eligible'),
        item.get('is_non_cc_installment_payment_eligible'),
        item.get('is_preferred_plus_seller'),
        item.get('shop_location'),
        item.get('shop_rating'),
        item.get('can_use_cod'),
    ]

    return data_row

# Create the table on SQL


with sqlite3.connect(db_filename) as conn:
    try:
        c = conn.cursor()
        c.execute('DROP TABLE IF EXISTS Products')
        c.execute('''
            CREATE TABLE IF NOT EXISTS Products (
                "product_name" TEXT,
                "current_stock" INTEGER,
                "shop_name" TEXT,
                "shop_id" INTEGER,
                "brand" TEXT,
                "unique_item_id" INTEGER,
                "sold_per_month" INTEGER,
                "historical_sold" INTEGER,
                "liked_count" INTEGER,
                "variation_type" TEXT,
                "variation" TEXT,
                "current_price" INTEGER,
                "min_price" INTEGER,
                "max_price" INTEGER,
                "lowest_price_guarantee" TEXT,
                "current_discount_percentage" INTEGER,
                "rating_star" INTEGER,
                "shopee_verified" TEXT,
                "official_shop" TEXT,
                "cc_installment" TEXT,
                "none_cc_installment" TEXT,
                "preferred_seller" TEXT,
                "shop_location" TEXT,
                "shop_rating" INTEGER,
                "cod" TEXT,
                "extraction_date" DATETIME
            )
        ''')
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")


# save extracted data to SQL
def save_data_db(data, sqlite_filename):
    data = [row + [extraction_date] for row in data]
    with sqlite3.connect(sqlite_filename) as conn:
        c = conn.cursor()
        c.executemany('''
                    INSERT INTO Products 
                    ("product_name", "current_stock", "shop_name", "shop_id", "brand", 
                    "unique_item_id", "sold_per_month", "historical_sold", "liked_count", 
                    "variation_type", "variation", "current_price", "min_price", "max_price", 
                    "lowest_price_guarantee", "current_discount_percentage", "rating_star", "shopee_verified", 
                    "official_shop", "cc_installment", "none_cc_installment", "preferred_seller", 
                    "shop_location", "shop_rating", "cod", "extraction_date")
                    VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data)


# Write the header to the CSV file
with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(csv_header)


def save_data_csv(data, csv_filename):
    data = [row + [extraction_date] for row in data]
    with open(csv_filename, mode='a', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(data)


def main():
    records = []  # initialize an empty list to count the number of records

    for shop_id in shop_ids:
        headers = {'User-Agent': user_agent.random}
        data = make_api_request(shop_id, headers)
        if data is None:
            continue

        data_rows = [extract_data(item) for item in data]
        save_data_db(data_rows, db_filename)
        save_data_csv(data_rows, csv_filename)
        print(f'{len(data)} items have been written to {db_filename}. {shop_id}')
        records.append(len(data))

        # Implement rate limiting
        print(f'Sleeping for {sleep_in_seconds} seconds.')
        sleep(sleep_in_seconds)
        print('Resuming. \n')

    print(f'Total of {sum(records)} records has been extracted.')
    print(f'Total number of shop scraped: {len(records)}')
    print(f'Total number of shop that has no stocks: {len(shop_ids) - len(records)}')
    print(f'Check your extracted data on {db_filename}.')


if __name__ == "__main__":
    main()
