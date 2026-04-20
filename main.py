import json
import time
import re
import csv
import os
from datetime import datetime
from curl_cffi import requests
from lxml import html
import gspread
from oauth2client.service_account import ServiceAccountCredentials


class PazaruvajFinalScraper:
    def __init__(self):
        self.base_url = "https://www.pazaruvaj.com"
        self.impersonate = "chrome110"
        self.headers = [
            "Product_URL", "Product_ID", "Parent_ID", "Title", "Storage_Variation",
            "Category", "Brand", "Price_EUR", "Seller_Name", "EAN", "MPN",
            "Images", "Specs", "Description", "Stock_Status", "Last_Updated"
        ]
        self.visited_ids = set()
        self.setup_google_sheets()

    def setup_google_sheets(self):
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            # GitHub Secrets থেকে ডাটা পড়া
            creds_json = os.environ.get('G_SHEET_CREDS')
            if creds_json:
                creds_dict = json.loads(creds_json)
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            else:
                # লোকাল পিসিতে টেস্ট করার জন্য credentials.json ফাইল খুঁজবে
                creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open("Pazaruvaj Smartfones")
            self.worksheet = self.spreadsheet.worksheet("Raw_Data")

            # --- অটো ক্লিন লজিক ---
            # নতুন ডাটা পাঠানোর আগে শিট পরিষ্কার করে ফেলা (হেডার বাদে)
            if self.worksheet.row_count > 1:
                self.worksheet.clear()
                self.worksheet.append_row(self.headers)
            print("Successfully connected and cleaned Raw_Data sheet.")
        except Exception as e:
            print(f"Google Sheets Connection Error: {e}")
            self.worksheet = None

    def upload_to_gsheet(self, data_list):
        if self.worksheet:
            try:
                rows = []
                for item in data_list:
                    row = [item.get(h, "") for h in self.headers]
                    rows.append(row)
                self.worksheet.append_rows(rows)
            except Exception as e:
                print(f"GSheet Upload Error: {e}")

    def get_response(self, url):
        try:
            res = requests.get(url, impersonate=self.impersonate, timeout=30)
            return res if res.status_code == 200 else None
        except:
            return None

    def get_product_links(self, category_url):
        page = 1
        all_links = []
        xpath_query = '//li[@class="c-product-list__item"]//a[contains(@class, "c-product__secondary-cta") or (parent::h3 and not(ancestor::li//a[contains(@class, "c-product__secondary-cta")]))]'
        print("\n--- Finding Links ---")
        while True:
            current_url = f"{category_url}?f={page}" if page > 1 else category_url
            res = self.get_response(current_url)
            if not res: break
            tree = html.fromstring(res.text)
            links = tree.xpath(xpath_query)
            page_found = 0
            for a in links:
                href = a.get('href')
                if href and "/p/" in href:
                    full_link = href if href.startswith('http') else self.base_url + href
                    if full_link not in all_links:
                        all_links.append(full_link)
                        page_found += 1
            if page_found == 0: break
            print(f"Page {page}: Found {page_found} links.")
            if 'rel="next"' not in res.text: break
            page += 1
            time.sleep(1)
        return all_links

    def scrape_product_details(self, url, is_sub_variant=False, parent_id_val=None):
        res = self.get_response(url)
        if not res: return None
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>', res.text)
        if not match: return None

        try:
            data = json.loads(match.group(1))
            props = data.get('props', {}).get('pageProps', {})
            detail = props.get('initialData', {}).get('productDetail') or props.get('productDetail')
            if not detail: return None

            product = detail.get('product', {})
            raw_id = product.get('localId')
            current_p_id = f"p{raw_id}"
            if current_p_id in self.visited_ids: return None
            self.visited_ids.add(current_p_id)

            brand = product.get('producers', [{}])[0].get('name', 'N/A')
            cat = " > ".join([b.get('name', '') for b in detail.get('category', {}).get('breadcrumbs', [])])
            desc = re.sub('<[^<]+?>', '', product.get('description', '')).strip()
            attrs = product.get('attributes', {}).get('attributes', [])
            specs = "|".join([f"{a['name']}: {a['value']}" for a in attrs])
            ean = next((a['value'] for a in attrs if 'ean' in a['name'].lower()), "N/A")
            mpn = next((a['value'] for a in attrs if 'mpn' in a['name'].lower()), "N/A")
            imgs = ",".join(filter(None, [i.get('url') for i in product.get('media', {}).get('images', [])]))
            price = detail['product']['minPrice']

            # Seller Name
            seller = "N/A"
            offers = detail.get('offers', {}).get('regular', []) + detail.get('offers', {}).get('bidding', [])
            if offers: seller = sorted(offers, key=lambda x: x.get('price', 999999))[0].get('shop', {}).get('name',
                                                                                                            'N/A')

            storage = "Standard"
            if detail.get('variants'):
                curr_v = next((v for v in detail['variants'] if str(v.get('platformProductId')) == str(raw_id)), None)
                if curr_v: storage = curr_v.get('value')

            rows = []
            rows.append({
                "Product_URL": url, "Product_ID": current_p_id,
                "Parent_ID": parent_id_val if parent_id_val else current_p_id,
                "Title": product.get('name'), "Storage_Variation": storage, "Category": cat, "Brand": brand,
                "Price_EUR": price, "Seller_Name": seller, "EAN": ean, "MPN": mpn, "Images": imgs,
                "Specs": specs, "Description": desc, "Stock_Status": "In Stock",
                "Last_Updated": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
            })

            if not is_sub_variant:
                for v in detail.get('variants', []):
                    v_url = f"{self.base_url}/p/{v.get('slug', {}).get('value', 'v')}-p{v.get('platformProductId')}/"
                    time.sleep(1)
                    v_data = self.scrape_product_details(v_url, True, current_p_id)
                    if v_data: rows.extend(v_data)
            return rows
        except:
            return None

    def run(self, cat_url):
        links = self.get_product_links(cat_url)
        print(f"\nScraping {len(links)} Products...")
        for i, link in enumerate(links):
            print(f"[{i + 1}/{len(links)}] Scraping: {link}")
            data = self.scrape_product_details(link)
            if data:
                self.upload_to_gsheet(data)
            time.sleep(1)


if __name__ == "__main__":
    scraper = PazaruvajFinalScraper()
    scraper.run("https://www.pazaruvaj.com/c/mobilni-telefoni-gsm-c3277/")