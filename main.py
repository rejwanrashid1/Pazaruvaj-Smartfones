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

class PazaruvajUltimateScraper:
    def __init__(self):
        self.base_url = "https://www.pazaruvaj.com"
        self.filename = "Master_Scrape.csv"
        self.impersonate = "chrome110"
        self.headers = [
            "Product_URL", "Product_ID", "Parent_ID", "Title", "Storage_Variation",
            "Category", "Brand", "Price_EUR", "Seller_Name", "EAN", "MPN",
            "Images", "Specs", "Description", "Stock_Status", "Last_Updated"
        ]
        self.visited_ids = set()
        self.setup_google_sheets()
        self.init_csv()

    def setup_google_sheets(self):
        """গুগল শিট কানেক্ট এবং অটো-ক্লিন লজিক"""
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            
            # GitHub Secrets থেকে ক্রেডেনশিয়াল নেওয়া
            creds_json = os.environ.get('G_SHEET_CREDS')
            if creds_json:
                creds_dict = json.loads(creds_json)
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            else:
                # লোকাল পিসিতে টেস্টের জন্য
                creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open("Pazaruvaj Smartfones")
            self.worksheet = self.spreadsheet.worksheet("Raw_Data")
            
            # ডাটা পাঠানোর আগে শিট পুরোপুরি পরিষ্কার করা (হেডার সহ)
            self.worksheet.clear()
            self.worksheet.append_row(self.headers)
            print("Successfully connected to Google Sheet and cleaned Raw_Data.")
        except Exception as e:
            print(f"Google Sheets Connection Error: {e}")
            self.worksheet = None

    def init_csv(self):
        """লোকাল ব্যাকআপের জন্য CSV ফাইল তৈরি"""
        with open(self.filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()

    def save_to_csv(self, data_list):
        with open(self.filename, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerows(data_list)

    def upload_to_gsheet(self, data_list):
        """গুগল শিটে রো (Row) আপলোড করা"""
        if self.worksheet:
            try:
                rows = [[item.get(h, "") for h in self.headers] for item in data_list]
                self.worksheet.append_rows(rows)
            except Exception as e:
                print(f"GSheet Upload Error: {e}")

    def get_response(self, url):
        try:
            res = requests.get(url, impersonate=self.impersonate, timeout=30)
            return res if res.status_code == 200 else None
        except: return None

    def get_product_links(self, category_url):
        """আপনার দেওয়া স্পেসিফিক XPath ব্যবহার করে সব লিঙ্ক খোঁজা"""
        page = 1
        all_links = []
        xpath_query = '//li[@class="c-product-list__item"]//a[contains(@class, "c-product__secondary-cta") or (parent::h3 and not(ancestor::li//a[contains(@class, "c-product__secondary-cta")]))]'

        print("\n--- Phase 1: Finding Product Links ---")
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
            print(f"Page {page}: Found {page_found} links. Total: {len(all_links)}")
            if 'rel="next"' not in res.text: break
            page += 1
            time.sleep(1)
        return all_links

    def scrape_product_details(self, url, is_sub_variant=False, parent_id_val=None):
        """প্রোডাক্ট ডিটেইলস এবং ভ্যারিয়েশন স্ক্র্যাপ করা"""
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

            # বেসিক ইনফো
            brand = product.get('producers', [{}])[0].get('name', 'N/A')
            cat = " > ".join([b.get('name', '') for b in detail.get('category', {}).get('breadcrumbs', [])])
            desc = re.sub('<[^<]+?>', '', product.get('description', '')).strip()
            
            # Attributes (Specs, EAN, MPN)
            attrs = product.get('attributes', {}).get('attributes', [])
            specs = "|".join([f"{a['name']}: {a['value']}" for a in attrs])
            ean = next((a['value'] for a in attrs if 'ean' in a['name'].lower()), "N/A")
            mpn = next((a['value'] for a in attrs if 'mpn' in a['name'].lower()), "N/A")
            
            # Images
            imgs = ",".join(filter(None, [i.get('url') for i in product.get('media', {}).get('images', [])]))
            if not imgs and product.get('mainImage'): imgs = product['mainImage'].get('url')
            
            # Price & Seller
            price = detail['product']['minPrice']
            seller = "N/A"
            all_offers = (detail.get('offers', {}).get('regular', []) + detail.get('offers', {}).get('bidding', []))
            if all_offers:
                seller = sorted(all_offers, key=lambda x: x.get('price', 999999))[0].get('shop', {}).get('name', 'N/A')

            # Storage Variation Logic (Fixing "Standard" issue)
            storage = "Standard"
            variants_list = detail.get('variants', [])
            if variants_list:
                curr_v = next((v for v in variants_list if str(v.get('platformProductId')) == str(raw_id)), None)
                if curr_v: storage = curr_v.get('value')
            
            # Backup: টাইটেল থেকে মেমোরি খোঁজা
            if storage == "Standard":
                mem_match = re.search(r'(\d+\s*(?:GB|TB))', product.get('name', ''), re.IGNORECASE)
                if mem_match: storage = mem_match.group(1)

            rows = []
            rows.append({
                "Product_URL": url, "Product_ID": current_p_id, "Parent_ID": parent_id_val if parent_id_val else current_p_id,
                "Title": product.get('name'), "Storage_Variation": storage, "Category": cat, "Brand": brand,
                "Price_EUR": price, "Seller_Name": seller, "EAN": ean, "MPN": mpn, "Images": imgs,
                "Specs": specs, "Description": desc, "Stock_Status": "In Stock",
                "Last_Updated": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
            })

            # যদি মেইন পেজ হয়, তবে সাব-ভ্যারিয়েন্ট লিঙ্কগুলোতেও যাবে
            if not is_sub_variant:
                for v in variants_list:
                    v_id = f"p{v.get('platformProductId')}"
                    if v_id not in self.visited_ids:
                        v_url = f"{self.base_url}/p/{v.get('slug', {}).get('value', 'v')}-p{v.get('platformProductId')}/"
                        time.sleep(1) # ডিলে
                        v_data = self.scrape_product_details(v_url, True, current_p_id)
                        if v_data: rows.extend(v_data)
            return rows
        except Exception as e:
            print(f"Error on {url}: {e}")
            return None

    def run(self, cat_url):
        links = self.get_product_links(cat_url)
        print(f"\n--- Phase 2: Scraping {len(links)} Products & Variants ---")
        for i, link in enumerate(links):
            print(f"[{i+1}/{len(links)}] Scraping: {link}")
            data = self.scrape_product_details(link)
            if data:
                self.save_to_csv(data)
                self.upload_to_gsheet(data)
            time.sleep(1)
        print("\nCOMPLETED! Master_Scrape.csv and Google Sheet updated.")

if __name__ == "__main__":
    target = "https://www.pazaruvaj.com/c/mobilni-telefoni-gsm-c3277/"
    scraper = PazaruvajUltimateScraper()
    scraper.run(target)
