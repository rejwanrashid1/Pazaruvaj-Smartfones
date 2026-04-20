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


class PazaruvajMasterScraper:
    def __init__(self):
        # ১. বেসিক সেটিংস
        self.base_url = "https://www.pazaruvaj.com"
        self.filename = "Master_Scrape.csv"
        self.impersonate = "chrome110"
        self.headers = [
            "Product_URL", "Product_ID", "Parent_ID", "Title", "Storage_Variation",
            "Category", "Brand", "Price_EUR", "Seller_Name", "EAN", "MPN",
            "Images", "Specs", "Description", "Stock_Status", "Last_Updated"
        ]
        self.visited_ids = set()

        # ২. গুগল শিট কানেকশন এবং অটো-ক্লিন
        self.sheet_name = "Pazaruvaj Smartfones"
        self.setup_google_sheets()

        # ৩. লোকাল CSV ফাইল ইনিশিয়ালাইজেশন
        self.init_csv()

    def setup_google_sheets(self):
        """গুগল শিট কানেক্ট করবে এবং রান শুরু হওয়ার আগে Raw_Data ক্লিন করবে"""
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds_json = os.environ.get('G_SHEET_CREDS')

            if creds_json:
                creds_dict = json.loads(creds_json)
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            else:
                creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open(self.sheet_name)
            self.worksheet = self.spreadsheet.worksheet("Raw_Data")

            # অটো-ক্লিন: নতুন ডাটা আসার আগে পুরনো সব মুছে হেডার বসানো
            self.worksheet.clear()
            self.worksheet.append_row(self.headers)
            print(f"Connected to {self.sheet_name}. Raw_Data sheet has been cleaned.")
        except Exception as e:
            print(f"Google Sheets Setup Error: {e}")
            self.worksheet = None

    def update_live_status(self, message):
        """ড্যাশবোর্ডে লাইভ দেখানোর জন্য শিটে স্ট্যাটাস আপডেট করা"""
        if not self.worksheet: return
        try:
            # Process_Log শিটের H1 সেলে স্ট্যাটাস আপডেট হবে
            status_sheet = self.spreadsheet.worksheet("Process_Log")
            status_sheet.update_acell('H1', f"LIVE: {message} | {datetime.now().strftime('%H:%M:%S')}")
        except: pass

    def init_csv(self):
        with open(self.filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()

    def save_to_csv(self, data_list):
        with open(self.filename, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerows(data_list)

    def upload_to_gsheet(self, data_list):
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
        except:
            return None

    def get_product_links(self, category_url):
        """আপনার দেওয়া স্পেসিফিক XPath ব্যবহার করে সব লিঙ্ক খোঁজা"""
        page = 1
        all_links = []
        xpath_query = '//li[@class="c-product-list__item"]//a[contains(@class, "c-product__secondary-cta") or (parent::h3 and not(ancestor::li//a[contains(@class, "c-product__secondary-cta")]))]'

        print(f"Scanning Category: {category_url}")
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
                
             # --- লাইভ আপডেট এখানে দেওয়া হয়েছে ---
            self.update_live_status(f"Page {page}: Found {page_found} items (Total: {len(all_links)})")
            print(f"Page {page}: Found {page_found} items.")
            
            if 'rel="next"' not in res.text: break
            page += 1
            time.sleep(1)
        return all_links
        
    def extract_json_data(self, html_text):
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>', html_text)
        if match:
            data = json.loads(match.group(1))
            props = data.get('props', {}).get('pageProps', {})
            return props.get('initialData', {}).get('productDetail') or props.get('productDetail')
        return None
        
def clean_html(self, raw_html):
        """HTML ট্যাগ রিমুভ করবে কিন্তু নতুন লাইন (\n) বজায় রাখবে"""
        if not raw_html:
            return "N/A"
        
        # ১. প্যারাগ্রাফ, ব্রেক এবং লিস্ট ট্যাগগুলোকে নিউ-লাইন দিয়ে বদলে দিবে
        # এতে "AppleМодел" না হয়ে "Apple\nМодел" হবে
        text = re.sub(r'<(br|p|div|li|tr|h1|h2|h3)[^>]*>', '\n', raw_html)
        
        # ২. বাকি সব HTML ট্যাগ মুছে ফেলবে
        text = re.sub(r'<[^>]+>', '', text)
        
        # ৩. অতিরিক্ত খালি স্পেস বা মাল্টিপল নিউ-লাইন ক্লিন করবে
        text = re.sub(r'\n\s*\n', '\n', text) 
        
        return text.strip()
    
    def scrape_product_details(self, url, is_sub_variant=False, parent_id_val=None):
        """ভ্যারিয়েশন হ্যান্ডেলিং এবং মেমোরি ডিটেকশন সহ ডিটেইল স্ক্র্যাপার"""
        res = self.get_response(url)
        if not res: return None

        detail = self.extract_json_data(res.text)
        if not detail: return None

        product = detail.get('product', {})
        raw_id = product.get('localId')
        current_p_id = f"p{raw_id}"

        # ডুপ্লিকেট এড়ানো
        if current_p_id in self.visited_ids: return None
        self.visited_ids.add(current_p_id)

        # ডাটা মেপিং
        brand = product.get('producers', [{}])[0].get('name', 'N/A')
        cat = " > ".join([b.get('name', '') for b in detail.get('category', {}).get('breadcrumbs', [])])
        # ডেসক্রিপশন ফরম্যাটিং (আমাদের নতুন ফাংশন ব্যবহার করে)
        raw_description = product.get('description') or ""
        clean_desc = self.clean_html(raw_description)

        # স্পেকস (Specs) ফরম্যাটিং (প্রতিটি আইটেম নতুন লাইনে আসবে)
        attrs = product.get('attributes', {}).get('attributes', [])
        specs = "\n".join([f"{a['name']}: {a['value']}" for a in attrs])
        
        ean = next((a['value'] for a in attrs if 'ean' in a['name'].lower()), "N/A")
        mpn = next((a['value'] for a in attrs if 'mpn' in a['name'].lower()), "N/A")

        imgs = ",".join(filter(None, [i.get('url') for i in product.get('media', {}).get('images', [])]))
        if not imgs and product.get('mainImage'): imgs = product['mainImage'].get('url')

        price = detail['product']['minPrice']

        # Best Seller
        seller = "N/A"
        all_offers = (detail.get('offers', {}).get('regular', []) + detail.get('offers', {}).get('bidding', []))
        if all_offers:
            seller = sorted(all_offers, key=lambda x: x.get('price', 999999))[0].get('shop', {}).get('name', 'N/A')

        # মেমোরি ভ্যারিয়েশন ফিক্স (Standard এর বদলে সঠিক ভ্যালু)
        storage = "Standard"
        variants_list = detail.get('variants', [])
        if variants_list:
            curr_v = next((v for v in variants_list if str(v.get('platformProductId')) == str(raw_id)), None)
            if curr_v: storage = curr_v.get('value')

        if storage == "Standard":
            mem_match = re.search(r'(\d+\s*(?:GB|TB))', product.get('name', ''), re.IGNORECASE)
            if mem_match: storage = mem_match.group(1)

        current_row = [{
            "Product_URL": url, "Product_ID": current_p_id,
            "Parent_ID": parent_id_val if parent_id_val else current_p_id,
            "Title": product.get('name'), "Storage_Variation": storage, "Category": cat, "Brand": brand,
            "Price_EUR": price, "Seller_Name": seller, "EAN": ean, "MPN": mpn, "Images": imgs,
            "Specs": specs, "Description": clean_desc, "Stock_Status": "In Stock",
            "Last_Updated": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        }]

        # সাব-ভ্যারিয়েশন ভিজিট (যদি এটি মেইন কল হয়)
        if not is_sub_variant:
            for v in variants_list:
                v_id = f"p{v.get('platformProductId')}"
                if v_id not in self.visited_ids:
                    v_url = f"{self.base_url}/p/{v.get('slug', {}).get('value', 'v')}-p{v.get('platformProductId')}/"
                    time.sleep(1)
                    v_data = self.scrape_product_details(v_url, True, current_p_id)
                    if v_data: current_row.extend(v_data)

        return current_row
        

                
    def run(self):
        self.update_live_status("Initializing Scraper...")
        """Categories শিট থেকে লিঙ্ক নিয়ে কাজ শুরু করবে"""
        print("\n--- System Starting: Reading Categories from Sheet ---")
        try:
            # গুগল শিট থেকে ক্যাটাগরি লিঙ্ক পড়া
            cat_worksheet = self.spreadsheet.worksheet("Categories")
            all_cat_urls = cat_worksheet.col_values(1)[1:]  # প্রথম কলামের ২ নম্বর রো থেকে সব
            valid_urls = [u.strip() for u in all_cat_urls if u and u.strip().startswith('http')]

            if not valid_urls:
                print("No valid URLs found in 'Categories' sheet.")
                self.update_live_status("Error: No URLs found.")
                return

            print(f"Total Categories to process: {len(valid_urls)}")

            for cat_url in valid_urls:
                self.update_live_status(f"Fetching links for: {cat_url.split('/')[-2]}")
                product_links = self.get_product_links(cat_url)
                print(f"Found {len(product_links)} unique products in this category.")

                for i, link in enumerate(product_links):
                    self.update_live_status(f"Scraping Product {i+1}/{len(product_links)}")
                    print(f"[{i + 1}/{len(product_links)}] Processing: {link}")
                    data = self.scrape_product_details(link)
                    if data:
                        self.save_to_csv(data)  # লোকাল CSV ব্যাকআপ
                        self.upload_to_gsheet(data)  # গুগল শিটে লাইভ পুশ
                    time.sleep(1)
                    
            self.update_live_status("SYNC COMPLETED SUCCESSFULLY")
            print("\n--- SYNC COMPLETED SUCCESSFULLY ---")
        except Exception as e:
            self.update_live_status(f"System Error: {str(e)[:20]}")
            print(f"Critical System Error: {e}")


if __name__ == "__main__":
    scraper = PazaruvajMasterScraper()
    scraper.run()
