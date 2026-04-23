import json
import time
import re
import csv
import os
import requests as py_requests  # ওয়ার্ডপ্রেস ট্রিগারের জন্য
from datetime import datetime
from curl_cffi import requests # স্ক্র্যাপিংয়ের জন্য
from lxml import html
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import html as html_lib

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
        self.scraped_data_today = [] # আজকের সব ডাটা এখানে থাকবে

        # ২. গুগল শিট কানেকশন
        self.sheet_name = "Pazaruvaj Smartfones"
        self.setup_google_sheets()

        # ৩. লোকাল CSV ফাইল ইনিশিয়ালাইজেশন
        self.init_csv()

    def setup_google_sheets(self):
        """গুগল শিট কানেক্ট করবে"""
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
            
            # সরাসরি মাস্টার শিট এবং লগ শিট কানেক্ট করা
            self.master_worksheet = self.spreadsheet.worksheet("Master_Sheet")
            self.log_worksheet = self.spreadsheet.worksheet("Process_Log")
            
            print(f"Connected to {self.sheet_name}. Direct Master_Sheet Access Enabled.")
        except Exception as e:
            print(f"Google Sheets Setup Error: {e}")
            self.master_worksheet = None

    def update_live_status(self, message):
        """ড্যাশবোর্ডে লাইভ দেখানোর জন্য শিটে স্ট্যাটাস আপডেট করা"""
        try:
            self.log_worksheet.update_acell('H1', f"LIVE: {message} | {datetime.now().strftime('%H:%M:%S')}")
        except: pass

    def get_system_status(self):
        """ড্যাশবোর্ডের J1 সেল থেকে ON/OFF স্ট্যাটাস চেক করা"""
        try:
            status = self.log_worksheet.acell('J1').value
            return status.upper() if status else "ON"
        except: return "ON"

    def init_csv(self):
        with open(self.filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()

    def save_to_csv(self, data_list):
        with open(self.filename, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerows(data_list)

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
            self.update_live_status(f"Page {page}: Found {page_found} items.")

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
        if not raw_html or raw_html == "None": return ""
        text = html_lib.unescape(raw_html)
        text = re.sub(r'<(br|p|div|li|tr|h1|h2|h3)[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</(p|div|li|tr|h1|h2|h3)>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)
    
    def scrape_product_details(self, url, is_sub_variant=False, parent_id_val=None):
        res = self.get_response(url)
        if not res or not res.text: return None

        detail = self.extract_json_data(res.text)
        if detail is None: return None

        product = detail.get('product')
        if not product: return None

        raw_id = product.get('localId') or product.get('id')
        if not raw_id: return None
        current_p_id = f"p{raw_id}"

        if current_p_id in self.visited_ids: return None
        self.visited_ids.add(current_p_id)

        brand = product.get('producers', [{}])[0].get('name', 'N/A') if product.get('producers') else "N/A"
        category_list = detail.get('category', {}).get('breadcrumbs', [])
        cat_path = " > ".join([b.get('name', '') for b in category_list]) if category_list else "N/A"
        
        clean_desc = self.clean_html(product.get('description') or "")

        attributes_obj = product.get('attributes', {})
        attrs = attributes_obj.get('attributes', []) if attributes_obj else []
        specs_list = [f"{self.clean_html(str(a.get('name', 'Unknown')))}: {self.clean_html(str(a.get('value', 'N/A')))}" for a in attrs if a.get('name')]
        specs = "\n".join(specs_list)

        ean = next((a.get('value') for a in attrs if 'ean' in str(a.get('name', '')).lower()), "N/A")
        mpn = next((a.get('value') for a in attrs if 'mpn' in str(a.get('name', '')).lower()), "N/A")

        media = product.get('media', {})
        img_list = [img.get('url') for img in media.get('images', [])] if media else []
        if not img_list and product.get('mainImage'): 
            img_list = [product['mainImage'].get('url')]
        images_str = ",".join(filter(None, img_list))

        price = detail.get('product', {}).get('minPrice', '0.00')

        seller = "N/A"
        offers_data = detail.get('offers', {})
        if offers_data:
            all_offers = (offers_data.get('regular', []) + offers_data.get('bidding', []))
            if all_offers:
                try:
                    seller = sorted(all_offers, key=lambda x: x.get('price', 999999))[0].get('shop', {}).get('name', 'N/A')
                except: pass

        storage = "Standard"
        variants_list = detail.get('variants', []) or []
        if variants_list:
            curr_v = next((v for v in variants_list if str(v.get('platformProductId')) == str(raw_id)), None)
            if curr_v: storage = curr_v.get('value')

        if storage == "Standard":
            mem_match = re.search(r'(\d+\s*(?:GB|TB))', product.get('name', ''), re.IGNORECASE)
            if mem_match: storage = mem_match.group(1)

        current_row = {
            "Product_URL": url, 
            "Product_ID": current_p_id,
            "Parent_ID": parent_id_val if parent_id_val else current_p_id,
            "Title": product.get('name', 'N/A'), 
            "Storage_Variation": storage, 
            "Category": cat_path, 
            "Brand": brand,
            "Price_EUR": price, 
            "Seller_Name": seller, 
            "EAN": ean, 
            "MPN": mpn, 
            "Images": images_str,
            "Specs": specs, 
            "Description": clean_desc, 
            "Stock_Status": "instock", # ডিফল্ট instock
            "Last_Updated": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        }

        results = [current_row]

        if not is_sub_variant and variants_list:
            for v in variants_list:
                v_slug = v.get('slug', {}).get('value', 'v')
                v_url = f"{self.base_url}/p/{v_slug}-p{v.get('platformProductId')}/"
                time.sleep(1)
                v_data = self.scrape_product_details(v_url, is_sub_variant=True, parent_id_val=current_p_id)
                if v_data: results.extend(v_data)

        return results

    def trigger_wordpress_import(self):
        """ওয়ার্ডপ্রেস ইম্পোর্ট শুরু করার জন্য ট্রিগার এবং প্রসেসিং লিঙ্ক হিট করবে"""
        t_url = "https://woocommerce-1599974-6345499.cloudwaysapps.com/wp-load.php?import_key=HacSr4&import_id=4&action=trigger"
        p_url = "https://woocommerce-1599974-6345499.cloudwaysapps.com/wp-load.php?import_key=HacSr4&import_id=4&action=processing"
        
        print("\n--- Triggering WordPress Import ---")
        try:
            py_requests.get(t_url, timeout=30)
            time.sleep(10)
            for i in range(7):
                py_requests.get(p_url, timeout=60)
                print(f"Kickstart Ping {i+1} sent.")
                time.sleep(20)
        except Exception as e:
            print(f"Trigger Error: {e}")

    def run(self):
        # ১. মাস্টার সুইচ চেক
        if self.get_system_status() == "OFF":
            print("System is DISABLED from Dashboard. Exiting...")
            return

        self.update_live_status("Starting Cloud Scraper...")
        
        # ২. মাস্টার শিট থেকে বর্তমান ডাটা মেমোরিতে নেওয়া
        try:
            existing_records = self.master_worksheet.get_all_records()
            master_map = {str(r['Product_ID']): r for r in existing_records if r.get('Product_ID')}
        except: master_map = {}

        # ৩. স্ক্র্যাপিং শুরু
        cat_ws = self.spreadsheet.worksheet("Categories")
        valid_urls = [u.strip() for u in cat_ws.col_values(1)[1:] if u and u.startswith('http')]
        
        scraped_ids_today = set()
        all_results = []
        new_count = 0
        update_count = 0

        for cat_url in valid_urls:
            product_links = self.get_product_links(cat_url)

            for i, link in enumerate(product_links):
                self.update_live_status(f"Scraping Product {i+1}/{len(product_links)}")
                data_list = self.scrape_product_details(link)
                if data_list:
                    for item in data_list:
                        p_id = str(item['Product_ID'])
                        scraped_ids_today.add(p_id)
                        
                        # নতুন নাকি আপডেট চেক করা
                        if p_id in master_map:
                            update_count += 1
                        else:
                            new_count += 1
                            
                        all_results.append(item)
                        self.save_to_csv([item])
                time.sleep(1)

        # ৪. স্টক আপডেট লজিক
        final_rows_for_sheet = []
        today_str = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
        oos_count = 0

        # ইন-স্টক পণ্য
        for item in all_results:
            item['Stock_Status'] = 'instock'
            item['Last_Updated'] = today_str
            final_rows_for_sheet.append(item)

        # আউট-অফ-স্টক পণ্য (আগে ছিল কিন্তু আজ পাওয়া যায়নি)
        for p_id, old_row in master_map.items():
            if p_id not in scraped_ids_today:
                old_row['Stock_Status'] = 'outofstock'
                old_row['Last_Updated'] = today_str
                final_rows_for_sheet.append(old_row)
                oos_count += 1

        # ৫. মাস্টার শিট আপডেট
        self.update_live_status("Finalizing Master Sheet...")
        upload_data = [[r.get(h, "") for h in self.headers] for r in final_rows_for_sheet]
        
        self.master_worksheet.clear()
        self.master_worksheet.append_row(self.headers)
        if upload_data:
            self.master_worksheet.append_rows(upload_data)

        # --- ৬. প্রসেস লগ (History) আপডেট করা ---
        # কলামগুলো: Date & Time, Total Scraped, New, Updated, Marked OOS, Status
        try:
            log_row = [
                today_str,           # Date & Time
                len(all_results),    # Total Scraped
                new_count,           # New Products
                update_count,        # Updated Products
                oos_count,           # Marked Out of Stock
                "Success"            # Status
            ]
            # Process_Log শিটের একদম নিচে নতুন রো হিসেবে যোগ হবে
            self.log_worksheet.append_row(log_row)
            print("History Log updated in Process_Log.")
        except Exception as e:
            print(f"Logging Error: {e}")

        # ৭. ওয়ার্ডপ্রেস ইম্পোর্ট শুরু করা
        self.trigger_wordpress_import()
        self.update_live_status("SYNC COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    scraper = PazaruvajMasterScraper()
    scraper.run()
