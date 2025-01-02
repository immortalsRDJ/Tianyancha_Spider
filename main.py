import logging
from playwright.sync_api import sync_playwright
import pandas as pd
from bs4 import BeautifulSoup
import time
import os

# Configure logging
logging.basicConfig(
    filename="scraping.log", 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def scrape_company_tables(page, company_name):
    """Extracts multiple tables (default and historical shareholder info) for a specific company and appends them to a consolidated Excel file."""
    output_file = "L1_share.xlsx"
    try:
        logging.info(f"Starting to scrape data for company: {company_name}")
        search_box = page.locator('input[placeholder="请输入公司名称、老板姓名、品牌名称等"]:visible').nth(0)
        search_box.fill(company_name)
        search_box.press("Enter")  # Press Enter to search
        time.sleep(2)

        # Click on the first company link
        company_link = page.locator("a.index_alink__zcia5").first
        if not company_link:
            logging.warning(f"No results found for {company_name}. Skipping...")
            return

        company_name_element = company_link.locator("span em")
        company_name_text = company_name_element.text_content().strip()

        with page.expect_popup() as popup_info:
            company_link.click()
        popup_page = popup_info.value  # Get the popup page object
        popup_page.wait_for_load_state("domcontentloaded")

        logging.info(f"Popup page loaded for company: {company_name}")

        # Scrape default shareholder table
        df_shareholders = scrape_table(popup_page, company_name, company_name_text, "股东信息")
        if df_shareholders is not None:
            append_to_excel(df_shareholders, output_file, "Shareholders")

        # Try scraping the historical shareholder table
        try:
            logging.info("Attempting to scrape historical shareholder information...")
            tab_found = False
            for tab_text in ["历史股东信息", "历史主要股东"]:
                try:
                    tab_locator = popup_page.locator("span.dim-tab-item").filter(has_text=tab_text)
                    tab_locator.click()
                    popup_page.wait_for_timeout(3000)  # Wait for tab content to load
                    logging.info(f"Clicked on tab: {tab_text}")
                    tab_found = True
                    break
                except Exception:
                    logging.warning(f"Tab '{tab_text}' not found. Trying next...")

            if not tab_found:
                logging.error(f"No historical shareholder tab found for {company_name}. Skipping...")
            else:
                # Scrape the historical shareholders table
                df_historical = scrape_table(popup_page, company_name, company_name_text, tab_text)
                if df_historical is not None:
                    append_to_excel(df_historical, output_file, "Historical Shareholders")
        except Exception as e:
            logging.error(f"Error occurred while scraping historical shareholder info for {company_name}: {e}")

    except Exception as e:
        logging.error(f"Error occurred while scraping data for {company_name}: {e}")

    finally:
        try:
            popup_page.close()
        except Exception:
            pass
        page.goto("https://www.tianyancha.com/")

def scrape_table(page, original_name, matched_name, table_title):
    """Scrapes a single table and returns it as a DataFrame."""
    try:
        logging.info(f"Scraping {table_title} for {original_name}")
        
        # Wait for the table element to load completely
        table_locator = page.locator("table.table-wrap.expand-table-wrap")
        max_retries = 5
        retries = 0
        table_loaded = False

        while retries < max_retries:
            table_html = table_locator.inner_html()
            if "加载中" not in table_html:  # Check if "加载中" is no longer in the table content
                table_loaded = True
                break
            logging.info(f"Table still loading... Retry {retries + 1}")
            retries += 1
            time.sleep(2)  # Wait before retrying

        if not table_loaded:
            logging.error(f"{table_title} table did not load completely for {original_name} after retries.")
            return None

        # Parse the table with BeautifulSoup
        logging.debug(f"Table HTML for {table_title}: {table_html[:500]}")  # Log the first 500 characters for debugging
        soup = BeautifulSoup(table_html, "html.parser")
        headers = [th.get_text(strip=True) for th in soup.find("thead").find_all("th")]
        rows = []
        for tr in soup.find("tbody").find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cols) == len(headers):  # Ensure the row matches the number of headers
                rows.append(cols)
            else:
                logging.warning(f"Incomplete row skipped: {cols}")

        # Return DataFrame
        df = pd.DataFrame(rows, columns=headers)
        df.insert(0, "Original Company Name", original_name)
        df.insert(1, "Matched Company Name", matched_name)
        return df
    except Exception as e:
        logging.error(f"Error occurred while scraping {table_title}: {e}")
        return None
    

def append_to_excel(df, output_file, sheet_name):
    """Appends a DataFrame to an Excel file in the specified sheet."""
    try:
        if not os.path.exists(output_file):
            # Create a new file if it doesn't exist
            with pd.ExcelWriter(output_file, mode="w") as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            # Append to the existing file
            existing_df = None
            try:
                existing_df = pd.read_excel(output_file, sheet_name=sheet_name)
            except Exception:
                logging.warning(f"Sheet {sheet_name} does not exist. Creating a new one.")

            if existing_df is not None:
                df = pd.concat([existing_df, df], ignore_index=True)

            with pd.ExcelWriter(output_file, mode="a", if_sheet_exists="replace") as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        logging.info(f"Data appended to {output_file} in sheet {sheet_name}")
    except Exception as e:
        logging.error(f"Error appending to Excel: {e}")

def retry_open_browser(playwright):
    """Attempts to open the browser and retry login if needed."""
    for attempt in range(3):
        try:
            browser = playwright.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            logging.info(f"Browser launched successfully on attempt {attempt + 1}")
            return browser, page
        except Exception as e:
            logging.warning(f"Browser launch failed on attempt {attempt + 1}: {e}")
    logging.error("Failed to launch browser after multiple attempts.")
    return None, None

def run(playwright):
    try:
        browser, page = retry_open_browser(playwright)
        if not browser or not page:
            logging.critical("Failed to launch browser. Exiting...")
            return

        page.goto("https://www.tianyancha.com/")
        page.get_by_text("登录/注册").first.click()
        page.locator(".login-toggle").click()

        # Enter login details
        page.get_by_placeholder("请输入中国大陆手机号").fill("131 7625 8693")
        page.get_by_text("密码登录").click()
        page.get_by_placeholder("请输入登录密码").fill("gh757603")
        page.get_by_label("我已阅读并同意《用户协议》《隐私权政策》").check()
        page.get_by_role("button", name="登录").click()

        # Wait for manual CAPTCHA solving
        logging.info("Waiting for CAPTCHA to be solved manually...")
        input("Paused! Solve the CAPTCHA manually and press Enter to continue...")

        # Load company names
        company_list = pd.read_excel("test.xlsx", header=None)[0].tolist()

        # Scrape data for each company
        for company_name in company_list:
            scrape_company_tables(page, company_name)

    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
    finally:
        try:
            browser.close()
        except Exception:
            pass

if __name__ == "__main__":
    with sync_playwright() as playwright:
        run(playwright)