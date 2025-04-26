# main.py

import os
from pathlib import Path
from shutil import rmtree
import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def press_button(driver, wait, xpath):
    button = wait.until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )  # Wait until button is clickable.
    ActionChains(driver).move_to_element(button).click().perform()  # Click button.


URL = "https://www.sectorspdrs.com/mainfund/XLB"
# XPATH = "(//span[contains(text(), 'Download a Spreadsheet')]/following-sibling::button[contains(text(), 'CSV File')])[2]"
TAB_XPATH = "//a[contains(text(), 'Portfolio Holdings')]"
CSV_XPATH = """//*[@id="__BVID__118"]/div/div[1]/div[2]/button[1]"""
DOWNLOAD_DIR = Path("stock_weights")
download_file_directory_absolute_path = f"{os.getcwd()}\\{DOWNLOAD_DIR}"

if DOWNLOAD_DIR.exists():
    rmtree(DOWNLOAD_DIR)
DOWNLOAD_DIR.mkdir(exist_ok=True)

options = webdriver.ChromeOptions()
prefs = {"download.default_directory": download_file_directory_absolute_path}
options.add_experimental_option("prefs", prefs)
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

chromedriver_path = os.getenv("CHROMEDRIVER_PATH", "chromedriver")
service = Service(chromedriver_path)
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 10)


try:
    driver.get(URL)
    time.sleep(2)
    press_button(driver, wait, TAB_XPATH)
    press_button(driver, wait, CSV_XPATH)
    print("Button clicked. Waiting for download...")

    # Wait for download to complete
    time.sleep(5)

except TimeoutException:
    print("Timeout while trying to find or click the button.")
    with open("timeout_debug.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)

finally:
    driver.quit()
