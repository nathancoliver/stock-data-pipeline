import os
import time
from pathlib import Path
from shutil import rmtree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class ChromeDriver:

    def __init__(self, download_file_directory: str | Path):
        self.download_file_directory_str = download_file_directory
        self.download_file_directory_path = Path(download_file_directory)
        self.download_file_directory_absolute_path = (
            f"{os.getcwd()}\\{download_file_directory}"
        )

        # Update ChromeDriver preferences to download files to self.download_file_directory
        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": self.download_file_directory_absolute_path
        }
        options.add_experimental_option("prefs", prefs)

        service = Service(executable_path="chromedriver.exe")
        self.driver = webdriver.Chrome(service=service, options=options)

        self.wait = WebDriverWait(self.driver, 10)

    def create_directory(self):
        if self.download_file_directory_path.exists():
            rmtree(self.download_file_directory_path)
        self.download_file_directory_path.mkdir(exist_ok=True)

    def load_url(self, url: str):
        self.driver.get(url)
        time.sleep(2)

    def press_button(self, xpath):
        button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )  # Wait until button is clickable.
        ActionChains(self.driver).move_to_element(
            button
        ).click().perform()  # Click button.

    def quit_driver(self):
        self.driver.quit()
