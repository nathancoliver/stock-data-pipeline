import os
import time
from pathlib import Path
from shutil import rmtree
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


from .functions import create_directory


class ChromeDriver:

    def __init__(self, download_file_directory: str | Path):
        self.download_file_directory_str = download_file_directory
        self.download_file_directory_path = Path(download_file_directory)
        self.download_file_directory_absolute_path = (
            f"{os.getcwd()}/{download_file_directory}"
        )
        # create_directory(Path(self.download_file_directory_absolute_path))

        # Update ChromeDriver preferences to download files to self.download_file_directory
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--start-maximized")
        prefs = {
            "download.default_directory": self.download_file_directory_absolute_path
        }
        options.add_experimental_option("prefs", prefs)
        chromedriver_path = os.getenv("CHROMEDRIVER_PATH", "chromedriver")
        service = Service(executable_path=chromedriver_path)
        self.driver = webdriver.Chrome(service=service, options=options)

        self.wait = WebDriverWait(self.driver, 10)

    def load_url(self, url: str):
        self.driver.get(url)
        time.sleep(2)

    def press_button(self, cell_type: str, path: str, element_index: int):
        elements = self.driver.find_elements(cell_type, path)
        target_button = elements[element_index]
        button = self.wait.until(
            EC.element_to_be_clickable(target_button)
        )  # Wait until button is clickable.
        ActionChains(self.driver).move_to_element(
            button
        ).click().perform()  # Click button.

    def quit_driver(self):
        self.driver.quit()

    def scroll_window(self, amount):
        self.driver.execute_script(f"window.scroll(0, {amount})")
