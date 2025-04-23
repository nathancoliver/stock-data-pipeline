from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


def get_xpath(driver, element):
    return driver.execute_script(
        """
    function absoluteXPath(element) {
        var comp, comps = [];
        var parent = null;
        var xpath = '';
        var getPos = function(element) {
            var position = 1, curNode;
            if (element.nodeType == Node.ATTRIBUTE_NODE) {
                return null;
            }
            for (curNode = element.previousSibling; curNode; curNode = curNode.previousSibling){
                if (curNode.nodeName == element.nodeName){
                    ++position;
                }
            }
            return position;
        }

        if (element instanceof Document) {
            return '/';
        }

        for (; element && !(element instanceof Document); element = element.nodeType == Node.ATTRIBUTE_NODE ? element.ownerElement : element.parentNode) {
            comp = comps[comps.length] = {};
            switch (element.nodeType) {
                case Node.TEXT_NODE:
                    comp.name = 'text()';
                    break;
                case Node.ATTRIBUTE_NODE:
                    comp.name = '@' + element.nodeName;
                    break;
                case Node.PROCESSING_INSTRUCTION_NODE:
                    comp.name = 'processing-instruction()';
                    break;
                case Node.COMMENT_NODE:
                    comp.name = 'comment()';
                    break;
                case Node.ELEMENT_NODE:
                    comp.name = element.nodeName;
                    break;
            }
            comp.position = getPos(element);
        }

        for (var i = comps.length - 1; i >= 0; i--) {
            comp = comps[i];
            xpath += '/' + comp.name.toLowerCase();
            if (comp.position !== null && comp.position > 1) {
                xpath += '[' + comp.position + ']';
            }
        }

        return xpath;
    }
    return absoluteXPath(arguments[0]);
    """,
        element,
    )


def extract_elements(driver):
    elements = driver.find_elements(By.XPATH, "//*")
    results = []
    for el in elements:
        try:
            label = (
                el.get_attribute("aria-label")
                or el.get_attribute("alt")
                or el.get_attribute("placeholder")
                or el.text
            )
            xpath = get_xpath(driver, el)
            results.append((xpath, label.strip() if label else ""))
        except:
            continue
    return results


# Setup Selenium
options = Options()
options.add_argument("--headless=new")
driver = webdriver.Chrome(options=options)

# Load your URL
url = "https://www.sectorspdrs.com/mainfund/XLC"  # Replace with your target URL
driver.get(url)
time.sleep(2)  # wait for page to load

elements_data = extract_elements(driver)
driver.quit()

# Save to file
output_file = "elements_output.txt"
with open(output_file, "w", encoding="utf-8") as f:
    for xpath, label in elements_data:
        f.write(f"XPath: {xpath}\nLabel: {label}\n{'-'*40}\n")

print(f"\n✅ Done! Results saved to: {output_file}")
