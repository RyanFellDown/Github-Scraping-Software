from selenium.webdriver.chrome.service import Service
from selenium import webdriver


def setDriver(path, chrome_options, web):
    service = Service(executable_path=path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(web)
    driver.set_page_load_timeout(10)
    return driver