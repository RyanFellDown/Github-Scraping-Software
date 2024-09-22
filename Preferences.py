from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver


def setPreferences():
    #Setting up to get response code from website...
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--headless")

    #Setting up more capabilities for getting the response code...
    caps = DesiredCapabilities.CHROME.copy()
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    for key, value in caps.items():
        chrome_options.set_capability(key, value)
    return chrome_options