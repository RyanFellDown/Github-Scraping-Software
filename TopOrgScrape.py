from CombineParquet import combineParquetFiles
from TotalPages import getPages, getFirstPage
from GetResponseCode import get_response_code
from ToParquetFile import writeToParquet
from Preferences import setPreferences
from Driver import setDriver
import re, math


#Establish website path and path to chromedriver...
web = "https://gitstar-ranking.com/organizations"
path = "C:/Users/rfell/WebScraping/chromedriver.exe"
chrome_options = setPreferences()


#Organizations takes the given URL and iterates through the correct number of
#pages and organizations to return the desired organizations.
def scrapeOrganizations(totalOrgs, index):   
    #Service takes driver path, driver connects to driver.exe, then .get to open website
    driver = setDriver(path, chrome_options, web)
    organizations = []

    #Get the response code from scraped page...
    logs = driver.get_log("performance")
    response_code = get_response_code(logs, web)
    
    #If the website accepted our request, aka the response code is '200' or '301', then we scrape the necessary webpages.
    if ((response_code == 200) or (response_code == 301)) and index <= 100:
        
        #The driver takes in the URL and returns all elements with the requested HTML part.
        orgNames = driver.find_elements(by='xpath', value='//span[@class="name"]')

        #We iterate through these HTML parts and return all the organization names into a list called 'organizations'.
        for orgs in orgNames:
            organizations.append(orgs.find_element(by='xpath', value='.//span[@class="hidden-xs hidden-sm"]').text)


    #The number of organizations listed on each page is 100 maximum. So, if we request more than 100 organizations, 
    #we need to iterate through multiple pages.
    if (totalOrgs+index) > 100:
        #Append '?page=' to use the correct URL.
        newWeb = (web + "?page=")
        
        #If The index is less than 100, the first scraped page would be 2, otherwise, get the first page by dividing index by 100 and adding 1 to the floor.
        if(index <= 100):
            firstPage = 2
        else:
            firstPage = getFirstPage(index, 100)
        maxPages = getPages(index+totalOrgs, 100, 1)
        
        #Iterate through the pages of the website we scrape the organizations from.
        for x in range(firstPage, maxPages):
            #Get new URL, initiate driver, and check for response code.
            newWeb = (newWeb+str(x))
            driver = setDriver(path, chrome_options, newWeb)
                
            #Get the response code from scraped page...
            logs = driver.get_log("performance")
            response_code = get_response_code(logs, newWeb)
                
            #If accepted, scrape the organizations and add to the list 'organizations'.
            if (response_code == 200) or (response_code == 301):                
                orgNames = driver.find_elements(by='xpath', value='//span[@class="name"]')
                for orgs in orgNames:
                    organizations.append(orgs.find_element(by='xpath', value='.//span[@class="hidden-xs hidden-sm"]').text)
                newWeb = re.sub(str(x), '', newWeb)
    
    correctOrgs(totalOrgs, organizations, index)
    
    
#This function makes sure we're only returning the organizations we wanted, from the correct index.
def correctOrgs(totalOrgs, organizations, index):
    organizations2 = []
    
    #If index is less than 100, just add organizations from where the index starts from the list, otherwise we need to remove the hundreds place from index
    #as our list starts from index 0 (ie if we scrape 20 organizations at index 350, our list takes all 100 organizations from page 4 (organizations 300-400), 
    # and we need organizations starting at index 50 in our list).
    if(index <= 100):
        for x in range(index, index+totalOrgs):
            organizations2.append(organizations[x])
    else:
        indexTemp = float(index/100)
        indexTemp = math.floor(indexTemp)
        indexTemp = indexTemp * 100
        indexTemp = index-indexTemp
        for x in range(indexTemp, indexTemp+totalOrgs):
            organizations2.append(organizations[x])
    
    getOrgsIntoParquet(totalOrgs, index, organizations2)
    
    
def getOrgsIntoParquet(totalOrgs, index, organizations2):
    #We finish by adding each organization to its own parquet file, done by creating a data frame and them 
    #adding that to a parquet file with the current index (aka orgs_0, orgs_1, etc...).
    writeToParquet(totalOrgs, index, organizations2, 'Organizations', 'OrgParquetFolder/orgs_$index.parquet')
    combineParquetFiles('OrgParquetFolder/*.parquet', 'OrgParquetFolder/CombinedOrgs/MergedOrgs.parquet')
    
    
#Used for testing the file...
#scrapeOrganizations(2, 350)