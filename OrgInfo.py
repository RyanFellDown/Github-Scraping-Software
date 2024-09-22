from CombineParquet import combineParquetFiles, multipleLocationCombination
from GetResponseCode import get_response_code
from HTMLElement import attemptScrapeElement
from ConvertNumbers import convertNumbers
from Preferences import setPreferences
from ToParquetFile import writeTable
from Driver import setDriver
from string import Template
import pandas as pd


#Establish website path and path to chromedriver.
web = Template("https://github.com/$organization")
path = "C:/Users/rfell/WebScraping/chromedriver.exe"

#Create global lists...
peopleTotal, projectsTotal, packagesTotal, verified, followersTotal, location, website, twitter, emails, languageList, topicsList, finalTotal = ([] for i in range(12))

#Setting up to get response code from website...
chrome_options = setPreferences()
  

#We're returning all the organization names to the list 'organizations' so the program knows what organizations it needs to scrape info for.
def orgsToScrape(index):
    df = pd.read_parquet('OrgParquetFolder/CombinedOrgs/MergedOrgs.parquet')
    organizations = df['Organizations'].to_numpy()
    scrapeOrgInfo(index, organizations)


#This function scrapes the information of each requested organization.
def scrapeOrgInfo(index, organizations):
    #Establish the strings to track the languages and topics used by each org.
    languageString = ''
    topicString = ''
    
    #Scraping websites for repositories (people, sponsors, languages later).
    for x in organizations:
        #Each organization replaces a portion of the URL in order to scrape each desired organizations' information, hence 'organization=x'.
        newWeb = web.substitute(organization=x)
        print(newWeb)
        driver = setDriver(path, chrome_options, newWeb)
    
        #Get the response code from org page...
        logs = driver.get_log("performance")
        response_code = get_response_code(logs, newWeb)

        #If the response code is accepted, scrape the page.
        if (response_code == 200) or (response_code == 301):
            #Scrape the correct information of the HTML of Github. This includes: People, projects, verification, 
            #followers, location, website URL, twitter, email, topics, and languages.
            peopleTotal.append(attemptScrapeElement(driver, '//span[@class="Counter js-profile-member-count"]', 'Total people'))
            projectsTotal.append(attemptScrapeElement(driver, '//span[@class="Counter js-profile-project-count"]', 'Total projects'))
            isVerified = attemptScrapeElement(driver, '//summary[@class="Label Label--success"]', 'Verifitcation')
            
            if isVerified != "Not listed...":
                verified.append("True")
            else:
                verified.append("False")
                
            followersTotal.append(attemptScrapeElement(driver, '//span[@class="text-bold color-fg-default"]', 'Total followers'))
            location.append(attemptScrapeElement(driver, '//span[@itemprop="location"]', 'Location'))
            website.append(attemptScrapeElement(driver, '//a[@class="color-fg-default"]', 'Website'))
            twitter.append(attemptScrapeElement(driver, '//a[@class="Link--primary"]', 'Twitter'))
            emails.append(attemptScrapeElement(driver, '//li[@class="mr-md-3 color-fg-default my-2 my-md-0 css-truncate css-truncate-target"]', 'Email'))
            
            topics = attemptScrapeElement(driver, '//div[@class="d-inline-flex flex-wrap"]', 'Topics')
            for top in topics:
                if(type(top) != type('String')):
                    topicString = topicString + "" + top.text
                else:
                    topicString = topicString + "" + top
            topicsList.append(topicString)
            topicString = ''
            
            languages = attemptScrapeElement(driver, '//a[@class="no-wrap color-fg-muted d-inline-block Link--muted mt-2"]', 'Languages')
            for langs in languages:
                if(type(langs) != type('String')):
                    languageString = languageString + "" + langs.text
                else:
                    languageString = languageString + "" + langs
            languageList.append(languageString)
            languageString = ''
                
    #This function converts numbers with K's to their respective whole number (aka 100K -> 100,000).
    finalTotal = convertNumbers(peopleTotal)
    
    #Send organization information to parquet files.
    orgInfoToParquet(finalTotal, index)


def orgInfoToParquet(finalTotal, index):
    #This gets all the repositories listed in the repository array and puts them into separate parquet folders, 
    #indexed 0--> # of organizations. The indexes of the files should line up with the org they're from.
    for x in range(0, len(finalTotal)):
        orgDF = pd.DataFrame({'Number of People': [finalTotal[x]], 'Number of Projects': [projectsTotal[x]], 'Verified': [verified[x]], 'Number of Followers': [followersTotal[x]], 'Location': [location[x]], 'Website': [website[x]], 'Twitter (X)': [twitter[x]], 'Email': [emails[x]], 'Top Languages': [languageList[x]], 'Top Topics': [topicsList[x]]})
        writeTable(orgDF, index, 'OrgInformationFolder/org_info_$index.parquet', x)
    
    combineParquetFiles('OrgInformationFolder/*.parquet', 'OrgInformationFolder/CombinedOrgInfo/MergedOrgInfo.parquet')
    multipleLocationCombination('OrgInformationFolder/CombinedOrgInfo/MergedOrgInfo.parquet', 'RepoAmountParquetFolder/CombinedRepoCountAndOrgs/MergedRepoCount.parquet', 'FinalizedParquets/Organization/FinalOrganizationInformation.parquet')

        
#Used for testing the file...
#orgsToScrape(2, 350)