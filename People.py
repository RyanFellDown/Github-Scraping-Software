from CombineParquet import combineParquetFiles, multipleLocationCombination
from GetResponseCode import get_response_code
from Preferences import setPreferences
from ToParquetFile import writeTable
from TotalPages import getPages
from Driver import setDriver
from string import Template
import pandas as pd


#Establish website path and path to chromedriver...
web = Template("https://github.com/orgs/$organization/people")
path = "C:/Users/rfell/WebScraping/chromedriver.exe"

#Establish global variables...
peopleArray = []

#Setting up to get response code from website...
chrome_options = setPreferences()


#This pulls what organizations we want to find the people for AND gives us the number of people per organization
#(roughly) so we know how many pages to scrape through.
def getPeoplePrep(index):
    #This scrapes all the parquet files for the organization names.
    df = pd.read_parquet('OrgParquetFolder/CombinedOrgs/MergedOrgs.parquet')
    organizations = df['Organizations'].to_numpy()
    

    #This scrapes all the parquet files for the nuber of people per organization.       
    df = pd.read_parquet('FinalizedParquets/Organization/FinalOrganizationInformation.parquet')
    numberPpl = df['Number of People'].to_numpy()
    
    peopleNames(organizations, numberPpl, index)


#This function returns the names of all the people in each organization
def peopleNames(organizations, numberPpl, index):
    peopleString = ''
    counter = 0

    #This for loop iterates through each organization, x.
    for x in organizations:
        if(numberPpl[counter] != 'Not listed...'):
            #Replace the portion of the template "organization" with the actual organization we'd like to scrape.
            newWeb = web.substitute(organization=x)
            driver = setDriver(path, chrome_options, newWeb)
        
            #Get the response code from org page...
            logs = driver.get_log("performance")
            response_code = get_response_code(logs, newWeb)


            #If the response code comes back good, we continue to scrape the first page.
            if (response_code == 200) or (response_code == 301):            
                #Scrape the correct parts of the HTML of Github.
                peopleNames = driver.find_elements(by='xpath', value='//a[@class="f4 d-block"]')
                for names in peopleNames:
                    urlName = names.get_attribute('href')
                    peopleString = peopleString + urlName + ', '

                #We prep "peopleIndex", equal to the total number of pages we need to scrape based
                #on the number of total people divided by 30 (as there's 30 people per page).
                peopleIndex = int(numberPpl[counter])
                peopleIndex = getPages(peopleIndex, 30, 0)
                peopleNames.clear()
                nextPageURL = newWeb + "?page="
                
                #We iterate through the rest of the pages, scraping people names. We add 5 to peopleIndex since the original 
                #source I got the number of people from is slightly outdated, so we scrape more just to be safe.
                for y in range(2, peopleIndex+5):
                    nextPageURLTemp = nextPageURL + str(y)
                    driver = setDriver(path, chrome_options, nextPageURLTemp)
        
                    #Get the response code from org page...
                    logs = driver.get_log("performance")
                    response_code = get_response_code(logs, nextPageURLTemp)
                        
                    #If accepted, scrape the page.
                    if (response_code == 200) or (response_code == 301):
                        #Scrape the correct parts of the HTML of Github.
                        peopleNames = driver.find_elements(by='xpath', value='//span[@class="color-fg-default"]')
                        for names in peopleNames:
                            peopleString = peopleString + names.text + ", "
                        peopleNames.clear()
                
                #To list all the people in one cell of parquet file, we add all names to one string.
                peopleString = peopleString[:len(peopleString)-2]
                peopleArray.append(peopleString)
                peopleString = ''
                counter = counter+1
                peopleIndex = 0
        else:
            peopleArray.append('Not listed...')
            counter = counter+1
            peopleString = ''
            peopleIndex = 0

    peopleToParquet(index)
        

def peopleToParquet(index):
    #This gets all the people listed in the people array and puts them into separate parquet folders, marked 0--># of orgs.
    #The indexes of the files should line up with the org they're from.
    for x in range(0, len(peopleArray)):
        peopleDF = pd.DataFrame({'People Names': [peopleArray[x]]})
        writeTable(peopleDF, index, 'PeopleNamesFolder/people_names_$index.parquet', x)
        
    combineParquetFiles('PeopleNamesFolder/*.parquet', 'PeopleNamesFolder/CombinedNames/MergedNames.parquet')
    multipleLocationCombination('PeopleNamesFolder/CombinedNames/MergedNames.parquet', 'FinalizedParquets/Organization/FinalOrganizationInformation.parquet', 'FinalizedParquets/Organization/FinalOrganizationInformationWithNames.parquet')


#Used for testing the file...
#getPeoplePrep(2, 350)