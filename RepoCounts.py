from CombineParquet import combineParquetFiles, multipleLocationCombination
from GetResponseCode import get_response_code
from ToParquetFile import writeToParquet
from Preferences import setPreferences
from Driver import setDriver
from string import Template
import pandas as pd


#Establish website path and path to chromedriver...
web = "https://gitstar-ranking.com/organizations"
path = "C:/Users/rfell/WebScraping/chromedriver.exe"
chrome_options = setPreferences()


def numReposPrep(index):
    df = pd.read_parquet('OrgParquetFolder/CombinedOrgs/MergedOrgs.parquet')
    organizations = df['Organizations'].to_numpy()
    numberOfRepositories(organizations, index)


def numberOfRepositories(organizations, index):
    #After getting the organizations, run through the organizations and gather the number of repositories per org.
    url = Template("https://gitstar-ranking.com/$organization")
    reposList = []
    for org in organizations:
        newURL = url.substitute(organization=org)
        driver = setDriver(path, chrome_options, newURL)
        
        #Get the response code from scraped page...
        logs = driver.get_log("performance")
        response_code = get_response_code(logs, newURL)
        
        #If accepted, add the amount of repositories to the list 'reposList'.
        if (response_code == 200) or (response_code == 301):
            #Get the amount of repos per organization, separating the number
            #from the word "Repositories", return the int to list.
            repoAmount = driver.find_element(by='xpath', value='//h3').text
            repoAmount = repoAmount.split()
            repoAmount = repoAmount[0]
            reposList.append(int(repoAmount))
            repoAmount = ''
        
        #Otheriwse, append 'Error' to indicate there was an issue.
        else:
            reposList.append("Error")

    driver.quit();
    
    #Now we send the number of repositories to the correct parquet file.
    getParquetFiles(reposList, index)


def getParquetFiles(reposList, index):
    #Now that we know the number of repositories per organization, we list corresponding number of repos in each file with the same
    #index as their respective organization is indexed at (aka if orgs_0 contains 'Microsoft', then repo_amount_0 contains 6300).
    writeToParquet(len(reposList), index, reposList, 'Repo Amounts', 'RepoAmountParquetFolder/repo_amount_$index.parquet')
    combineParquetFiles('RepoAmountParquetFolder/*.parquet', 'RepoAmountParquetFolder/CombinedRepoCount/MergedRepoCount.parquet')
    multipleLocationCombination('RepoAmountParquetFolder/CombinedRepoCount/MergedRepoCount.parquet', 'OrgParquetFolder/CombinedOrgs/MergedOrgs.parquet', 'RepoAmountParquetFolder/CombinedRepoCountAndOrgs/MergedRepoCount.parquet')

#Used for testing the file...
#numReposPrep(2, 350)