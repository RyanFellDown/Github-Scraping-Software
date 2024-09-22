from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from CombineParquet import specialCaseMerge
from GetResponseCode import get_response_code
from HTMLElement import attemptScrapeElement
from selenium.webdriver.common.by import By
from ToParquetFile import writeTable
from Preferences import setPreferences
from TotalPages import getPages
from Driver import setDriver
from string import Template
import pyarrow.parquet as pq
import urllib.request
import pyarrow as pa
import pandas as pd
import json


#Create global lists...
repos, exactRepoAmounts, forksTotal, starsTotal, issuesTotal, pulls, projects, hasWiki, security, branches, tags, commits, softwares, watching, releases, recentRelease, contributors, languages, repositories = ([] for i in range(19))

#Setting up to get response code from website...
chrome_options = setPreferences()

#Establish website path and path to chromedriver...
web = Template("https://github.com/orgs/$organization/repositories?type=all&q=sort%3Astars")
jsonURL = Template("https://api.github.com/users/$organization/repos?")
path = "C:/Users/rfell/WebScraping/chromedriver.exe"


#Reads in parquet so we scrape the correct organization repositories and the correct amount.
def getReposPrep(index):
    #This scrapes all the parquet files for the organization names.
    df = pd.read_parquet('OrgParquetFolder/CombinedOrgs/MergedOrgs.parquet')
    organizations = df['Organizations'].to_numpy()
    
    #This scrapes all the parquet files for the nuber of repositories per organization.       
    df = pd.read_parquet('RepoAmountParquetFolder/CombinedRepoCount/MergedRepoCount.parquet')
    repoAmounts = df['Repo Amounts'].to_numpy()
        
    getRepositories(repoAmounts, organizations, index)


#This returns the names of the repositories for each organization.
def getRepositories(repoAmounts, organizations, index):
    #repositories = oldRepos, counter = starting index of repo list
    repoString = ''
    counter = 0
    
    #This was just to test if the code works, it's a small organization with only 7 repositories.
    #organizations = ['ohmyzsh']
    #repoAmounts = [7]
    
    #We iterate through the organizations, and for the number of repositories they have, we scrape each of those repositories.
    for x in organizations:
        #Getting the org page first...
        newWeb = web.substitute(organization=x)
        driver = setDriver(path, chrome_options, newWeb)
    
        #Get the response code from org page...
        logs = driver.get_log("performance")
        response_code = get_response_code(logs, newWeb)

        #If accepted, scrape the desired page.
        if (response_code == 200) or (response_code == 301): 
            #Scrape the correct parts of the HTML of Github.
            repoNames = driver.find_elements(by='xpath', value='//a[@class="Link__StyledLink-sc-14289xe-0 dIlPa TitleHeader-module__inline--rL27T Title-module__anchor--SyQM6"]')
            for repos in repoNames:
                repoString = repoString + " " + repos.find_element(by='xpath', value='.//span[@class="Text__StyledText-sc-17v1xeu-0 hWqAbU"]').text

            #Getting the rest of the pages, adding 2 in case we miss some pages.
            repoIndex = int(repoAmounts[counter])
            repoIndex = getPages(repoIndex, 30, 2)

            #Set up the URL so we can start appending page numbers...                
            nextPageURL = newWeb + "&page="
            
            #Now, we scrape the rest of the pages.
            for y in range(2, repoIndex):
                #Getting the next page...
                nextPageURLTemp = nextPageURL + str(y)
                driver = setDriver(path, chrome_options, nextPageURLTemp)
    
                #Get the response code from next page...
                logs = driver.get_log("performance")
                response_code = get_response_code(logs, nextPageURLTemp)
                
                #If accepted, scrape the next page.
                if (response_code == 200) or (response_code == 301):
                    #Scrape the correct parts of the HTML of Github.
                    repoNames = driver.find_elements(by='xpath', value='//a[@class="Link__StyledLink-sc-14289xe-0 dheQRw TitleHeader-module__inline--rL27T Title-module__anchor--SyQM6"]')
                    for repos in repoNames:
                        repoString = repoString + " " + repos.find_element(by='xpath', value='.//span[@class="Text-sc-17v1xeu-0 gPDEWA"]').text
            
            #Add all repositories to the repositories array, then clear string. They're indexed
            #By which organization they pertain to, at the same index of that organization.
            repositories.append(repoString)
            repoString = ''
            counter = counter+1
            repoIndex = 0
            
    

    repositoryInfo(organizations, index)
    

#This function returns all necessary information pertaining to each individual repository.
def repositoryInfo(organizations, index):
    repoTemplate = Template("https://github.com/$organization/$repo")
    count = 0
    numberRepos = 0
        
    #Testing...
    #organizations = ['microsoft']
    #repositories = ['vscode']
    
    #At each index is a list of all the repositories for an organization, and we need to
    #split it up so that each repository is its own index in a list.
    for repo in repositories:
        #Here we split string of repos into a list of multiple repos.
        split = repo.split()
        for word in split:
            if(word):
                repos.append(word)
                numberRepos = numberRepos+1
        exactRepoAmounts.append(numberRepos)
        numberRepos = 0

        #Then, for all the repositories from an organization, we scrape its information.
        for rps in repos:
            softwareString = ''
            
            #Getting the org page first...
            newTemplate = repoTemplate.substitute(repo=rps, organization=organizations[count])
            driver = setDriver(path, chrome_options, newTemplate)
            
            #Get the response code from org page...
            logs = driver.get_log("performance")
            response_code = get_response_code(logs, newTemplate)

            #If accepted, scrape information off of repo page.
            if (response_code == 200) or (response_code == 301):         
                #Here we scrape: forks, stars, issues, pulls, projects, wiki, security, branches, commits,
                #softwares, watchers, releases, recent releases, and contributors from the repo page.
                forksTotal.append(WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//span[@id="repo-network-counter"]'))).text)
                
                starsTotal.append(WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//span[@class="Counter js-social-count"]'))).text)
                
                issuesTotal.append(attemptScrapeElement(driver, '//span[@id="issues-repo-tab-count"]', 'Issues'))
                
                pulls.append(attemptScrapeElement(driver, '//span[@id="pull-requests-repo-tab-count"]', 'Pulls'))
                
                projects.append(attemptScrapeElement(driver, '//span[@id="projects-repo-tab-count"]', 'Projects'))
                
                if(attemptScrapeElement(driver, '//a[@id="wiki-tab"]', 'Wiki') == "Not listed..."):
                    hasWiki.append("Not listed...")
                else:
                    hasWiki.append(newTemplate + '/wiki')
                    
                sec = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//a[@id="security-tab"]')))
                security.append(attemptScrapeElement(sec, './/span[@class="Counter"]', 'Security')) 
                
                try:
                    branchTags = driver.find_elements(by='xpath', value='//a[@class="types__StyledButton-sc-ws60qy-0 eybzoG"]/span[@class="Box-sc-g0xbh4-0 kkrdEu"]/span[@data-component="text"]/div[@class="Box-sc-g0xbh4-0"]')
                    for nums in branchTags:
                        if nums.find_element(by='xpath', value='./span').text == "Branches":
                            branches.append(nums.find_element(by='xpath', value='./strong[@class="color-fg-default"]').text)
                        else:
                            tags.append(nums.find_element(by='xpath', value='./strong[@class="color-fg-default"]').text)
                except NoSuchElementException:
                    branches.append('Not listed...')
                    tags.append('Not listed...')
                    
                if(len(branches) < len(security)):
                    branches.append('Not listed...')
                if(len(tags) < len(security)):
                    tags.append('Not listed...')
                
                if(attemptScrapeElement(driver, '//span[@class="Text-sc-17v1xeu-0 gPDEWA fgColor-default"]', 'Commits special') == 'Not listed...'):
                    print('Commits NOT special listed...')
                    commitsBad1 = True
                else:
                    commits.append(attemptScrapeElement(driver, '//span[@class="Text-sc-17v1xeu-0 gPDEWA fgColor-default"]', 'Commits special'))
                if(attemptScrapeElement(driver, '//span[@class="Text-sc-17v1xeu-0 gPDEWA fgColor-default"]', 'Commits default') == 'Not listed...'):
                    print('Commits NOT default listed...')
                    commitsBad2 = True
                else:
                    commits.append(attemptScrapeElement(driver, '//span[@class="fgColor-default"]', 'Commits default'))
                if commitsBad1 and commitsBad2:
                    commits.append('Not listed...')
                
                    
                sw = driver.find_elements(by='xpath', value='//div[@class="f6"]/a[@class="topic-tag topic-tag-link"]')
                for elements in sw:
                    softwareString = softwareString + elements.text + ' '
                softwares.append(softwareString)


                watchersTemplate = Template('//a[@href="/$organization/$repository/watchers" and @class="Link Link--muted"]')
                watch_string = watchersTemplate.substitute(organization = organizations[count], repository = rps)
                watch = driver.find_elements(by='xpath', value=watch_string)
                for elements in watch:
                    watching.append(elements.find_element(by='xpath', value='./strong').text)
                
                releasesTemplate = Template('//a[@class="Link--primary no-underline Link" and @href="/$organization/$repository/releases"]')
                releases_string = releasesTemplate.substitute(organization = organizations[count], repository = rps)
                try:
                    rel = driver.find_elements(by='xpath', value=releases_string)
                    for elements in rel:
                        releases.append(elements.find_element(by='xpath', value='./span[@class="Counter"]').text)
                except NoSuchElementException:
                    releases.append('Not listed...')                
                
                recentRelease.append(attemptScrapeElement(driver, '//relative-time[@class="no-wrap"]', 'Recents releases'))
                
                cont_template = Template('//a[@class="Link--primary no-underline Link d-flex flex-items-center" and @href="/$organization/$repository/graphs/contributors"]')
                cont_string = cont_template.substitute(organization = organizations[count], repository = rps)
                try:
                    cont = driver.find_elements(by='xpath', value=cont_string)
                    for elements in cont:
                        contributors.append(elements.find_element(by='xpath', value='.//span[@class="Counter ml-1"]').text)
                except NoSuchElementException:
                    contributors.append('Not listed...')          
            
            #Now we get json data from the repository API, specifically the languages a repository uses.
            newJsonTemplate = Template("https://api.github.com/repos/$organization/$repository/languages")
            newJsonURL = newJsonTemplate.substitute(organization=organizations[count], repository=rps)
            with urllib.request.urlopen(newJsonURL) as response:
                body_json = response.read()

            json_dictionary = json.loads(body_json)
            languages.append(json_dictionary)
          
        #Iterate the count to let the template know what organization we're looking at.  
        count = count + 1
    
    repoInfoToParquet(index)
       

#Finally, add all the repository information to parquet files.
def repoInfoToParquet(index):
    #This gets all the repository info listed in the repository array and puts them into separate parquet folders, 
    #marked 0--> # of repositories. The indexes of the files should line up with the repository they're from.
    mergedOrgRepoInfo = Template('FinalizedParquets/Repositories/MergedRepoInfo$org.parquet') 
        
    for x in range(0, len(repositories)):
        #Write repository names into parquet files.
        reposDF = pd.DataFrame({'Repository Names': [repositories[x]]})
        writeTable(reposDF, index, 'RepositoriesParquetFolder/repo_name_$index.parquet', x)

        for y in range(0, exactRepoAmounts[0]):
            #Write repository info into parquet files.
            infoDF = pd.DataFrame({'Repository': [repos[0]], 'Forks': [forksTotal[0]], 'Stars': [starsTotal[0]], 'Issues': [issuesTotal[0]], 'Pulls': [pulls[0]], 'Projects': [projects[0]], 'Wiki': [hasWiki[0]], 'Security': [security[0]], 'Branches': [branches[0]], 'Tags': [tags[0]], 'Commits': [commits[0]], 'Softwares': [softwares[0]], 'Watching': [watching[0]], 'Releases': [releases[0]], 'Recent Release': [recentRelease[0]], 'Languages': [languages[0]]})
            infoTable = pa.Table.from_pandas(infoDF)
            newInfoTable = Template('RepoInfoParquetFolder/repo_info_$index1$index2.parquet')
            indexReplaced = newInfoTable.substitute(index1=x, index2=y)
            pq.write_table(infoTable, indexReplaced)
            
            #Remove the first element from each list.
            forksTotal.pop(0)
            starsTotal.pop(0)
            issuesTotal.pop(0)
            pulls.pop(0)
            projects.pop(0)
            hasWiki.pop(0)
            security.pop(0)
            branches.pop(0)
            tags.pop(0)
            commits.pop(0)
            softwares.pop(0)
            watching.pop(0)
            releases.pop(0)
            recentRelease.pop(0)
            languages.pop(0)
            repos.pop(0)
        
        mergedRepoInfo = mergedOrgRepoInfo.substitute(org=repositories[x])
        specialCaseMerge('RepoInfoParquetFolder/*.parquet', mergedRepoInfo)

        #We got the number of repos for org 1, now onto two, so popping the first element.
        exactRepoAmounts.pop(0)
        

getReposPrep(350)