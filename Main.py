from TopOrgScrape import scrapeOrganizations
from Repositories import getReposPrep
from RepoCounts import numReposPrep
from OrgInfo import orgsToScrape
from People import getPeoplePrep


#Based on the 'Gitstar Ranking' website, we scrape organizations, starting from the most popular ones. So, if we scrape 10 organizations at index 0, we get 
#organizations 1-10, and if we were to scrape 20 organizations at index 90, we'd get organizations 91-110.
orgsScrape = input("Enter the number of organizations you would like to scrape: ")
indexStart = input("Enter the index you would like to start scraping at: ")

orgsScrape = int(orgsScrape)
indexStart = int(indexStart)

print(type(orgsScrape))
print(type(indexStart))


#1. 'scrapeOrganizations', derived from 'TopOrgScrape.py', is the first function called. It takes in the number of organizations and the start index the user inputs, 
#   scrapes 'Gitstar Ranking' for these organizations, and returns these organizations into their own respective parquet files in the folder 'OrgInformationFolder'.
scrapeOrganizations(orgsScrape, indexStart)

#2. 'numReposPrep', derived from 'RepoCounts.py', is used in order to get the number of repositories each organization has, which will be sent to individually indexed 
#   parquet files in 'RepoAmountParquetFolder'.
numReposPrep(indexStart)

#3. 'orgsToScrape', derived from 'OrgInfo.py', collects various information about the requested organizations. It will store this information in a folder called 
# "OrgInformationFolder".
orgsToScrape(indexStart)

#4. 'getPeoplePrep', derived from 'People.py', scrapes the desired organizations for the names of the people they contain, and stores this information in a folder 
# called "PeopleNamesFolder".
getPeoplePrep(indexStart)

#5. Lastly, 'getReposPrep', derived from 'Repositories.py', iterates through every repository in each organization and returns information, such as the name, number
#   of stars, number of followers, number of pulls, etc. This information is stored in a folder called "RepoInfoParquetFolder".
getReposPrep(indexStart)