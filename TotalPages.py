import math


#We use this function to get the number of pages we want to iterate through, and add extra pages in case our number may be off.
def getPages(total, itemsPerPage, additionalPages):
    maxPages = float(total/itemsPerPage)
    maxPages = math.ceil(maxPages)
    maxPages = maxPages + additionalPages
    return maxPages
    
def getFirstPage(total, itemsPerPage):
    firstPage = float(total/itemsPerPage)
    firstPage = math.floor(firstPage)
    firstPage = firstPage + 1;
    return firstPage