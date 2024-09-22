from string import Template
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


#This writes list to a parquet file, but only takes in one list to write into a file.
def writeToParquet(maxToIterate, firstIndex, list, title, templateString):
    orgTest = []
    for x in range(0, maxToIterate):
        orgTest.append(list[x])
        orgDF = pd.DataFrame({title: [orgTest[x]]})
        writeTable(orgDF, firstIndex, templateString, x)
    return orgTest


#This fills in a table to be used in an indexed parquet file.
def writeTable(Table_DF, firstIndex, location, ind):
    table = pa.Table.from_pandas(Table_DF)
    newTable = Template(location)
    indexReplaced = newTable.substitute(index=(ind+firstIndex))
    pq.write_table(table, indexReplaced)
    