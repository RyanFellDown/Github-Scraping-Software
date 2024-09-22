import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import glob


#Used for combining multiple files together from the same location, ie if we have 20 organizations into 20 parquet files, we combine them into 1 here.
def combineParquetFiles(parquetLocation, combinationLocation):
    fileList = glob.glob(parquetLocation)
    tables = [pq.read_table(file) for file in fileList]
    merged_table = pa.concat_tables(tables)
    pq.write_table(merged_table, combinationLocation)
    
#Used for combining multiple files together from different locations, ie if we have merged organizations in one location and merged repo counts
#in another location, we combine them into 1 file here.
def multipleLocationCombination(parquetLocation1, parquetLocation2, combinationLocation):
    df1 = pd.read_parquet(parquetLocation1)
    df2 = pd.read_parquet(parquetLocation2)
    merged_df = pd.concat([df1, df2], axis=1)
    
    #It wouldn't align the columns correctly, adding NULL values where there shouldn't be, so we align and drop NULL values.
    aligned_df = merged_df.dropna(how='all')
    aligned_df = aligned_df.reset_index(drop=True)
    
    aligned_df.to_parquet(combinationLocation)
    
    
def specialCaseMerge(file_list, output_path):
    #Language list in repository scrape is presenting a problem, so we're fixing it with this merging structure.
    file_list = glob.glob(file_list)
    
    noLanguageDF = []
    languageColumns = []

    for file in file_list:
        df = pd.read_parquet(file)

        languages = df['Languages']
        languageColumns.append(languages)

        noLanguageDF.append(df.drop(columns=['Languages']))

    mergedNoLanguagesDF = pd.concat(noLanguageDF, ignore_index=True)

    mergedDF = mergedNoLanguagesDF.copy()
    startIndex = 0
    
    for i, languages in enumerate(languageColumns):
        end_idx = startIndex + len(languages)
        mergedDF.loc[startIndex:end_idx-1, 'Languages'] = languages.values
        startIndex = end_idx

    mergedDF.to_parquet(output_path)