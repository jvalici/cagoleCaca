import csv
import numpy as np
import pandas as pd
import os
import re
from pathHelper import get_existing_path 
from datetime import datetime

#https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data
reducedDataDir = os.path.dirname('C:\\Users\\gh\\Documents\\code\\workspace\\KaggleKKBox\\data\\')


# sort the files of the same file type.
# the id is to merge two files, sort and re-split and do it again with the files containing the bid ids and the following file
# at the end of the first loop, the last file will contain all the larger ids.
# we do restart with the first file, but this time we can stop at the file before the end
# etc...
# when splitting the file we are getting careful to all the same ids in the same file
def sort_files( fileType, index_for_second_sort ):
    numberOfFiles = len([name for name in os.listdir( reducedDataDir ) if re.match(fileType+'-[0-9]+'+'.csv', name)])
    numberOfFiles = min( numberOfFiles, 81 )
    if numberOfFiles == 1:
        raise("Sort single file separately")
    # start the loop where at the end of an iteration all the following files are sorted (the "backward loop")
    for indexOfTheSortedFileAtTheEndOfThisLoop in range(numberOfFiles,0,-1): # this goes from numberOfFiles to 1
        # start the forward loop, where we merge sort and split
        smallIdsFilePath = get_existing_path( reducedDataDir, fileType, 1 )
        # put the file with small ids into a data frame
        dfSmall = pd.read_csv(smallIdsFilePath, header=None )
        for fileNumber in range(1,indexOfTheSortedFileAtTheEndOfThisLoop ):# this goes from 1 to indexOfTheSortedFileAtTheEndOfThisLoop -1 
            print( str( fileNumber ) + ' at ' +  str( datetime.now()) )
            bigIdsFilePath = get_existing_path( reducedDataDir, fileType, fileNumber + 1 )
            # put the file with large ids into a data frame
            dfBig = pd.read_csv(bigIdsFilePath, header=None )
            #  merge
            df = dfSmall.append(dfBig)
            # sort
            if  index_for_second_sort is None:
                df.sort_values(by=0,inplace =True)
            else:   
                df.sort_values(by=[0,index_for_second_sort],inplace =True)
            # split (being careful that on id is not in two files)
            mid = int( df.shape[0]/2 )
            while df[0].values[mid-1]==df[0].values[mid]:
                mid = mid + 1
            dfs = np.split(df, [mid], axis=0)
            # write the small ids
            dfs[0].to_csv(smallIdsFilePath, index=False, header=None)
            # also write the large ids on the last iteration
            if fileNumber + 1 == indexOfTheSortedFileAtTheEndOfThisLoop:
                dfs[1].to_csv(bigIdsFilePath, index=False, header=None)
                print( bigIdsFilePath + ' is now sorted at ' +  str( datetime.now()))
            else:# prepare the next iteration: the bid ids become the small ids
                dfSmall = dfs[1]
                smallIdsFilePath = bigIdsFilePath
    # now write the first id of each file into a file
    res = []
    for fileNumber in range( 1, numberOfFiles + 1):
        filePath = get_existing_path( reducedDataDir, fileType, fileNumber )
        with open(filePath, 'r') as infile:
            firstRow = next(csv.reader(infile))
            res = res + [firstRow[0]]
    df = pd.DataFrame(res)
    outFilePath = os.path.join( reducedDataDir, fileType + '_first_ids.csv')
    df.to_csv(outFilePath, index=False, header=None)
    return


# do it
#sort_files( 'members', None )
sort_files( 'user_logs', 1 )
sort_files( 'transactions', 6 )

