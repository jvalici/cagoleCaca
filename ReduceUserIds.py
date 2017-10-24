import numpy as np
import pandas as pd
import os
import re
from pathHelper import get_path, get_existing_path 

#https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data
kaggleDataDir = os.path.dirname('C:\\Users\\gh\\Documents\\code\\workspace\\KaggleKKBox\\data\\')
splitDataDir = os.path.dirname('F:\\JeanTmp\\KaggleKKBox\\splitDataFullIds\\')
#splitDataDir = os.path.dirname('C:\\Users\\gh\\Documents\\code\\workspace\\KaggleKKBox\\dataTest\\')
reducedDataDir = os.path.dirname('C:\\Users\\gh\\Documents\\code\\workspace\\KaggleKKBox\\data\\')


# get number of known and unknown users(): 
def get_number_of_known_and_unknown_users(): 
    mappingFilePath = os.path.join( splitDataDir, 'sorted_member_ids.csv')
    tmp = pd.read_csv( mappingFilePath, header=None )
    number_of_known_users = tmp.shape[0]
    mappingFilePath = os.path.join( splitDataDir, 'unknown_user_ids_sorted.csv')
    tmp = pd.read_csv( mappingFilePath, header=None )
    number_of_unknown_users = tmp.shape[0]
    tmp=pd.DataFrame([number_of_known_users, number_of_unknown_users])
    mappingFilePath = os.path.join( splitDataDir, 'number_of_known_and_unknown_users.csv')
    tmp.to_csv(mappingFilePath, index=False, header=None)

# the index in that list is the new id!
def save_known_user_ids(): 
    membersFilePath = os.path.join( kaggleDataDir, 'members.csv')
    tmp = pd.read_csv( membersFilePath, header=None )
    tmp.sort_values(by=0,inplace =True)
    ids = tmp[0]
    mappingFilePath = os.path.join( splitDataDir, 'sorted_member_ids.csv')
    ids.to_csv(mappingFilePath, index=False, header=None)
    return
    
# add unknown user to the current list      
def get_unknown_user_ids_sorted( fileType, knownSortedUserIds, current_unknow_user_ids):
    numberOfFiles = len([name for name in os.listdir( reducedDataDir ) if re.match(fileType+'-[0-9]+'+'.csv', name)])
    for fileIndex in range(1,numberOfFiles + 1):# from 1 to numberOfFiles)
        infilePath = get_existing_path( reducedDataDir, fileType, fileIndex )
        df = pd.read_csv(infilePath, header=None, dtype={0: object} )
        new_unknow_user_ids  = [ x for x in df[0] if x not in knownSortedUserIds ] 
        current_unknow_user_ids =  current_unknow_user_ids + sorted(set(new_unknow_user_ids) - set(current_unknow_user_ids))
    return current_unknow_user_ids

# save a file of the encountered user ids which are not in 'members'
def save_unknown_user_ids():
    knownSortedUserIdsPath = os.path.join( splitDataDir, 'sorted_member_ids.csv')
    knownSortedUserIds = pd.read_csv(knownSortedUserIdsPath, header=None, dtype=str )
    current_unknow_user_ids=[]
    current_unknow_user_ids = get_unknown_user_ids_sorted( 'train', knownSortedUserIds[0], current_unknow_user_ids )
    current_unknow_user_ids = get_unknown_user_ids_sorted( 'user_logs', knownSortedUserIds[0], current_unknow_user_ids )
    current_unknow_user_ids = get_unknown_user_ids_sorted( 'transactions', knownSortedUserIds[0], current_unknow_user_ids )
    df = pd.DataFrame(current_unknow_user_ids)
    tmp = df[0].values.tolist()
    tmp.sort() # sorts normally by alphabetical order
    tmp.sort(key=len) # sorts by descending length
    df = pd.DataFrame(tmp)
    outFilePath = os.path.join( reducedDataDir, 'unknown_user_ids_sorted2.csv')
    df.to_csv(outFilePath, index=False, header=None)

# replace the old ids in the files by the new ids (index in the sorted list of ids)
# this reduces the size of the file by two
def reduce_ids( filePath, knownSortedUserIds, unknownSortedUserIds ):
    df = pd.read_csv( filePath, header=None )
    numberOfKnownUsers = len(knownSortedUserIds)
    #  map old ids to new ids (index in the list of the sorted id)
    idx_known = knownSortedUserIds.searchsorted(df[0])
    idx_unknown = unknownSortedUserIds.searchsorted(df[0])
    # only copy where the match is exact
    exact_matches_known = knownSortedUserIds[idx_known] == df[0].values
    exact_matches_unknown = unknownSortedUserIds[idx_unknown] == df[0].values
    df[0] = np.where(exact_matches_known, idx_known, df[0])
    df[0] = np.where(exact_matches_unknown, [x + numberOfKnownUsers for x in idx_unknown], df[0])
    # write to file
    return df

# merge 2 consecutive files and rename
def reduce_number_of_files_and_ids( fileType ):
    # get the mapping
    knownSortedUserIdsPath = os.path.join( splitDataDir, 'sorted_member_ids.csv')
    knownSortedUserIds = pd.read_csv(knownSortedUserIdsPath, header=None, dtype=str )
    unknownSortedUserIdsPath = os.path.join( splitDataDir, 'unknown_user_ids_sorted.csv')
    unknownSortedUserIds = pd.read_csv(unknownSortedUserIdsPath, header=None, dtype=str )
    
    # get the number of files
    numberOfFiles = len([name for name in os.listdir( splitDataDir ) if re.match(fileType+'-[0-9]+'+'.csv', name)])
    # loop through the files
    newFileIndex = 1
    for workIndex in range(1,numberOfFiles + 1,2):# from 1 to numberOfFiles - 1 by 2
        infilePath = get_existing_path( splitDataDir, fileType, workIndex ) 
        df = reduce_ids( infilePath, knownSortedUserIds[0], unknownSortedUserIds[0] )
        if workIndex < numberOfFiles:
            infilePath2 = get_existing_path( splitDataDir, fileType, workIndex + 1 )
            df2 = reduce_ids( infilePath2, knownSortedUserIds[0], unknownSortedUserIds[0] )
            # merge
            df = df.append(df2)
            # remove file 2
            if splitDataDir == reducedDataDir:
                os.remove(infilePath2)
        # remove file 1
        if splitDataDir == reducedDataDir:
            os.remove(infilePath)
        # write to file
        df.sort_values(by=0,inplace =True)
        df.to_csv( get_path( reducedDataDir, fileType, newFileIndex ), index=False, header=None)
        newFileIndex = newFileIndex + 1



# merge 2 consecutive files and rename
def reduce_number_of_files( fileType ):
    # get the number of files
    numberOfFiles = len([name for name in os.listdir( reducedDataDir ) if re.match(fileType+'-[0-9]+'+'.csv', name)])
    # loop through the files
    newFileIndex = 1
    for workIndex in range(1,numberOfFiles + 1,2):# from 1 to numberOfFiles - 1 by 2
        infilePath = get_existing_path( reducedDataDir, fileType, workIndex ) 
        df = pd.read_csv( infilePath, header=None )
        if workIndex < numberOfFiles:
            infilePath2 = get_existing_path( reducedDataDir, fileType, workIndex + 1 )
            df2 =  pd.read_csv( infilePath2, header=None )
            # merge
            df = df.append(df2)
            # remove file 2
            os.remove(infilePath2)
            print( 'delete ' + infilePath2 )
        # remove file 1
        os.remove(infilePath)
        print( 'delete ' + infilePath )
        # write to file
        df.to_csv( get_path( reducedDataDir, fileType, newFileIndex ), index=False, header=None)
        print( 'save file ' + get_path( reducedDataDir, fileType, newFileIndex ) )
        newFileIndex = newFileIndex + 1



# do it
#save_known_user_ids()
#save_unknown_user_ids()
#get_number_of_known_and_unknown_users()
#reduce_number_of_files_and_ids( 'train' )
#reduce_number_of_files_and_ids( 'user_logs' )
#reduce_number_of_files_and_ids( 'members' )
#reduce_number_of_files_and_ids( 'transactions' )
reduce_number_of_files( 'user_logs' )
