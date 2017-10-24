import csv
import os
import re
import pandas as pd
import numpy as np
from pathHelper import get_path, get_existing_path 

#https://www.kaggle.com/c/kkbox-churn-prediction-challenge/data
kaggleDataDir = 'C:\\Users\\gh\\Documents\\code\\workspace\\KaggleKKBox\\data\\'
splitDataDir  ='F:\\JeanTmp\\KaggleKKBox\\splitDataFullIds\\'
reducedDataDir = os.path.dirname('C:\\Users\\gh\\Documents\\code\\workspace\\KaggleKKBox\\dataTest2\\')
nbLinesPerFile = 2000000


def split_file( fileType ):
    outFileNo = 1
    outFile = None
    inFilePath = kaggleDataDir  + fileType  + '.csv'
    with open(inFilePath, 'r') as infile:
        for index, row in enumerate(csv.reader(infile)):
            if index % nbLinesPerFile == 0:
                if outFile is not None:
                    outFile.close()
                outFileName = fileType + '-{}.csv'.format(outFileNo)
                outFilePath = splitDataDir + outFileName
                outFile = open(outFilePath, 'w', newline="\n" )
                outFileNo += 1
                writer = csv.writer(outFile)
            writer.writerow(row)

def resplit_user_log_files():
    firstIds = []
    numberOfFiles = len([name for name in os.listdir( reducedDataDir ) if re.match('user_logs-[0-9]+'+'.csv', name)])
    for fileNumber in range(numberOfFiles,0,-1): # this goes from numberOfFiles to 1
        dfBig = pd.read_csv( get_existing_path( reducedDataDir, 'user_logs', fileNumber ), header=None )
        mid = int( dfBig.shape[0]/2 )
        while dfBig[0].values[mid-1]==dfBig[0].values[mid]:
            mid = mid + 1
        dfs = np.split(dfBig, [mid], axis=0)
        dfs[0].to_csv( get_path( reducedDataDir, 'user_logs', 2 * fileNumber -1 ), index=False, header=None)
        dfs[1].to_csv( get_path( reducedDataDir, 'user_logs', 2 * fileNumber ), index=False, header=None)
        firstIds = firstIds + [dfs[0][0].values[0],dfs[1][0].values[0]]
    df = pd.DataFrame( firstIds )
    df.sort_values(by=0,inplace =True)
    outFilePath = os.path.join( reducedDataDir, 'user_logs_first_ids.csv')
    df.to_csv(outFilePath, index=False, header=None)
    return
# do it       
#split_file( 'members' )
#split_file( 'transactions' )
#split_file( 'user_logs' )
resplit_user_log_files()
