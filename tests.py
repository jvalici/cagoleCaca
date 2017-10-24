import os
import pandas as pd
import numpy as np
from pathHelper import get_path
 
reducedDataDir = os.path.dirname('C:\\Users\\gh\\Documents\\code\\workspace\\KaggleKKBox\\dataTest3\\')



# generate the features for all the user with id in [currentId, nextId-1].
# dfs is the list of the three dataframes for members, transactions, user_logs, and train
def generate_features2( currentId, nextId, dfs ):
    ids = np.arange(currentId, nextId)
    
    # counts
    indicesLeft = [np.zeros(1), np.zeros(1), np.zeros(1), np.zeros(1)]
    counts = [np.zeros(1), np.zeros(1), np.zeros(1), np.zeros(1)]
    for i in range(4):
        indicesLeft[i] = dfs[i][0].searchsorted( ids, side='left' )
        counts[i] = dfs[i][0].searchsorted( ids, side='right' )
        counts[i] = np.subtract(counts[i], indicesLeft[i])
        counts[i] = np.where( counts[i] ==  currentId-currentId, 0, counts[i] )
      
    # reads members
    ids_member_matches = ids ==  dfs[0][0].values[indicesLeft[0]]
    sex = np.where( ids_member_matches, dfs[0][3].values[indicesLeft[0]], 0.5) # 0 for female, 1 for male, 0.5 unknown
    sex = np.where( np.isnan( sex ), 0.5, sex)
    registered_via = np.where( ids_member_matches, dfs[0][4].values[indicesLeft[0]], 0) # date, 0 unknown, should all be known. 0 for missing users
    expiration_date = np.where( ids_member_matches, dfs[0][6].values[indicesLeft[0]], 0) # date, 0 unknown,  should all be known. 0 for missing users
        
    # compute stuff
    #subDfs = [pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
    #for userId in ids:
    #    for i in range(3):
     #       subDfs[i]= dfs[i].loc[[indicesLeft[i][userId], indicesLeft[i][userId] + counts[i][userId]],:]
            
    # generate the output
    return pd.DataFrame.from_dict( {
                                'ids':ids,
                                'count_in_members':counts[0],
                                'count_in_transactions':counts[1],
                                'count_in_logs':counts[2],
                                'count_in_train':counts[3],
                                'sex': sex,
                                'registered_via': registered_via,
                                'expiration_date':expiration_date
                                  }, orient = 'columns')


dfTrain = pd.read_csv( os.path.join( reducedDataDir, 'train-1.csv' ), header=None )
dfMembers = pd.read_csv( os.path.join( reducedDataDir, 'members-1.csv' ), header=None )
dfTransactions = pd.read_csv( os.path.join( reducedDataDir, 'transactions-1.csv' ), header=None )
dfUserLogs = pd.read_csv( os.path.join( reducedDataDir, 'user_logs-1.csv' ), header=None )
dfs = [dfMembers, dfTransactions, dfUserLogs, dfTrain]
dfFeatures = generate_features2(0, 6, dfs)
tmp = generate_features2(6, 11, dfs)
dfFeatures  = dfFeatures.append(tmp)
dfFeatures.to_csv( get_path( reducedDataDir, 'features', None ), index=False, 
                   columns=['ids','count_in_members','count_in_transactions','count_in_logs','count_in_train','sex','registered_via','expiration_date'])