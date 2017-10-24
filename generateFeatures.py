import numpy as np
import pandas as pd

# generate the features for all the user with id in [currentId, nextId-1].
# dfs is the list of the three dataframes for members, transactions, user_logs, and train
def generate_features( currentId, nextId, dfs ):
    ids = np.arange(currentId, nextId)
    indicesLeft = [np.zeros(1), np.zeros(1), np.zeros(1), np.zeros(1)]
    counts = [np.zeros(1), np.zeros(1), np.zeros(1), np.zeros(1)]
    for i in range(4):
        indicesLeft[i] = dfs[i][0].searchsorted( ids, side='left' )
        counts[i] = dfs[i][0].searchsorted( ids, side='right' )
        counts[i] = np.subtract(counts[i], indicesLeft[i])
        counts[i] = np.where( counts[i] ==  currentId-currentId, 0, counts[i] )
    return pd.DataFrame.from_dict( {0:ids, 1:counts[0], 2:counts[1], 3:counts[2], 4:counts[3] }, orient = 'columns')
