import numpy as np
import pandas as pd
from os import listdir
from os import system
from os.path import isfile, join
import time
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib

matplotlib.use('Qt5Agg')
pd.set_option('display.max_columns', 12)
pd.set_option('display.width', 1000)

start = time.time()
end = start
path = '//home//hassaan//Data//TUM//2nd Year//4th Semester//IDP//Project//data//raw//FinancesData//'#'Repo\\data\\raw\\FinancesData\\'
congs = listdir(path)

log = []
uniqueFiles = []
addedPaths = []
dfs = []

for i in congs:
    congmen = listdir(path + i)
    print(i)
    for j in congmen:
        files = listdir(path + i + '//' + j)
        for k in files:
            cid = k[k.find('cid=') + 4:k.find('&year')]
            year = k[k.find('&year=') + 6:k.find('.csv')]
            if (cid, year) in uniqueFiles:
                continue

            filePath = path + i + '//' + j + '//' + k
            try:
                df = pd.read_csv(filePath)
            except pd.errors.EmptyDataError:
                log.append('pd EmptyDataError: ' + filePath)
                continue

            if df.empty:
                log.append('DataFrame empty: ' + filePath)
                continue

            df['cid'] = cid
            df['personName'] = j
            df['year'] = year
            df['folderOfOrigin'] = i

            dfs.append(df.copy())
            uniqueFiles.append((cid, year))
            addedPaths.append(filePath)
            del df
    print('Total time taken for ' + i + ': ', time.time() - end)
    log.append('Total time taken for ' + i + ': ' + str(time.time() - end))
    end = time.time()

end = time.time()
print('Total time taken to read all files: ', end - start)
log.append('Total time taken to read all files: ' + str(end - start))

with open('consolidate log.txt', 'w') as f:
    for item in log:
        f.write("%s\n" % item)

with open('consolidate added paths.txt', 'w') as f:
    for item in addedPaths:
        f.write("%s\n" % item)

data = pd.concat(dfs, ignore_index=True)
data = data[['cid', 'personName', 'year', 'orgid', 'orgname', 'industry', 'asset_type', 'min', 'max', 'hidemax', 'type', 'lobbies', 'contributes', 'folderOfOrigin']]

data.to_pickle('consolidated.pkl')
data.to_csv('consolidated.csv')

end = time.time()
print('Total time taken: ', end - start)
log.append('Total time taken: ' + str(end - start))

# People per year

years = data.groupby(['personName', 'year']).size().index.to_list()
years = pd.Series([i[1] for i in years]).value_counts()

