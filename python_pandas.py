import time
import pandas as pd
import dask.dataframe as dd


def getDistinctColVals(ddf):
  counts = []
  for column in ddf.columns:
    counts.append(ddf[column].nunique().compute())
  return pd.Series(counts, index=ddf.columns).to_json()


def getDataInJSON(ddf):
  return pd.DataFrame(data=ddf).to_json()

def getOnlyNumCols(ddf):
  ddf.apply(lambda s: pd.to_numeric(s, errors='coerce').notnull().all(), axis=1, meta=ddf)

start = time.time()
ddf = dd.read_csv("data.csv").persist()

end = time.time()

# print(ddf.dtypes)
# print(getDataInJSON(ddf))
# print(getDistinctColVals(ddf))
print(getOnlyNumCols(ddf))

# print(list(ddf["Lower_CI"].astype('str').str.isnumeric()))


print("Loaded in ", end-start, "seconds")