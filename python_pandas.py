import json
import pprint
import time
import dask.dataframe as dd
from sqlalchemy import distinct


def getDistinctColVals(ddf):
  colValsDict = {'distinctValues': list(ddf.nunique().compute())}
  return json.dumps(colValsDict, indent = 4)


def getDataInJSON(ddf):
  # numCols = {}
  # for column in ddf.columns:
  #   numCols[column] = list(ddf[column].values.compute())
  # return json.dumps(numCols, indent = 4)
  return json.dumps(json.loads(ddf.to_json(orient="records")))



start = time.time()
ddf = dd.read_csv("data.csv").persist()

end = time.time()

# print(ddf.dtypes)
print(getDataInJSON(ddf))
print(getDistinctColVals(ddf))
# print(list(ddf["Lower_CI"].astype('str').str.isnumeric()))


print("Loaded in ", end-start, "seconds")
