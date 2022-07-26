import time
import pandas as pd
import numpy as np
import dask.dataframe as daskdf

from dask.distributed import Client

client = Client()


def getColumnValuesMaxLength(df):
    maxVals = []
    for col in df:
        maxVals.append(df[col].astype(str).str.len().max().compute())
    return maxVals


def getColumnDataTypes(df):
    return df.dtypes


def getEmptyValues(df):
    return len(df.isnull())


def getColumnDistinctValuesLength(df: daskdf):
    distinctCounts = []
    for label, _ in df.items():
        distinctCounts.append(df[label].nunique().compute())
    return distinctCounts


start = time.time()

# read data without chunks of 1 million rows at a time
# df = pd.read_csv("../1m_sales.csv")

# read data in chunks of 11 million rows at a time
# chunk = pd.read_csv("./11m-data.csv", chunksize=1000000)
# df = pd.concat(chunk)

# read data dask of 1 million rows at a time
df = daskdf.read_csv("../1m_sales.csv")

# print(dict(df.head()))
# print(getColumnDataTypes(df))
# print(getColumnValuesMaxLength(df))
# print(getColumnDistinctValuesLength(df))
print(getEmptyValues(df))

end = time.time()
print("Read csv with pandas: ", (end-start), "sec")

# measurer = np.vectorize(len)
# print(measurer(df.values.astype(str)).max(axis=0))
# print(df.to_json(indent=4, orient="records"))
