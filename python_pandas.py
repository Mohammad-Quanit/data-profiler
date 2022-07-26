import json
import time
import pandas as pd
import dask.dataframe as dd


def get_percentage(current, previous):
    if current == previous:
        return 100.0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return 0


def getDataInJSON(ddf):
    return pd.DataFrame(data=ddf).to_json()


def getColumnDataTypes(ddf):
    return dict(ddf.dtypes)


def getDistinctColumnValuesLength(ddf):
    counts = []
    for column in ddf.columns:
        counts.append(ddf[column].nunique().compute())
    return pd.Series(counts, index=ddf.columns).to_json()


def getOnlyNumCols(ddf):
    ddf.apply(lambda s: pd.to_numeric(
        s, errors='coerce').notnull().all(), axis=1, meta=ddf)


def getColumnValuesMaxLength(ddf):
    maxVals = []
    for col in ddf:
        maxVals.append(ddf[col].astype(str).str.len().max().compute())
    return pd.Series(maxVals, index=ddf.columns).to_json()


def getColumnEmptyValuesLength(ddf):
    nullCounts = []
    for column in ddf.columns:
        nullCounts.append(ddf[column].isnull().sum().compute())
    return pd.Series(nullCounts, index=ddf.columns).to_json()


def getColumnFilledValuesLength(ddf):
    filledCounts = []
    for column in ddf.columns:
        filledCounts.append(len(ddf) - ddf[column].isnull().sum().compute())
    return pd.Series(filledCounts, index=ddf.columns).to_json()


def getColumnFilledValuesPercentage(ddf):
    filledPercent = []
    for column in ddf.columns:
        percent = int(get_percentage(
            ddf[column].isnull().sum().compute(), len(ddf)))
        filledPercent.append(percent)
    return pd.Series(filledPercent, index=ddf.columns).to_json()


start = time.time()
ddf = dd.read_csv("data.csv").persist()

end = time.time()

# print(getDataInJSON(ddf))
# print(getColumnDataTypes(ddf))
# print(getDistinctColumnValuesLength(ddf))
# print(getOnlyNumCols(ddf))
# print(getColumnValuesMaxLength(ddf))
# print(getColumnEmptyValuesLength(ddf))
# print(getColumnFilledValuesLength(ddf))
print(getColumnFilledValuesPercentage(ddf))

# print(list(ddf["Lower_CI"].astype('str').str.isnumeric()))


print("Loaded in ", end-start, "seconds")
