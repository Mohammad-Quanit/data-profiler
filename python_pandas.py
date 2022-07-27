import json
import time
import numpy as np
import pandas as pd
import dask.dataframe as dd
# from dask.distributed import Client

# client = Client(scheduler='threads')


def get_percentage(current, previous):
    if current == previous:
        return 100.0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return 0


def get_json_data(ddf):
    return ddf.compute().to_json(indent=2, orient='records')
    # return pd.Series(ddf.columns).to_json(indent=2, orient='records', )


def get_column_data_types(ddf):
    return ddf.dtypes.apply(lambda x: x.name).to_dict()
    # return json.dumps(dict(ddf.dtypes), indent=2)


def get_distinct_column_values_length(ddf):
    counts = []
    for column in ddf.columns:
        counts.append(ddf[column].nunique().compute())
    return pd.Series(counts, index=ddf.columns).to_json(indent=2)


def get_column_values_max_length(ddf):
    maxVals = []
    for col in ddf:
        maxVals.append(ddf[col].astype(str).str.len().max().compute())
    return pd.Series(maxVals, index=ddf.columns).to_json(indent=2)


def get_column_empty_values_length(ddf):
    nullCounts = []
    for column in ddf.columns:
        nullCounts.append(ddf[column].isnull().sum().compute())
    return pd.Series(nullCounts, index=ddf.columns).to_json(indent=2)


def get_column_filled_values_length(ddf):
    filledcounts = []
    for column in ddf.columns:
        filledcounts.append(len(ddf) - ddf[column].isnull().sum().compute())
    return pd.Series(filledcounts, index=ddf.columns).to_json(indent=2)


def get_column_filled_values_percentage(ddf):
    filledPercent = []
    for column in ddf.columns:
        percent = int(get_percentage(
            ddf[column].isnull().sum().compute(), len(ddf)))
        filledPercent.append(percent)
    return pd.Series(filledPercent, index=ddf.columns).to_json(indent=2)


def get_min(ddf):
    numCols = ddf.describe(include=[np.number])
    return json.dumps(dict(numCols.min().compute()), indent=2)


def get_max(ddf):
    numCols = ddf.describe(include=[np.number])
    return json.dumps(dict(numCols.max().compute()), indent=2)


def get_mean(ddf):
    numCols = ddf.describe(include=[np.number])
    return json.dumps(dict(numCols.mean().compute()), indent=2)


start = time.time()
ddf = dd.read_csv("5m Sales Records.csv").persist()
print("File Loaded in --> ", time.time()-start, " seconds")
ddf = ddf.compute()

# print(ddf.to_json(orient='records', indent=2))

data_dict = {
    "col_data_types": get_column_data_types(ddf),
    "col_distinct_values_len": get_distinct_column_values_length(ddf),
    "col_values_max_len": get_column_values_max_length(ddf),
    "col_empty_values_len": get_column_empty_values_length(ddf),
    "col_filled_values_len": get_column_filled_values_length(ddf),
    "col_filled_values_percentage": get_column_filled_values_percentage(ddf),
    "col_min_value_length": get_min(ddf),
    "col_max_value_length": get_max(ddf),
    "col_mean_value": get_mean(ddf),
}

# print(data_dict)
print(json.dumps(data_dict, indent=4))

# print(list(ddf["Lower_CI"].astype('str').str.isnumeric()))

end = time.time()
print("Finished in --> ", end-start, "seconds")


# def getOnlyNumCols(ddf):
#     ddf.apply(lambda s: pd.to_numeric(
#         s, errors='coerce').notnull().all(), axis=1, meta=ddf)
