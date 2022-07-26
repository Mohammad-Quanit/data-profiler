import json
from dataprofiler import Data, Profiler

# Auto-Detect & Load: CSV, AVRO, Parquet, JSON, Text
data = Data("../1m_sales.csv")
print(len(data.data))

# Access data directly via a compatible Pandas DataFrame
print(data.data.head(5))

# Profile the dataset
profile = Profiler(data)

# Generate a report and use json to prettify.
report = profile.report(report_options={"output_format": "pretty"})

# Print the report
print(json.dumps(report, indent=4))
