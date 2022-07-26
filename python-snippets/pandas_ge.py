from textwrap import indent
import great_expectations as ge
import pandas as pd
import dask.dataframe as dd

from pandas_profiling import ProfileReport

# pd.read_csv("sdasd").to_json()
df = dd.read_csv("../1m_sales.csv")
pd = df.compute()
print(df.dtypes)
print(df.to_json())

#profile = ProfileReport(df, title="Pandas Profiling Report", explorative=True)
# profile.to_file("your_report.json")
# df.to_html("file.html")

# # Example 1
# # Obtain expectation suite, this includes profiling the dataset, saving the expectation suite, validating the
# # dataframe, and building data docs
# suite = profile.to_expectation_suite(suite_name="titanic_expectations")

# # Example 2
# # Run Great Expectations while specifying the directory with an existing Great Expectations set-up by passing in a
# # Data Context
# data_context = ge.data_context.DataContext(
#     context_root_dir="my_ge_root_directory/")

# suite = profile.to_expectation_suite(
#     suite_name="titanic_expectations", data_context=data_context
# )

# # Example 3
# # Just build the suite
# suite = profile.to_expectation_suite(
#     suite_name="titanic_expectations",
#     save_suite=False,
#     run_validation=False,
#     build_data_docs=False,
# )

# # Example 4
# # If you would like to use the method to just build the suite, and then manually save the suite, validate the dataframe,
# # and build data docs

# # First instantiate a data_context
# data_context = ge.data_context.DataContext(
#     context_root_dir="my_ge_root_directory/")

# # Create the suite
# suite = profile.to_expectation_suite(
#     suite_name="titanic_expectations",
#     data_context=data_context,
#     save_suite=False,
#     run_validation=False,
#     build_data_docs=False,
# )

# # Save the suite
# data_context.save_expectation_suite(suite)

# # Run validation on your dataframe
# batch = ge.dataset.PandasDataset(df, expectation_suite=suite)

# results = data_context.run_validation_operator(
#     "action_list_operator", assets_to_validate=[batch]
# )
# validation_result_identifier = results.list_validation_result_identifiers()[0]

# # Build and open data docs
# data_context.build_data_docs()
# data_context.open_data_docs(validation_result_identifier)
