# Code for custom code recipe upsert (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# # import the classes for accessing DSS objects from the recipe
# import dataiku
# # Import the helpers for custom recipes
# from dataiku.customrecipe import *

# # Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# # or more dataset to each input and output role.
# # Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# # To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
# input_A_names = get_input_names_for_role('input_A_role')
# # The dataset objects themselves can then be created like this:
# input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# # For outputs, the process is the same:
# output_A_names = get_output_names_for_role('main_output')
# output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# # The configuration consists of the parameters set up by the user in the recipe Settings tab.

# # Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# # the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# # user will be prompted for values.

# # The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
# my_variable = get_recipe_config()['parameter_name']

# # For optional parameters, you should provide a default value in case the parameter is not present:
# my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd
import numpy as np
from dataiku import pandasutils as pdu
from dataiku import SQLExecutor2
# Import the helpers for custom recipes
from dataiku.customrecipe import *
import datetime

# Get input and output roles
targetDatasetName = get_input_names_for_role('target_dataset')[0]
sourceDatasetName = get_input_names_for_role('source_dataset')[0]
upsertOutputName = get_output_names_for_role('upsert_output')[0]

# Get target and source datasets
targetDataset = dataiku.Dataset(targetDatasetName)
sourceDataset = dataiku.Dataset(sourceDatasetName)

# Get SQL table names for target and source datasets
# https://community.dataiku.com/t5/Using-Dataiku/List-of-all-project-and-respective-tables-in-dataiku/m-p/23480
targetDatasetFullName = targetDataset.get_location_info()["info"]["table"]
sourceDatasetFullName = sourceDataset.get_location_info()["info"]["table"]

# Get the target and source dataset primary keys
targetDatasetPrimaryKey = get_recipe_config()['target_dataset_primary_key']
sourceDatasetPrimaryKey = get_recipe_config()['source_dataset_primary_key']

# Get SQL table column names for target and source datasets
targetDatasetColumnNamesList = list(
    map(lambda feature: feature["name"], targetDataset.read_schema()))
sourceDatasetColumnNamesList = list(
    map(lambda feature: feature["name"], sourceDataset.read_schema()))

# Get SQL table full column names (with table name prefix) for source and target datasets
sourceDatasetFullColumnNamesList = ['"{0}"."{1}"'.format(
    sourceDatasetFullName, i) for i in sourceDatasetColumnNamesList]
targetDatasetFullColumnNamesList = ['"{0}"."{1}"'.format(
    sourceDatasetFullName, i) for i in targetDatasetColumnNamesList]

# Get SQL "SET" statement clause RE: target dataset
targetDatasetSetStatementList = ['{0} = EXCLUDED.{0}'.format(
    i) for i in targetDatasetColumnNamesList if i != targetDatasetPrimaryKey]

# Build SQL Query as list
# https://dba.stackexchange.com/questions/134493/upsert-with-on-conflict-using-values-from-source-table-in-the-update-part
# https://stackoverflow.com/questions/48922972/postgres-insert-on-conflict-do-update-vs-insert-or-update

# Template:
# INSERT INTO target (target_pk, target_column_1, target_column_2)
# SELECT source_pk, source_column_1, source_column_2
# FROM   source
# ON     CONFLICT (target_pk) DO UPDATE -- conflict is on the unique column
# SET    target_column_1 = EXCLUDED.target_column_1,
#        target_column_2 = EXCLUDED.target_column_2; -- key word "excluded", refer to target column

sqlQueryList = [
    "INSERT INTO",
    '"{0}"'.format(targetDatasetFullName),
    "({0})".format(", ".join(targetDatasetColumnNamesList)),
    "\nSELECT",
    ", ".join(sourceDatasetColumnNamesList),
    "\nFROM",
    '"{0}"'.format(sourceDatasetFullName),
    "\nON CONFLICT",
    '({0})'.format(targetDatasetPrimaryKey),
    "\nDO UPDATE",
    "\nSET\n",
    ",\n".join(targetDatasetSetStatementList)
]

sqlQueryString = " ".join(sqlQueryList)

print("-------------------- RUNNING THE FOLLOWING SQL QUERY --------------------")
print("-------------------- -------------------------------- --------------------")
print("-------------------- -------------------------------- --------------------")
print(sqlQueryString)
print("-------------------- -------------------------------- --------------------")
print("-------------------- -------------------------------- --------------------")
print("-------------------- -------------------------------- --------------------")

# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

# Execute SQL query against target dataset
executor = SQLExecutor2(dataset=targetDataset)
output_df = executor.query_to_df(sqlQueryString, post_queries=['COMMIT'])
output_df = pd.DataFrame({"executed_sql_query": [sqlQueryString],
                         "timestamp": [datetime.datetime.now()]})

# Compute a Pandas dataframe to write into merge_output
merge_output_df = output_df

# Write recipe outputs
merge_output = dataiku.Dataset(upsertOutputName)
merge_output.write_with_schema(merge_output_df)
