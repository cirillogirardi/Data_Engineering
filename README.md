# Schema

## RDS Access

Endpoint: dbwatches.cvzsgnwko6ng.eu-west-2.rds.amazonaws.com
Port: 5432
Master username: postgres
Password: qwerty123


## Data Lineage (PNG)

The image portrays the data process envisioned for the project.

## Data PySpark Notebook

The purpose of the notebook is twofold. The first part is dedicated to connecting to AWS RDS and uploading the relevant parquet files to the schema created. The second part of the notebook is dedicated to conducting basic SQL queries. Specifically, the second part is devoted to retrieving data from the RDS Database and merging various tables to create a unique file which can be used for explanatory data analysis and/ or machine learning.

## Machine Learning Notebook

This notebook uses the table created using SQL queries in the Data PySpark Notebook to conduct machine learning and solve the business solution initially posed. Specifically, the project wanted to use the data retrieved from the data sources in order to use the data to further understand how the watch industry functions. With the data retrieved it was possible to conduct a regression analysis to understand what major features drive aftermarket price.


## Watches (SQL & PDF)

These files depict the schema that was utilised for the development of the AWS RDS database. The PDF version graphically depicts the schema and the connections between the tables, whilst the SQL file shows the sql code for the development of the schema.
