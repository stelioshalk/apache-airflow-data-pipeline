# Project Overview: Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

# Project requirements
To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

# Datasets
For this project, you'll be working with two datasets. Here are the s3 links for each:
- Log data: s3://udacity-dend/log_data
- Song data: s3://udacity-dend/song_data

# Project files
Project has two directories named dags and plugins. A create tables script and readme file are at root level:

1. create_tables.sql: SQL create table statements provided with template.

3. dags directory contains:
- udac_example_dag.py : Defines main DAG, tasks and link the tasks in required order.

3. plugins/operators directory contains:

- stage_redshift.py: Defines StageToRedshiftOperator to copy JSON data from S3 to staging tables in the Redshift via copy command.
- load_dimension.py: Defines LoadDimensionOperator to load a dimension table from staging table(s).
- load_fact.py: Defines LoadFactOperator to load fact table from staging table(s).
- data_quality.py: Defines DataQualityOperator to run data quality checks on all tables passed as parameter.
- sql_queries.py: Contains SQL queries for the ETL pipeline (provided in template).

# How to run the project
- Create a Redshift cluster and run create_tables.sql once only.

Make sure to add following two Airflow connections:

- AWS credentials, named aws_conn_id
- Connection to Redshift, named redshift

-Airflow aws_conn_id connection:
<img src="/Images/AWSConnection.png"/>
(/Images/AWSConnection.png)

-Airflow redshift conneciton:
<img src="/Images/RedshiftConnection.png"/>
(/Images/RedshiftConnection.png)

