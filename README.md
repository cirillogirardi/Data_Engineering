# Apache Airflow

## Airflow Access 

Airflow was installed and accessed with the following commands

pip install apache-airflow==2.2.3 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-no-providers-3.9.txt

airflow db init

cd airflow

airflow users create \
          --username admin \
          --firstname Cirillo \
          --lastname Girardi \
          --role Admin \
          --email cirillo.girardi@gmail.com


sudo lsof -i tcp:8080

kill -9 xxxxx

airflow webserver -D

airflow scheduler -D

localhost:8080


## Data Lineage 

This picture depicts the data process utilized throughout the project.

## Apache Diagram and Tree

The two images Apache Diagram and Apache Tree, are screenshots from Apache Airflow respectively portraying the final graph and tree used to run the python script 'final_watches'.

## S3 Bucket

This picture depicts the files on AWS S3

## Final Watches

The python script 'final_watches' is the final script that was created utilizing the notebooks present within the other brances. The script was developed to automate the web scraping and data storage process in order to retrieve relevant information from different data sources, clean the data and further store it in an AWS S3 bucket. The script was developed for Apache Airflow and thus clearly delineates how and when each activity should be run. 

The results of the Airflow pipeline are stored on S3. 

