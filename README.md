Gathering British Airways data from Twitter and Skytrax using API and beautifulsoup respectively and storing them into PostreSQL upon cleaning it. The entire process is orchestrated with Apache Airflow and makes use of a dockerized infastructure. As an extension, a ML model is trained on a SageMaker Notebook instance using the cleaned Skytrax data, with Apache Spark, that predicts who would recommend British Airways based on passenger, flight and rating information. Additionally, an interactive dashboard is created with these data using Holistics.io.

Requirements:
Docker Desktop
Amazon aws account (s3, postgreSQL, SageMaker notebook instance)
Holistics.io account
