Gathering British Airways data from Twitter and Skytrax using API and beautifulsoup respectively and storing them into PostreSQL upon cleaning it. A ML model is trained on the Skytrax data, using Apache Spark, that predicts who would recommend British Airways based on passenger, flight and rating information. The entire process is orchestrated with Apache Airflow and makes use of a dockerized infastructure. Aa an extension interactive dashboard is also created with these data using Holistics.io.

Requirements:
Docker Desktop
Amazon aws account (s3, postgreSQL)
Holistics.io account