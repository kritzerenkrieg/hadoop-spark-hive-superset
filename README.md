# Data Analytics Big Data Stack with Hadoop, Spark, Hive, Superset, Presto, and Hue
	git clone https://github.com/knight99rus/hadoop_full_pack --config core.autocrlf=input

 	cd ./hadoop_full_pack/docker-files

	docker-compose up --no-start

	docker-compose up -d db redis 

	wait  30 seconds then || docker-compose up -d

This repository provides a Docker Compose environment for setting up a comprehensive big data analytics platform. It includes:

Spark || as Lake and query

Hive Metastore & HiveServer2 || Warehouse

Apache Superset || for BI and visualization

Supporting databases (MySQL for Hue, PostgreSQL for Hive Metastore, PostgreSQL for Superset)

to run test docker cp the datasets and python to hadoop then create table from that csv in hadoop.

<br/>

Service|URL (only list GUIs)|Notes|
| :---:   | :---: | :---: |
namenode|localhost:9870||
datanode|localhost:9864||
hive|localhost:10002||
hue|localhost:8888||
spark master|localhost:8080||
superset|localhost:8008|username:admin - password:admin|
