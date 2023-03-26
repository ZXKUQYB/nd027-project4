# Project: Data Lake

<br>

This project is considered as an extension of both the Project: Data Modeling with Postgres, the 1st project of the Data Engineering Nanodegree program, and the Project: Data Warehouse, the 3rd project of the Data Engineering Nanodegree program. It shares the same purpose and analytical goals in context of the company Sparkify, as well as the design of database schemas, but the differences in ETL pipelines intensify even further:

* There is no database system any more. The main data platform used in the project is [Apache Spark](https://spark.apache.org/), which is a unified multi-language engine for large-scale data analytics.
* Though it is possible to create an in-house Spark installation from scratch, we will be using [Amazon EMR](https://aws.amazon.com/emr/), an [analytics](https://aws.amazon.com/big-data/datalakes-and-analytics/) service offered by AWS, to setup a cloud-based Spark installation to test run Python code scripts of the project. But be aware that you will need to choose [the cheapest instance types](https://aws.amazon.com/emr/pricing/) by yourself, and some of the cheap instance types may be unavailable in the AWS regions you choose to create the EMR cluster (see [this](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-INSTANCE_TYPE_NOT_SUPPORTED-error.html) for more info).
* Like the previous project, the unprocessed JSON raw files are placed in a S3 bucket managed by Udacity, and this favors the manipulation of necessary AWS services in this project.
* The staging area of the intermediate data in the project is user-created Spark DataFrames, which are some in-memory data structures similar to the [ones](https://pandas.pydata.org/) we used in the 1st project. 
* Because Apache Spark is an in-memory datastore in essence, the way we handle the processed final data is also different in this project. In the previous projects, the processed final data is stored in database tables; but in this project, we must save the data as [parquet files](https://parquet.apache.org/) to a safe location (in this project, it would be a S3 bucket created in advance).

Due to these differences, the ETL pipelines of this project will be placed in AWS environments, and the Udacity workspace will be used to initiate these pipelines remotely.

<br>

The way how Apache Spark works to wrangle datasets shows some track records of growth that can be found in other existing solutions. For example, 

* The resemblance between Pandas DataFrames and SQL tables. Usually you can manipulate your in-memory data in a style that is equivalent to a SQL query, as long as you can think the problem in the SQL way.
* The functionality of various SQL databases to query external flat files.
    - External tables in Oracle Database ([example](https://schneide.blog/2020/06/16/using-csv-data-as-external-table-in-oracle-db/))
    - Linked server in Microsoft SQL Server ([example](https://jayanthkurup.com/adding-csv-files-as-linked-server-connection/))
    - CSV storage engine in MySQL Database (examples [here](https://fromdual.com/csv-storage-engine) and [here](https://sqlexpertz.wordpress.com/2013/11/20/mysql-create-external-table-using-csv-engine-and-file/))

The good news is, Apache Spark has the pros of the both sides, and it improved them even further. We can use Spark SQL, the SQL-like programming language native to the Spark engine, to query the DataFrames directly and intuitively, and we can also load raw flat files in many different formats into Spark DataFrames without doing any transformation. To put it simply: with Apache Spark, we can do many of the same tasks done by Pandas, but now at a greater scale; we can also do many of the same tasks done by SQL databases and data warehouses, but now with the flexibility of keep the original flat files intact and readily available any time.

But then, here's the drawback. Because Spark DataFrames are not real SQL database tables, there will be no table constraint that can be declared and enforced (not only PRIMARY KEY, but also NOT NULL will become unavailable in this project). Because Spark SQL is just a SQL-like language, there will be no IDENTITY column and MERGE statement that can be issued in the SQL queries. The situation is similar to the 3rd project, but the difficulty of the data wrangling process is higher. These problems should be thoroughly considered before trying to implement Apache Spark as the main datastore in the environment.

<br>

To give the project a test run, open a new launcher in the project workspace, click on the "Terminal" icon to start a new terminal session. Under the directory path /home/workspace , first we will need to supply the *dl.cfg* file with a valid AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY value. Remember that there is no need to enclose these values with quotes, and if you do enclose these values with quotes (either '' or ""), it will be treated as invalid credentials by AWS.

Then we can proceed with processing data with Apache Spark (an Amazon EMR cluster in this project) by running the *etl.py* script.

    $ python3 etl.py

The most time-consuming part when testing this script is the steps to write parquet files to a S3 bucket. And this is also where you are likely to exhaust your AWS Free Tier quotas. Be careful and only try to load the entire dataset when it is necessary.