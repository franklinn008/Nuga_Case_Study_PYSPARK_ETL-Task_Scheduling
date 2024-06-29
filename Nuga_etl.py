#Installing pyspark using terminal

#Importing Libraries
import pandas as pd
from pyspark.sql import*
from pyspark.sql.functions import*
from pyspark.sql import SparkSession
import os
import psycopg2

#Java was downloaded same with postgres jdbc file
#Setting Java Environment
os.environ['JAVA_HOME'] = r'C:\java8'
#Initializing my spark Session
spark = SparkSession.builder\
        .appName("Nuga Bank")\
        .config("spark.jars","postgresql-42.7.3.jar") \
        .getOrCreate()
spark
#Extracting the data into spark
df = spark.read.csv(r'dataset\rawdata\nuga_bank_transactions.csv', header=True, inferSchema=True)
df.show()
df.printSchema()
#Cleaning and Transformation
#Checking for null
for column in df.columns:
    print(column, 'Nulls: ', df.filter(df[column].isNull()).count())

#Checking summary statistics
df.describe().show()
#Filling up Nulls
df1 = df.fillna({
    'Customer_Name': 'Unknown',
    'Customer_Address': 'Unknown',
    'Customer_City': 'Unknown',
    'Customer_State': 'Unknown',
    'Customer_Country': 'Unknown',
    'Company': 'Unknown',
    'Job_Title': 'Unknown',
    'Email': 'Unknown',
    'Phone_Number': 'Unknown',
    'Credit_Card_Number': 0,
    'IBAN': 'Unknown',
    'Currency_Code': 'Unknown',
    'Random_Number': 0.0,
    'Category': 'Unknown',
    'Group': 'Unknown',
    'Is_Active': 'Unknown',
    'Description': 'Unknown',
    'Gender': 'Unknown',
    'Marital_Status': 'Unknown',
     })

#Dropping missing values for Last_Updated
df1 = df1.na.drop(subset=['Last_Updated'])
#ReChecking for null
for column in df1.columns:
    print(column, 'Nulls: ', df1.filter(df1[column].isNull()).count())

#Creating the tables,dataframes
#transaction table
transactions = df1.select('Transaction_Date',  'Amount','Transaction_Type')\
                  .withColumn('Transaction_ID', monotonically_increasing_id())\
                  .select('Transaction_ID','Transaction_Date',  'Amount','Transaction_Type')
transactions.show(5)
#customers table
customers = df1.select('Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country')\
                  .withColumn('Customer_ID', monotonically_increasing_id())\
                  .select('Customer_ID','Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country')
customers.show(5)
#employees table
employees = df1.select('Company','Job_Title','Email','Phone_Number','Gender','Marital_Status')\
                  .withColumn('Employee_ID', monotonically_increasing_id())\
                  .select('Employee_ID','Company','Job_Title','Email','Phone_Number','Gender','Marital_Status')
employees.show(5)
#nuga_bank_fact
fact_table = df1.join(transactions,['Transaction_Date',  'Amount','Transaction_Type'],'inner')\
                .join(customers,['Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country'])\
                .join(employees,['Company','Job_Title','Email','Phone_Number','Gender','Marital_Status'])\
                .select('Transaction_ID','Customer_ID','Employee_ID','Credit_Card_Number','IBAN','Currency_Code','Random_Number',\
                        'Category','Group','Is_Active','Last_Updated','Description')
fact_table.show(5)
#Loading into Postgre SQL
#Creating a function
def get_db_connection():
    connection = psycopg2.connect (
        user = 'postgres',
        password = 'u0987',
        host = 'localhost',
        port = '',
        database = 'Nuga_Bank_PySpark'
    )
    return connection
#connecting to sql database
conn = get_db_connection()
transactions.printSchema()
#Creating a function to create tables
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
                        DROP TABLE IF EXISTS transactions;
                        DROP TABLE IF EXISTS customers;
                        DROP TABLE IF EXISTS employees;
                        DROP TABLE IF EXISTS fact_table;

                        CREATE TABLE transactions (
                        Transaction_ID BIGINT ,
                        Transaction_Date DATE,
                        Amount FLOAT,
                        Transaction_Type VARCHAR (10000)
                        );
                        
                        
                        CREATE TABLE customers (
                        Customer_ID BIGINT ,
                        Customer_Name VARCHAR (10000),
                        Customer_Address VARCHAR (10000),
                        Customer_City VARCHAR (10000),
                        Customer_State VARCHAR (10000),
                        Customer_Country VARCHAR (10000)
                        );

                        CREATE TABLE employees (
                        Employee_ID BIGINT ,
                        Company VARCHAR (10000),
                        Job_Title VARCHAR (10000),
                        Email VARCHAR (10000),
                        Phone_Number VARCHAR (10000),
                        Gender VARCHAR (10000),
                        Marital_Status VARCHAR (10000)
                        );

                        CREATE TABLE fact_table (
                        Transaction_ID BIGINT ,
                        Customer_ID BIGINT ,
                        Employee_ID BIGINT ,
                        Credit_Card_Number BIGINT ,
                        IBAN VARCHAR(1000) ,
                        Currency_Code VARCHAR(10000),
                        Random_Number FLOAT ,
                        Category VARCHAR(10000),
                        "Group" VARCHAR(10000),
                        Is_Active VARCHAR(10000),
                        Last_Updated DATE,
                        Description VARCHAR(10000)
                        );

                        
                        '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
create_table()
#building url for loading the data
url = "jdbc:postgresql://localhost:5432/Nuga_Bank_PySpark"
properties = {
    "user": "postgres",
    "password": "",
    "driver": "org.postgresql.Driver"
}
customers.write.jdbc(url=url,table="customers", mode="append",properties = properties)
employees.write.jdbc(url=url,table="employees", mode="append",properties = properties)
transactions.write.jdbc(url=url,table="transactions", mode="append",properties = properties)


fact_table.write.jdbc(url=url,table="fact_table", mode="append",properties = properties)
print('DataBase and Table loaded succesfully!!!')
#Automating the process using.py