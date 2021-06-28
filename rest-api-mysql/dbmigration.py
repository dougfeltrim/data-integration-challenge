# -*- coding: utf-8 -*-
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error
import pymysql
import sqlalchemy
from sqlalchemy import create_engine

#insert your file names here

files = ['q1_catalog.csv', 'q2_clientData.csv']

for f in files:
    data = pd.read_csv(f ,index_col=False, sep=';', quotechar='"')
    # print(data.head())
    # print(data.dtypes)
    try:
        conn = msql.connect(host='localhost', user='root',  
                            password='')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE dbtest")
            print("dbtest database is created")
    except Error as e:
        print("Error while connecting to MySQL", e)
    try:
        conn = msql.connect(host='localhost', database='dbtest', user='root', password='')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            # cursor.execute('DROP TABLE IF EXISTS customer;')
            print('Creating table....')
            # name;addresszip;website
            cursor.execute("CREATE TABLE customer (name CHAR(100) NOT NULL, addresszip CHAR(100), website CHAR(200))")
            print("customer table is created....")
            for i,row in data.iterrows():
                # print(i,row)
                sql = "INSERT INTO customer VALUES (%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                print(row)
                print("Record inserted")
                # the connection is not autocommitted by default, so we must commit to save our changes
                conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)

    # Execute query
    sql = "SELECT * FROM customer"
    cursor.execute(sql)

    # Fetch all the records
    result = cursor.fetchall()
    for i in result:
        print(i)


    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"  
                        .format(user="root", pw="", 
                        db="dbtest"))
    # Insert whole DataFrame into MySQL
    data.to_sql('customer', con = engine, if_exists = 'append', chunksize = 1000, 
                index=False)

    # Execute query
    sql = "SELECT * FROM dbtest.customer"
    cursor.execute(sql)
    # Fetch all the records
    result = cursor.fetchall()
    for i in result:
        print(i)

    # Close the connection
    if (conn.is_connected()):
        cursor.close()
        conn.close()
        print("MySQL connection is closed")