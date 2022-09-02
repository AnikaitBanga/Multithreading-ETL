import mysql.connector
import random
import pandas as pd
import csv
import pymysql
import time
from concurrent.futures import ThreadPoolExecutor

l=[5000,10000,15000,20000,25000]
mydb = mysql.connector.connect(
host="localhost",
user="root",
password="alaskayoung",
database="MYDB"
)

#CASE1

df1 = pd.DataFrame()
mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE student1(id INT PRIMARY KEY, name VARCHAR(255), age INT, address VARCHAR(255), gender CHAR);")
query = " create table studentc1 as select * from student1 where 1=2;"
mycursor.execute(query)
for j in range(len(l)):
    for i in range(0,l[j]):
        query = "insert into student1 values(" + str(i) + ',' + " 'name" + str(i) + "'," + str(random.randint(3, 20)) + ',' + ' "Delhi" ' + ',' + ' 0 ' ");"
        mycursor.execute(query)
    mydb.commit()
    start=time.time()
    query="INSERT INTO studentc1 (id,name, age, address,gender) SELECT id,upper(name), age+1, upper(address),not gender FROM student1"
    mycursor.execute(query)
    mydb.commit()
    end=time.time()
    diff1=end-start
    df1.loc[j, 0] = diff1
    mycursor.execute("set sql_safe_updates=0;")
    mycursor.execute("delete from student1 where 1=1;")
    mycursor.execute("delete from studentc1 where 1=1;")
mydb.commit()
df1.to_csv('analysis.csv', index=False)
print("CASE 1 DONE")

#CASE 2

df=pd.read_csv("analysis.csv")
conn = pymysql.connect(host="localhost", user='root', password='alaskayoung', database='MYDB')
cursor = conn.cursor()
cursor.execute("CREATE TABLE student2(id INT PRIMARY KEY, name VARCHAR(255), age INT, address VARCHAR(255), gender CHAR);")
query = " create table studentc2 as select * from student2 where 1=2; "
cursor.execute(query)
for j in range(len(l)):
     for i in range(0,l[j]):
        query = "insert into student2 values(" + str(i) + ',' + " 'name" + str(i) + "'," + str(random.randint(3, 20)) + ',' + ' "Delhi" ' + ',' + ' 0 ' ");"
        cursor.execute(query)
     mydb.commit()
     start=time.time()
     query = 'select * from student2'
     results = pd.read_sql_query(query, conn)
     results.to_csv("output.csv", index=False)
     data = pd.read_csv("output.csv")
     data["name"] = data["name"].str.upper()
     data["age"] = data["age"]+1
     data["address"] = data["address"].str.upper()
     data['gender'] = data['gender'].map({0:1,1: 0})
     data.to_csv("output.csv", index=False)
     df1 = pd.DataFrame(data, columns= ["id","name","age", "address","gender"])
     for row in df1.itertuples():
        query1 ="insert into studentc2 values(" + str(row.id) + ',' + '"' + str(row.name) + '"' + ',' + str(row.age) + ',' + '"' + str(row.address) + '"' + ',' + str(row.gender) + ");"
        cursor.execute(query1)
     conn.commit()
     end=time.time()
     diff1=end-start
     df.loc[j, 1] = diff1
     cursor.execute("delete from student2 where 1=1;")
     cursor.execute("delete from studentc2 where 1=1;")
df.to_csv('analysis.csv', index=False)
print("CASE 2 DONE")

#CASE3

df=pd.read_csv("analysis.csv")
conn = pymysql.connect(host="localhost", user='root', password='alaskayoung', database='MYDB')
cursor = conn.cursor()

def load(file):
    data = pd.read_csv(file)
    df = pd.DataFrame(data, columns=["id", "name", "age", "address", "gender"])
    for row in df.itertuples():
        query1 = "insert into studentc3 values(" + str(row.id) + ',' + '"' + str(row.name) + '"' + ',' + str(
            row.age) + ',' + '"' + str(row.address) + '"' + ',' + str(row.gender) + ");"
        cursor = conn.cursor()
        cursor.execute(query1)
    conn.commit()

def transform(file):
     data = pd.read_csv(file)
     data["name"] = data["name"].str.upper()
     data["age"] = data["age"]+1
     data["address"] = data["address"].str.upper()
     data['gender'] = data['gender'].map({0:1,1: 0})
     data.to_csv(file, index=False)
     load(file)

def extract(x,j):
    conn = pymysql.connect(host="localhost", user='root', password='alaskayoung', database='MYDB')
    query = 'select * from student3 limit ' + str(int(x)) + ', ' + str(int(j-x)) + ';'
    results = pd.read_sql(query, conn)
    results.to_csv("output" + str(int(x)) + str(int(j)) + ".csv", index=False)
    transform("output" + str(int(x)) + str(int(j)) + ".csv")

if __name__ == "__main__":
    i=-1
    for j in l:
        x=0
        conn = pymysql.connect(host="localhost", user='root', password='alaskayoung', database='MYDB')
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE student3 (id INT PRIMARY KEY, name VARCHAR(255), age INT, address VARCHAR(255), gender CHAR);")
        for ix in range(0, j):
             query = "insert into student3 values(" + str(ix) + ',' + " 'name" + str(ix) + "'," + str(
                  random.randint(1, 100)) + ',' + ' "Delhi" ' + ',' + ' 0 ' ");"
             cursor.execute(query)
        conn.commit()
        query = " create table studentc3 as select * from student3 where 1=2; "
        cursor.execute(query)
        start = time.time()
        n=5
        value1=[x,x+(j/n),x+2*(j/n),x+3*(j/n),x+4*(j/n)]
        value2=[j/n,2*(j/n),3*(j/n),4*(j/n),5*(j/n)]
        with ThreadPoolExecutor(n) as exe:
            exe.map(extract, value1, value2)
        end = time.time()
        diff1=end-start
        i = i+1
        df.loc[i, 2] = diff1
        conn.ping(reconnect=True)
        cursor.execute("drop table student3;")
        cursor.execute("drop table studentc3;")
    df.to_csv('analysis.csv', index=False)
    print("CASE 3 DONE")
