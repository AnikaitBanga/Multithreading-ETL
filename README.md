# Multithreading-ETL
In this project, I have done a comparison of different techniques for tranforming a large dataset and compared the time taken in each technique.

For this, I have created a table in MySQL, extracted the data from it, Tranformed it and then Loaded it again in the table. This ETL process has been implemented in 3 cases (discussed below) and I have analyzed the amount of time it takes to run the 3 cases. The above process has been done for different number of rows in the table i.e. 5000 rows, 10,000 rows, 15,0000 rows, 20,000 rows and 25,000 rows.

Database name: mydb

Table Column names: id, name, age, address, gender

Transformations applied on all records:

1.Converted name to uppercase
2.Added 1 to original age
3.Converted address to uppercase
4.Complemented the gender
CASE 0:

1.Extracted data from table 'student1'
2.Tranformed the data while Loading it to table 'studentc1'
CASE 1:

1.Extracted data from table 'student2' to a file 'output.csv'
2.Applied the transformations on the file.
3.Loaded the data from the file to the table 'studentc2'
CASE 2:

1.Extracted data from table 'student3' into a number of small files (each file contains a subset of the whole data)
2.Applied the transformations on all the files simultaneously using multithreading (I have taken 5 threads here)
3.Loaded the data from the files to the table 'studentc3'.For the process of Multithreading, I have used the ThreadPoolExecutor class available in python.
For all the above 3 cases and all the number of records, I have noted the time in 'analysis.csv' file.
