import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import csv
import psycopg2
import airflow
import requests
import os

#def print_world():
#    print('world')

def LoadFile_1():
  file1_url = "http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2MassAssignUsersSample_.csv"
  input1 = requests.get(file1_url, stream = True)
  output1 = open("/home/baadmin/airflow_ishi/dags/input/file1.csv",'a')
  for line in input1:
    content = ''.join(chr(x) for x in line)
    c=content.rstrip()
    print(c, file=output1)

def LoadFile_2():
  file2_url = "http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2ImportFamRelSample.csv"
  input2 = requests.get(file2_url, stream = True)
  output2 = open("/home/baadmin/airflow_ishi/dags/input/file2.csv",'a')
  for line in input2:
    content = ''.join(chr(x) for x in line)
    c=content.rstrip()
    print(c, file=output2)

def ConnectDb_1():
  conn = psycopg2.connect("host='localhost' port='5432' dbname='sangeedb' user='postgres' password='Password!234'")
  cur = conn.cursor()
  f=open("/home/baadmin/airflow_ishi/dags/input/file1.csv", 'r')
  #copy_stmt = """COPY {} FROM STDIN WITH CSV {} QUOTE AS '"' """.format('csvtable1',('"UserCode"','"GroupCode"'))
  cur.copy_from(f, 'csvtable1', sep=',',columns=('"UserCode"','"GroupCode"'))
  cur.execute(""" Delete from csvtable1 where "UserCode" = 'UserCode' """)
  #cur.copy_expert("COPY csvtable1 TO STDOUT WITH CSV HEADER", sys.stdout)
  cur = conn.cursor()
  conn.commit()
  conn.close()

def ConnectDb_2():
  conn = psycopg2.connect("host='localhost' port='5432' dbname='sangeedb' user='postgres' password='Password!234'")
  cur = conn.cursor()
  f=open("/home/baadmin/airflow_ishi/dags/input/file2.csv", 'r')
  cur.copy_from(f, 'csvtable2', sep=',',columns=('"Parent Identifier"','"Student Identifier"'))
  cur.execute(""" Delete from csvtable2 where "Parent Identifier" = 'Parent Identifier' """)
  #cur.copy_expert("COPY csvtable2 TO STDOUT WITH CSV HEADER", sys.stdout)
  cur = conn.cursor()
  conn.commit()
  conn.close()

def RemoveCsvFile():
  os.remove("/home/baadmin/airflow_ishi/dags/input/file1.csv")
  os.remove("/home/baadmin/airflow_ishi/dags/input/file2.csv")

def MergeTable():
  conn = psycopg2.connect("host='localhost' port='5432' dbname='sangeedb' user='postgres' password='Password!234'")
  cur = conn.cursor()
  f=open("/home/baadmin/december6/input/merge.csv", 'w')
  cur.execute("""copy mergetable to  '/home/baadmin/airflow_ishi/dags/input/merge.csv' delimiter ',' csv header""")
  #cur.copy_to(f, mergetable, sep='\t')
  cur = conn.cursor()
  conn.commit()
  conn.close()

default_args = {
    'owner': 'ishi_aa',
    'start_date': datetime.datetime(2018, 12, 9),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

with DAG('1_FileToDb',
         default_args=default_args,
         ) as dag:

    
    csvfile1 = PythonOperator(task_id='csvfile1',
                                 python_callable= LoadFile_1)

    csvfile2 = PythonOperator(task_id='csvfile2',
                                 python_callable= LoadFile_2)

    db1 = PythonOperator(task_id='connectdb1',
                                 python_callable= ConnectDb_1)
    
    db2 = PythonOperator(task_id='connectdb2',
                                 python_callable= ConnectDb_2)
     
    remove= PythonOperator(task_id='removecsv',
                                 python_callable= RemoveCsvFile)
    
    merge = PythonOperator(task_id='merge',
                                 python_callable= MergeTable)

csvfile1 >> csvfile2 >> db1 >> db2 >> remove >> merge
