import csv
import requests
import psycopg2
import os
from urllib import request

def LoadFile_1():
  file1_url = "http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2MassAssignUsersSample_.csv"
  input1 = requests.get(file1_url, stream = True)
  output1 = open("/home/baadmin/december6/input/file1.csv",'a')
  for line in input1:
    content = ''.join(chr(x) for x in line)
    c=content.rstrip()
    print(c, file=output1)

def LoadFile_2():
  file2_url = "http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2ImportFamRelSample.csv"
  input2 = requests.get(file2_url, stream = True)
  output2 = open("/home/baadmin/december6/input/file2.csv",'a')
  for line in input2:
    content = ''.join(chr(x) for x in line)
    c=content.rstrip()
    print(c, file=output2)

def ConnectDb_1():
  conn = psycopg2.connect("host='localhost' port='5432' dbname='sangeedb' user='postgres' password='Password!234'")
  cur = conn.cursor()
  f=open("/home/baadmin/december6/input/file1.csv", 'r')
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
  f=open("/home/baadmin/december6/input/file2.csv", 'r')
  cur.copy_from(f, 'csvtable2', sep=',',columns=('"Parent Identifier"','"Student Identifier"'))
  cur.execute(""" Delete from csvtable2 where "Parent Identifier" = 'Parent Identifier' """)
  #cur.copy_expert("COPY csvtable2 TO STDOUT WITH CSV HEADER", sys.stdout)
  cur = conn.cursor()
  conn.commit()
  conn.close()

def RemoveCsvFile():
  os.remove("/home/baadmin/december6/input/file1.csv")
  os.remove("/home/baadmin/december6/input/file2.csv")
  
LoadFile_1()
LoadFile_2()
ConnectDb_1()
ConnectDb_2()
RemoveCsvFile()

















          
