import os
import lasio as ls
import numpy as np
import mysql.connector

host,user,passwd,database =localhost,user,your_pasword,name_database

# Create Database
connection = mysql.connector.connect(host=host,user=user,passwd=passwd)
cursor = connection.cursor()
cursor.execute("CREATE DATABASE "+database)

# Create Table
connection = mysql.connector.connect(host=host,user=user,passwd=passwd,database=database)
cursor = connection.cursor()

cursor.execute("""
CREATE TABLE Basic_Information (
  NAME VARCHAR(40),
  CODE VARCHAR(100),
  COORDINATE VARCHAR(40),
  COMPANY VARCHAR(40),
  FIELD VARCHAR(40),
  STATE VARCHAR(40),
  START_DEPTH INT,
  STOP_DEPTH INT,
  STEP_DEPTH INT
  );
 """)

# Read LAS files and Insert The Values

with os.scandir(your_files_loaction) as f:
    a = 0
    for file in f:
        nama = file.name
        print(nama)
        data = ls.read(your_files_location+'\\'+nama)
        well_information = data.sections['Well']
        cursor.execute("""
        INSERT INTO Basic_Information (NAME, CODE, COORDINATE, COMPANY, FIELD, STATE, START_DEPTH, STOP_DEPTH, STEP_DEPTH)
        VALUES ('%s','%s','%s','%s','%s','%s', %s, %s, %s);""" % (nama,str(well_information['WELL'].value),str(well_information['LOC'].value),str(well_information['COMP'].value),str(well_information['FLD'].value),str(well_information['STAT'].value),well_information['STRT'].value,well_information['STOP'].value,well_information['STEP'].value))

        cursor.execute("""
        CREATE TABLE Welllog"""+str(a)+""" (
          DEPTH INT,
          GR INT,
          ILD INT,
          SPR INT,
          ASN INT
          );
         """)

        dataframe = data.df()
        dataframe = dataframe.replace(-999.2500,np.nan).dropna()

        gr,ild,spr,asn =  [],[],[],[]

        for l in dataframe :
            dataframeku = dataframe[l]
            for k in dataframeku.index:

                if l == "GR" or l == "GRN" or l == "GRD" or l == "GRR":
                    gr.append(dataframeku[k])

                elif l == "SPR":
                    spr.append(dataframeku[k])

                elif l == "ASN":
                    asn.append(dataframeku[k])

                elif l == "ILD":
                    ild.append(dataframeku[k])

                else:
                    pass

        dept = np.array(dataframe.index)
        print(len(dept),len(gr),len(ild),len(spr),len(asn))

        if len(gr) == 0 :
            gr = np.zeros(len(dept))
        if len(ild) == 0 :
            ild = np.zeros(len(dept))
        if len(spr) == 0 :
            spr = np.zeros(len(dept))
        if len(asn) == 0 :
            asn = np.zeros(len(dept))

        for m in range(len(dept)):
            cursor.execute("""
            INSERT INTO Welllog"""+str(a)+""" (DEPTH, GR, ILD, SPR, ASN)
            VALUES
            (%s, %s, %s, %s, %s);""" % (dept[m], gr[m], ild[m], spr[m], asn[m]))

        a = a + 1
