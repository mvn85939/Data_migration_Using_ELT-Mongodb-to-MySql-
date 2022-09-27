from concurrent.futures import thread
from unicodedata import name
from matplotlib import collections
from pymongo import MongoClient
import asyncio
import aiomysql
import pprint
import json
import csv
import sys
import gridfs
import pandas as pd
import time
import threading
from threading import *
import warnings
warnings.filterwarnings("ignore")
import logging

maxint=sys.maxsize
while True:
    try:
        csv.field_size_limit(maxint)
        break
    except OverflowError:
        maxint=int(maxint/10)
#logger for mongodb
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s: %(asctime)s : %(message)s", datefmt="%d-%m-%Y %H:%M",)
file_handler = logging.FileHandler("mongodb.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

'''Implementation of mongodb class'''
#start = time.process_time()

class mongodb:
    
    '''constructor function'''

    def __init__(self,hostm,port_number):
        self.client=None
        
        self.hostm=hostm
        self.port_number=port_number
        
    
    '''initializing mongodb'''
    def initialize(self,hostm,port_number):
        try:
            self.client = MongoClient(hostm,port_number)
            '''database:activity'''
            #db=client[cred['database']]
            '''collection:data'''
            #coll=db[cred['collection']]
            print("MongoDB connected successfully!")
            
        except ConnectionError:
            print( " MongoDB Connection failed!!!")

    '''connection to mongodb'''
    def connect(self,hostm,port_number):
        try:
            mongodb.initialize(self,hostm,port_number)
            return True, "MongoDB connected successfully!"
        
        except Exception:
            print("Could not connect to MongoDB!!!")
     
    '''loading file into mongodb'''
    
    async def read_file(self,hostm,port_number,coll):
            
            client=MongoClient(hostm,port_number)
            db=client.activity
            col=db.data
            if coll in db.list_collection_names():
                db.drop_collection(coll)
                new_coll=db.create_collection(coll)
            print('Created collection {0}!'.format(new_coll))
          
        
            with open("50k_sales.csv", "r") as f:
                file=csv.DictReader(f,delimiter=',')
                ndata=[]
                try:
                    for row in file:
                        ndata.append(row)
                    result=await db.data.insert_many(ndata)
                    print('result %s' % repr(result.inserted_id))
                except Exception:
                    print('Exception Occured')
            logger.info("Successfully loaded data into MongoDB where Database is {} and Collection is {}".format(db, col))
            print('Inserted data into collection in mongodb!')
            
            

    def cred(self,hostm,port_number):
        client=MongoClient(hostm,port_number)
        db=client.activity

   
    def crud(query1,query2,query3,query4):
        client=MongoClient('mongodb://localhost:27017')
        db=client.activity
            #create
        insert=db.data.insert_one(query1)
        print('Inserted 1 document',insert)
         
            #read
        read=db.data.find(query3).limit(2) #cursor
        for record in read:
                print(record)

            #update
        try:
            update=db.data.update_one(query2,upsert=False)
            for res in update:
                print(res)
        except TypeError:
            print('Updated 1 document')
        
            #delete
        delete=db.data.delete_one(query4)
        print('Deleted 1 document!')



    def create_doc(query1):
        client=MongoClient('mongodb://localhost:27017')
        db=client.activity
            #create
        insert=db.data.insert_one(query1)
        print('Inserted 1 document')
        return insert
    
    def read_doc(query3):
        client=MongoClient('mongodb://localhost:27017')
        db=client.activity
        read=db.data.find(query3).limit(2) #cursor
        for record in read:
                print(record)
    
    def update_doc(query2):
        client=MongoClient('mongodb://localhost:27017')
        db=client.activity

        try:
            update=db.data.update_one(query2,upsert=False)
            for res in update:
                print(res)
        except :
            print('Could not update document!')
    
    def delete_doc(query5):
        client=MongoClient('mongodb://localhost:27017')
        db=client.activity
        delete=db.data.delete_one(query4)
        print('Deleted 1 document!')


#mongodb().initialize()
#mongodb().connect()
'''Read the file and load it to mongodb'''
#mongodb.read_file()

'''CRUD Operations'''
#insert one document
query1={"STA":"10002","Date":"1942-7-20","Precip":"0","WindGustSpd":"50","MaxTemp":"40.11111"}

#update a document
query2={"Date":"1942-7-20"},{"$set":{"Date":"2022-8-02"}}

#find one document
query3={"MaxTemp":{"$gt":"28.33333333"}}

#delete one document
query4={"Date":"2022-8-02"}

#calling crud function in mongodb class
#mongodb().crud(query1,query2,query3,query4)

#mongodb.crud({"MaxTemp":"28.33333333"})
            

'''------------------------------------------------------------------------------------------'''
import mysql.connector
import pandas as pd
import mysql.connector.pooling
from mysql.connector import errors
import time


class Mysqlconn:
    start=time.time()
    
    def __init__(self,credentials):
        self.connection=None
        self.credentials=credentials
        self.host = credentials['host']
        self.user = credentials['user']
        self.password = credentials['password']
        self.db_name = credentials['database']
    
    #@staticmethod takes only 1 parameter
    #this is an instance method since self parameter is used
    def initialize(self,credentials):
        try:
            print("Connecting to MySQL server...")
            config = {
            'host' : credentials['host'],
            'user' : credentials['user'],
            'password' : credentials['password'],
            'database' : credentials['database']
            }
            self.connection = mysql.connector.connect(**config)
            print("MySQL Server connected successfully!!!")
        except ConnectionError as e:
            print('Connection Error! mysql not connected')
   
            
    async def create_db(self,credentials):
        #Mysqlconn.initialize(self,credentials)
        
        config = {
            'host' : credentials['host'],
            'user' : credentials['user'],
            'password' : credentials['password']
        }
        cxn= mysql.connector.connect(**config)
        cursor=cxn.cursor()
        

        '''Drop database if already exists'''
        cursor.execute("DROP DATABASE IF EXISTS mydb")

        '''Creating a databse'''
        cursor.execute("CREATE DATABASE mydb")

        #connect to database mydb
        config = {
            'host' : credentials['host'],
            'user' : credentials['user'],
            'password' : credentials['password'],
            'database' : credentials['database']
            } 
        connection= mysql.connector.connect(**config)
        cursor1=connection.cursor()
        

        cursor1=connection.cursor()
        connection.commit()

        cursor1.execute("SHOW TABLES")
        tables= cursor1.fetchall()
        ## showing all the tables one by one
        for table in tables:
                print(table)
    
        '''Dropping table if already exists in database'''
        cursor1.execute("DROP TABLE IF EXISTS ndata")
        

    
    
    async   def create_table(self,hostm,port_number,credentials):
        client=MongoClient(hostm,port_number)
        db=client['activity']
        coll=db['data']
        
        mongo_docs = coll.find({},{'_id':0})
        fieldnames = list(mongo_docs[0].keys()) #gives all the columns in the dataset
        
        fieldnames=list(map(lambda x:x.replace(' ','_'),fieldnames))
        print(fieldnames)

        config = {
            'host' : credentials['host'],
            'user' : credentials['user'],
            'password' : credentials['password'],
            'database' : credentials['database']
            } 
        connection= mysql.connector.connect(**config)
        cursor1=connection.cursor()
        connection.commit()
        cursor1.execute("DROP TABLE IF EXISTS ndata")
        
        
        count=len(fieldnames)
        col_namestypes=['VARCHAR(255)']*count

        for i in range(len(fieldnames)):
                if 0==i:
                        cursor1.execute("CREATE TABLE IF NOT EXISTS ndata({0} {1})".format(fieldnames[i],col_namestypes[i]))
                
                else:
                        cursor1.execute("ALTER TABLE ndata ADD {0} {1}".format(fieldnames[i],col_namestypes[i]))
        
        print('Created table ndata in mydb')
        
        cursor1.execute("Show COLUMNS FROM ndata")
        

    async def insert(self,hostm,port_number,credentials):
        #mongodb collection--->Mysql table
            client=MongoClient(hostm,port_number)
            db=client['activity']
            coll=db['data']
            mongo_docs = coll.find({},{'_id':0})
            fieldnames = list(mongo_docs[0].keys()) 
           
            fieldnames=list(map(lambda x:x.replace(' ','_'),fieldnames))
            
        
            values=[]
            for record in mongo_docs:
                rows=list(record.values())
                values.append(rows)
        
            config = {
            'host' : credentials['host'],
            'user' : credentials['user'],
            'password' : credentials['password'],
            'database' : credentials['database']
            } 
            connection=mysql.connector.connect(**config)
            cursor1=connection.cursor()
            connection.commit()
        
            count=len(fieldnames)
            qmark=['%s']*count
        
            q=','.join(qmark)
        
            query = f"INSERT INTO ndata VALUES ({q})"
            
            self.host=credentials['host']
            self.user=credentials['user']
            self.password=credentials['password']
            self.db_name=credentials['database']
            
            
        
            try:
                print('Inserting values......' )
                pool = mysql.connector.pooling.MySQLConnectionPool(     
                host=self.host,
                port=3306,
                user=self.user,
                password=self.password,
                pool_name="mysql_pool",
                pool_size=3,
                autocommit=False)

                conn = pool.get_connection()
                #print('pooling connection id: ',conn.connection_id)
                
                
                cursor2=conn.cursor()
                use_database = "USE {}".format(self.db_name)
                cursor2.execute(use_database)
                cursor2.executemany(query, values)
        
                conn.commit()
                conn.close()
                connection.close()
                print('Inserted values into mysql table')
            except errors.PoolError as e:
                print(e)
                conn.close()

                print('An exception occured!')
    end=time.time()
    res=end-start
    final_time=res/60 #in minutes
    print('Time taken: {0} minutes'.format(final_time))



#main function
if __name__=="__main__":
    start = time.process_time()
    st=time.time()

    #mongodb credentials
    hostm=input("Enter the  MongoDB host name: ")
    port_number=int(input("Enter the port_number: "))
    coll=input('Enter the collection name: ')
    print('Mongodb credentials: ',hostm,port_number,coll)
    
    #mysql credentials
    host=input("Enter the  MySQL host name: ")
    user=input("Enter the user name: ")
    password=input("Enter the password: ")
    database=input("Enter the database name: ")
    credentials={"host":host,"user":user,"password":password,"database":database}
    print('Credentials: ',credentials)
    

    
    cred=(hostm,port_number)
    print(cred)
    a=mongodb(hostm,port_number)
    print(a.initialize(hostm,port_number))
    b=mongodb(hostm,port_number)
    print('Collection :',coll)
    try:
        
        threading.Thread(target=asyncio.run(b.read_file(hostm,port_number,coll)),name='Thread1')
    except:
          print('Unable to thread!')
    
    
    x=Mysqlconn(credentials)
    print(x.initialize(credentials))
    
    z=Mysqlconn(credentials)
    
    asyncio.run(z.create_db(credentials))

    tb=Mysqlconn(credentials)
    ins=Mysqlconn(credentials)
    
    try:
        threading.Thread(target=asyncio.run(tb.create_table(hostm,port_number,credentials)),name='Thread-2')
        threading.Thread(target=asyncio.run(ins.insert(hostm,port_number,credentials)),name='Thread-3')
    except:
        print('Unable to thread!')
    Mysqlconn(credentials)
    ed=time.time()
    t=(ed-st)/60
    print('Time taken (main) : {0} minutes'.format(t))
    print('CPU Execution Time: {0} s'.format(time.process_time() - start))
