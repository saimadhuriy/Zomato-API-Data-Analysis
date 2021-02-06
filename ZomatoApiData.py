import requests as rq
import json as js
import time
import mysql.connector
from mysql.connector import Error
from dateutil import parser
import pandas as pd

ip_host   = 'localhost'
ip_user   = 'newuser'
ip_pass   = 'Masterdb@1'
ip_dbname = 'masterdb'



def connect(ip_city, ip_state, ip_district):
    
    #    print("*************************************************")
    #    print("************Method to insert the data on MySQL***")
    #    print("connect to MySQL database and insert twitter data")
    #    print("*************************************************")
        
        
    try :
            con = mysql.connector.connect(host=ip_host,
            database=ip_dbname, user=ip_user, password = ip_pass, charset = 'utf8mb4')

            if con.is_connected():
                    
                    #   print("*********************")
                    #    print("Connection Sucessful*")
                    #    print("*********************")
                        

                    cursor = con.cursor()

                    query = "INSERT INTO ApiData(city,state,district) VALUES (%s, %s, %s)"
                    cursor.execute(query, (ip_city, ip_state, ip_district))
                    con.commit()
                    cursor.close()
                    con.close()

    except Error as e:
            print("*****************************************************")
            print("*****Unknow Error while connecting the DB***" + str(e))
            print("*****************************************************")
    return

# Common class to get the API data to data python field
def get_data(api_key,url):
    response = rq.request("GET", url, headers=headers)
    data = response.text
    #PublicApi_listener(data)
    print(data)
    
# Public API listener to get the requested data
def PublicApi_listener(data):

        try :
                raw_data = js.loads(data)
                for in_text in raw_data:
                        ip_city         = in_text['City']
                        ip_state        = in_text['State']
                        ip_district     = in_text['District']
                        connect(ip_city,ip_state,ip_district)
        except Error as e:
                print("*******************************************************")
                print("***** Something wrong on_data ****************" + str(e))
                print("*******************************************************")

 
url = "https://developers.zomato.com/api/v2.1/restaurant?res_id=16774318"
api_key = 'RdxKXl3DmdOLLBYksRmJCJamw'
fm_type = 'application/json'
headers =  {'Accept' : fm_type, 'user-key' : api_key}

get_data(api_key,url)


