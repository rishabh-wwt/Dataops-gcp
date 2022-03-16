# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 17:40:37 2022

@author: shahris
"""

import requests
import json
from requests.auth import HTTPBasicAuth

#api key and secret key stored here in a variable
api_key = "kIVnsK2i9paDLij2"
api_secret = "11S8JSYAfkIHnqBAphbaixBhw6mxpsUY"


#authorization done via api key and secret key
auth = HTTPBasicAuth(api_key,api_secret)

#Fivetran REST API uses API Key authentication For each request to the API provide an Authorization HTTP header
headers = {
'Authorization': 'Basic ' + api_key,
'Content-Type': 'application/json'
}


#url for group creation
url_for_group_creation="https://api.fivetran.com/v1/groups"

#payload( group name)
data_for_group_creation={"name":"warehouse_demo_group"}

#getting value of group id 
response_1 = requests.post(url = url_for_group_creation,auth =auth,json=data_for_group_creation,headers=headers).json()
print(response_1)
group_id= response_1.get('data', {}).get('id')
print(group_id)



#url for connectors creation
API_ENDPOINT = "https://api.fivetran.com/v1/connectors"

#payload values where we need to declare source connection details
data ={
       "service":"google_cloud_sqlserver",
       "group_id": group_id,
       "paused": False,
       "is_historical_sync": False,
       "sync_frequency": 60,
       "trust_certificates": True,
       "trust_fingerprints": True,
       "run_setup_tests": True, #set
       "config": {
           "host": "146.148.83.14",
           "port": 1433,
           "schema_prefix": "connector_demo", 
           "schema": ["sch_fivetran","dbo"], 	#multiple schemas can be defined in this way	
           "database": "fivetran_test", 	
           "user": "fivetranadmin",			
           "password": "fivetran@123"
           },
        'api_paste_format':'python',
        "schedule_type": "manual"
        }

response = requests.post(url = API_ENDPOINT,auth =auth,json=data,headers=headers).json()
print(response)

#getting value of connector id
connector_id = response.get('data', {}).get('id')
print(connector_id)









