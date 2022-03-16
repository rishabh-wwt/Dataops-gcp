# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 21:36:11 2022

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


#connector id mentioned here
connector_id = "behaving_railway"

#url mentioned here
url = "https://api.fivetran.com/v1/connectors/{}".format(connector_id)

#payload where we can mention the parameters we want to modify the connector
body = {
    "paused": False,
    "is_historical_sync": False,
    "sync_frequency": 60,
    "trust_certificates": True,
    "run_setup_tests": True,
    "config": {
        "schema": "sch_fivetran", 		
        "database": "fivetran_test", 	
        "user": "fivetranadmin",			
        "password": "fivetran@123"
        },
    "schedule_type": "manual"
}


response = requests.patch(url=url,auth=auth,json=body).json()
print(response)


#url for reloading schema
url_reload_schema="https://api.fivetran.com/v1/connectors/behaving_railway/schemas/reload"

#url for sync connector
url_sync="https://api.fivetran.com/v1/connectors/behaving_railway/force"
data={
    "exclude_mode":"PRESERVE"
}

response_reload_schema = requests.post(url=url_reload_schema,auth=auth,json=data).json()
print(response_reload_schema)



response_sync = requests.post(url=url_sync,auth=auth,json=data).json()
print(response_sync)



schema_name='sch_fivetran'
#url for connector id specific schema
url_specific = "https://api.fivetran.com/v1/connectors/{}/schemas/{}".format(connector_id,schema_name)
print(url)
# Create Request Body
# Ensure That The Connector Config Contains the Specific Updated Config for your Source
# Documentation: https://fivetran.com/docs/rest-api/connectors#modifyaconnectordatabaseschemaconfig

#Here we are enabling fivetran replication for all columns in fivetran_demo 
#disabling replication for fivetran_temp2,enabling replication for few columns of test table
body = {
        "enabled": True,
        "tables": { "fivetran_demo":{
            
                "enabled": True
            } ,       
            "fivetran_temp2"  :{
                
                    "enabled": False
                } ,
            "test":{
                "enabled": True,
                "columns": {
                    "id": {
                        "enabled": True,
                        "hashed": False
                    },
                    "name": {
                        "enabled": True,
                        "hashed": False
                    },
                    "column_2":{"enabled": False},
                    "column_3":{"enabled": False}                   
                }
                }
            
        }
        
        
}

response = requests.patch(url=url,auth=auth,json=body).json()
print(response)



