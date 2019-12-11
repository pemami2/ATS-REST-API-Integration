import requests
import ast
import json
import requests
import collections
import pandas as pd

# Exchanging refresh token for an new access token 
token_url = 'https://id.jobadder.com/connect/token/'

data = {'client_id':'********************',
        'client_secret':'************************************',
        'grant_type': 'refresh_token',
        'refresh_token': '************************'}

# Saving the new access token as response1
response1 =requests.post(token_url, data, verify=True, allow_redirects=False)

# Converting the response from JSON to a Python Dictionary
intake = json.loads(response1.text)

# Concatenating our access token with our authorization string
authorization = "Bearer " + intake["access_token"]
print("New authorization token: ", authorization)

# Taking job ID as user input
job_ID = input("Enter the job ID: ")

# Using our access token to send a GET request for candidate applications limited to 1000 responses
url = "https://us1api.jobadder.com/v2/jobs/" + job_ID + "/applications/active?limit=1000"

headers = {
    'Authorization': authorization,
    'User-Agent': "PostmanRuntime/7.20.1",
    'Accept': "*/*",
    'Cache-Control': "no-cache",
    'Postman-Token': "110071e7-262e-4def-9f50-96919fd10a95,0ed931fb-586c-4d81-a4a2-1837fbe17970",
    'Host': "us1api.jobadder.com",
    'Accept-Encoding': "gzip, deflate",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
    }

# Capturing our GET response as response2
response2 = requests.request("GET", url, headers=headers)

# Converting our response from JSON to a Python Dictionary 
data = json.loads(response2.text)
print("Number of Candidates: ", len(data["items"]))

# REST API url for our Microsoft Power BI streaming data set
power_BI_url = "https://api.powerbi.com/beta/****************************/datasets/******************************/rows?noSignUpCheck=1&key=cuUm%2BRtUYs5NiU9ITmCRgg6N8%2FOSOQUHhnOL5Trimtr3ZwUIWsPMora1z6%2Fi0JivrPE6EBGRkaMB2cgPU2DNVQ%3D%3D"

# Initializing an empty dictionary
Candidates = {}

# Capturing and filtering relevant candidate information from our Python Dictionary called "Data"
for x in range(len(data["items"])):
    jobTitle = (data["items"][x].get("jobTitle"))
    firstName = (data["items"][x]["candidate"]["firstName"])
    lastName = (data["items"][x]["candidate"]["lastName"])
    status = (data["items"][x]["status"]["name"])
    email = (data["items"][x]["candidate"].get("email"))
    mobile = (data["items"][x]["candidate"].get("mobile"))
    link = (data["items"][x]["candidate"]["links"].get("self"))
    owner = (data["items"][x]["owner"]["email"])
    updatedAt = (data["items"][x]["updatedAt"])

# Loading filtered information into our new dictionary called "Candidates"
    Candidates[x] = {
        'jobTitle' : jobTitle,
        'firstName' : firstName,
        'lastName' : lastName,
        'status' : status,
        'email' : email,
        'mobile' : mobile,
        'link' : link,
        'owner' : owner,
        'updatedAt' : updatedAt,
    }
    
# Converting each candidate's info in a list of applicants
data_raw = []

for i in range(len(Candidates)):
    row = Candidates[i]
    data_raw.append(row)

# Establishing POST headers        
HEADER = ["jobTitle", "firstName", "lastName", "status", "email", "mobile", "link", "owner", "updatedAt"]

# Converting data back to JSON for POST request
data_df = pd.DataFrame(data_raw, columns=HEADER)
data_json = bytes(data_df.to_json(orient='records'), encoding='utf-8')


# Post data to Microsoft Power BI dashboard
req = requests.post(power_BI_url, data_json)

# Print response from POST request
print("POST response: ", req.text)
