import json
import requests
import pandas as pd
import datetime
import adal


def Delete_dataset(username, password):
    
    #using Power BI's REST API to clear the dataset of previous data
    
    authority_url = 'https://login.windows.net/common'
    resource_url = 'https://analysis.windows.net/powerbi/api'
    client_id = '**********************************'
    username = username
    password = password

    context = adal.AuthenticationContext(authority=authority_url,
                                         validate_authority=True,
                                         api_version=None)

    token = context.acquire_token_with_username_password(resource=resource_url,
                                                         client_id=client_id,
                                                         username=username,
                                                         password=password)

    access_token = token.get('accessToken')

    refresh_url = 'https://api.powerbi.com/v1.0/myorg/datasets/c230986b-0466-42ab-87b6-ac53ccd1bfe3/tables/RealTimeData/rows'

    header = {'Authorization': f'Bearer {access_token}'}

    r = requests.delete(url=refresh_url, headers=header)

    return r

def accessToken():
    
    # Exchanging refresh token for an new access token
    
    token_url = 'https://id.jobadder.com/connect/token/'

    data = {'client_id':'***************************',
            'client_secret':'******************************************',
            'grant_type': 'refresh_token',
            'refresh_token': '***************************'}

    # Saving the new access token 
    new_token =requests.post(token_url, data, verify=True, allow_redirects=False)

    # Converting the response from JSON to a Python Dictionary
    intake = json.loads(new_token.text)

    # Concatenating our access token with our authorization string
    authorization = "Bearer " + intake["access_token"]

    return authorization

def Jobadder_request(url,authorization):
    
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
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text)
    return data


def main():
    
    #Deleting old data from power BI dataset
    username = input("Please enter your Microsoft username: ")
    password = input("Please enter your Microsoft password: ")

    power_BI_API = Delete_dataset(username,password)
    print("Power BI DELETE response: " , power_BI_API.text)
    
    #retrieving a new access token for jobadder API 
    auth = accessToken()
    
    #retrieving active job ID's from the API
    active_jobs = Jobadder_request("https://us1api.jobadder.com/v2/jobs/", auth)
    
    # Initializing an empty dictionary and current date 
    Candidates = {}
    dateStamp = datetime.datetime.today()

    # Adding active job ID's to the empty set
    activeJobs = set()
    
    for x in range(len(active_jobs["items"])):
        if active_jobs["items"][x]["status"]["statusId"] == 38350:
            activeJobs.add(active_jobs["items"][x]["jobId"])

    
    for x in activeJobs:

        #retrieving candidate info from the API
        candidate_info = Jobadder_request("https://us1api.jobadder.com/v2/jobs/" + str(x) + "/applications/active?limit=1000",auth)
        
        # Capturing and filtering relevant candidate information from our Python Dictionary called "Data"
        for x in range(len(candidate_info["items"])):
            jobTitle = (candidate_info["items"][x].get("jobTitle"))
            firstName = (candidate_info["items"][x]["candidate"]["firstName"])
            lastName = (candidate_info["items"][x]["candidate"]["lastName"])
            status = (candidate_info["items"][x]["status"]["name"])
            email = (candidate_info["items"][x]["candidate"].get("email"))
            mobile = (candidate_info["items"][x]["candidate"].get("mobile"))
            link = (candidate_info["items"][x]["candidate"]["links"].get("self"))
            owner = (candidate_info["items"][x]["owner"].get("email"))
            updatedAt = (candidate_info["items"][x]["updatedAt"])
            updated = datetime.datetime.strptime(updatedAt, "%Y-%m-%dT%H:%M:%SZ")
            days_since_refresh = dateStamp - updated

            # Loading filtered information into our new dictionary called "Candidates"
            Candidates[len(Candidates)] = {
                'jobTitle' : jobTitle,
                'Name' : firstName + " " + lastName,
                'status' : status,
                'email' : email,
                'mobile' : mobile,
                'link' : link,
                'owner' : owner,
                'updatedAt' : updatedAt,
                'days' : int(days_since_refresh.days)
            }

    # Converting each candidate's info in a list of applicants
    data_raw = []

    for i in range(len(Candidates)):
        row = Candidates[i]
        data_raw.append(row)

    # Establishing POST headers        
    HEADER = ["jobTitle", "Name", "status", "email", "mobile", "link", "owner", "updatedAt" ,"days"]

    # Converting data back to JSON for POST request
    data_df = pd.DataFrame(data_raw, columns=HEADER)
    data_json = bytes(data_df.to_json(orient='records'), encoding='utf-8')

    # REST API url for our Microsoft Power BI streaming data set
    power_BI_url = "https://api.powerbi.com/beta/************************/datasets/*****************************/rows?noSignUpCheck=1&key=cuUm%2BRtUYs5NiU9ITmCRgg6N8%2FOSOQUHhnOL5Trimtr3ZwUIWsPMora1z6%2Fi0JivrPE6EBGRkaMB2cgPU2DNVQ%3D%3D"

    # Post data to Microsoft Power BI dashboard
    req = requests.post(power_BI_url, data_json)

    # Print response from POST request
    print("Power BI POST response: ", req.text)


main()
