# Python-OAuth2-REST-API-Integration
# This Python script is used to update our company Analytics Dashboard with information from our ATS system. 
# It first clears old information from the the dataset on Microsoft Power BI using a DELETE request to their REST API. 
# The script then extracts data from an online ATS (Applicant Tracking System) in JSON using OAuth2 REST API.
# The data is filtered and structured into a list of dictionaries, which is then posted to Microsoft Power BI's REST API for use in our company Analytics Dashboard. 
