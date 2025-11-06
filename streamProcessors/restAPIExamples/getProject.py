# Get Project ID from Project Name using MongoDB Atlas REST API

import requests, configparser
import urllib.parse
from requests.auth import HTTPDigestAuth
import json

# MongoDB Atlas API configuration
config = configparser.ConfigParser()
config.read("config.ini")

BASE_URL = config.get("DEFAULT", "BASE_URL")
API_KEY = config.get("DEFAULT", "API_KEY")
API_SECRET = config.get("DEFAULT", "API_SECRET")
PROJECT_NAME = config.get("DEFAULT", "PROJECT_NAME")

# Create digest auth object
auth = HTTPDigestAuth(API_KEY, API_SECRET)

# Headers for the request
headers = {
    'Accept': 'application/vnd.atlas.2024-08-05+json'
}

def formatName(name):
    # Format name to be URL friendly using a URL encoding
    return urllib.parse.quote(name)

def get_groupID():
    """Get list of all projects in the account"""
    endpoint = BASE_URL+"/groups/byName/"+formatName(PROJECT_NAME)
    try:
        response = requests.get(
            endpoint,
            headers=headers,
            auth=auth
        )
        response.raise_for_status()
        groupID = response.json()['id']
        return groupID
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        raise e
