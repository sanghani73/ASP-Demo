#  This script deletes a workspace in MongoDB Atlas Stream Processing

import requests, configparser
from requests.auth import HTTPDigestAuth
from getProject import get_groupID
from streamProcessorsHelper import delete_all_sps

# MongoDB Atlas API configuration
config = configparser.ConfigParser()
config.read("config.ini")

BASE_URL = config.get("DEFAULT", "BASE_URL")
API_KEY = config.get("DEFAULT", "API_KEY")
API_SECRET = config.get("DEFAULT", "API_SECRET")
WORKSPACE_NAME = config.get("DEFAULT", "WORKSPACE_NAME")


# Create digest auth object
auth = HTTPDigestAuth(API_KEY, API_SECRET)

# Headers for the request
headers = {
    'Accept': 'application/vnd.atlas.2024-08-05+json'
}


def delete_workspace(groupID, workspaceName):
    """Delete a workspace"""
    # delete all stream processors
    delete_all_sps(workspaceName)

    endpoint = BASE_URL+"/groups/"+groupID+"/streams/"+workspaceName
    try:
        response = requests.delete(
            endpoint,
            headers=headers,
            auth=auth
        )
        response.raise_for_status()
        print(f"Workspace deleted: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {response.text}")
        return None

def main():
    # Get group id from projects
    groupID = get_groupID()

    print(f"Deleting workspace {WORKSPACE_NAME}...")
    delete_workspace(groupID, WORKSPACE_NAME)

if __name__ == "__main__":
    main()  