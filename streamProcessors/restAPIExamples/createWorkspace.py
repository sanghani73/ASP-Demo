# This script creates a new workspace and sample connections in MongoDB Atlas Stream Processing

import requests, configparser
from requests.auth import HTTPDigestAuth
from getProject import get_groupID

# MongoDB Atlas API configuration
config = configparser.ConfigParser()
config.read("config.ini")

BASE_URL = config.get("DEFAULT", "BASE_URL")
API_KEY = config.get("DEFAULT", "API_KEY")
API_SECRET = config.get("DEFAULT", "API_SECRET")
WORKSPACE_NAME = config.get("DEFAULT", "WORKSPACE_NAME")

WORKSPACE_DEFINITION = {
  "dataProcessRegion": {
    "cloudProvider": "AWS",
    "region": "LONDON_GBR"
  },
  "name": WORKSPACE_NAME,
  "sampleConnections": {
    "solar": False
  },
  "streamConfig": {
    "maxTierSize": "SP10",
    "tier": "SP2"
  }
}

DB_CONNECTION_DEFINITION = {
  "clusterName": "AnandSandbox",
  "name": "AtlasDatabase",
  "dbRoleToExecute": {
    "role": "readWriteAnyDatabase",
    "type": "BUILT_IN"
  },
  "type": "Cluster"
}

# Create digest auth object
auth = HTTPDigestAuth(API_KEY, API_SECRET)

# Headers for the request
headers = {
    'Accept': 'application/vnd.atlas.2024-08-05+json'
}


def create_workspace(groupID):
    """Create a new workspace"""
    endpoint = BASE_URL+"/groups/"+groupID+"/streams"
    try:
        response = requests.post(
            endpoint,
            headers=headers,
            auth=auth,
            json=WORKSPACE_DEFINITION
        )
        response.raise_for_status()
        print(f"Workspace created: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        if e.response.status_code == 409:
            print(f"Workspace {WORKSPACE_DEFINITION['name']} (conflict) already exists: {e}")
            return None
        print(f"Error making request: {e}")
        return None

def create_connections(groupID, config):
    """Create sample connections in the workspace"""
    endpoint = BASE_URL+"/groups/"+groupID+"/streams/"+WORKSPACE_DEFINITION['name']+"/connections"
    print(endpoint)

    try:
        response = requests.post(
            endpoint,
            headers=headers,
            auth=auth,
            json=config
        )
        response.raise_for_status()
        print(f"Connections created: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        if e.response.status_code == 409:
            print(f"Connection {DB_CONNECTION_DEFINITION['name']} (conflict) already exists: {e}")
            return None
        print(f"Error making request: {e}")
        return None


def main():
    # Get group id from projects
    groupID = get_groupID()

    print(f"Creating workspace {WORKSPACE_DEFINITION['name']}...")
    create_workspace(groupID)
    print(f"Creating connections {DB_CONNECTION_DEFINITION['name']}...")
    DB_CONNECTION_DEFINITION["clusterGroupId"] = groupID
    create_connections(groupID, DB_CONNECTION_DEFINITION)

if __name__ == "__main__":
    main()  