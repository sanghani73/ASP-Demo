# Helper functions to interact with MongoDB Atlas Stream Processors via REST API

import requests, configparser, time
import urllib.parse
from requests.auth import HTTPDigestAuth
from getProject import get_groupID

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

def create_sp(workspaceName, config):
    """Create a new stream processor with the given configuration"""
    # Get group id from projects
    groupID = get_groupID()

    endpoint = BASE_URL+"/groups/"+groupID+"/streams/"+workspaceName+"/processor"
    try:
        response = requests.post(
            endpoint,
            headers=headers,
            auth=auth,
            json=config
        )
        response.raise_for_status()
        
        print(f"SP {config['name']} created: {response.json()}")
        return response.json()
    # except requests.exceptions.HTTPError as e:
    except requests.exceptions.RequestException as e:
        if e.response.status_code == 409:
            print(f"SP {config['name']} already exists: {response.text}")
            return None
        print(f"Error making request: {response.text}")
        return None
        # raise e

def start_stop_sp(workspaceName, processorName, action):
    """Start or stop a stream processor"""
    # check action is valid
    if action not in ["start", "stop"]:
        raise ValueError("Action must be 'start' or 'stop'")
    
    # Get group id from projects
    groupID = get_groupID()
    endpoint = BASE_URL+"/groups/"+groupID+"/streams/"+workspaceName+"/processor/"+processorName+":"+action

    try:
        response = requests.post(
            endpoint,
            headers=headers,
            auth=auth,
            json={}
        )
        response.raise_for_status()

        print(f"SP {processorName} {action}ed: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {response.text}")
        return None

def start_sp(workspaceName, processorName):
    """Start a stream processor"""
    return start_stop_sp(workspaceName, processorName, "start")

def stop_sp(workspaceName, processorName):
    """Stop a stream processor"""
    return start_stop_sp(workspaceName, processorName, "stop")
    
def get_sps(workspaceName):
    """Get list of all stream processes in the workspace"""
    # Get group id from projects
    groupID = get_groupID()
    endpoint = BASE_URL+"/groups/"+groupID+"/streams"+"/"+workspaceName+"/processors"
    try:
        response = requests.get(
            endpoint,
            headers=headers,
            auth=auth
        )
        response.raise_for_status()
        processors = response.json()['results']
        for processor in processors:
            print(f"SP Name: {processor['name']}, Status: {processor['state']}")

        return processors
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        raise e

def delete_sp(workspaceName, processorName):
    """Delete a stream processor"""
    # Get group id from projects
    groupID = get_groupID()
    endpoint = BASE_URL+"/groups/"+groupID+"/streams"+"/"+workspaceName+"/processor/"+processorName
    try:
        response = requests.delete(
            endpoint,
            headers=headers,
            auth=auth
        )
        response.raise_for_status()
        print(f"SP {processorName} deleted: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {response.text}")
        return None
    
def stop_all_sps(workspaceName):
    """Stop all stream processors in the workspace"""
    sps = get_sps(workspaceName)
    for sp in sps:
        if sp['state'] == 'STARTED':
            stop_sp(workspaceName, sp['name'])

def start_all_sps(workspaceName):
    """Start all stream processors in the workspace"""
    sps = get_sps(workspaceName)
    for sp in sps:
        if sp['state'] == 'STOPPED':
            start_sp(workspaceName, sp['name'])

def delete_all_sps(workspaceName):
    """Delete all stream processors in the workspace"""
    sps = get_sps(workspaceName)
    for sp in sps:
        if sp['state'] == 'STARTED':
            stop_sp(workspaceName, sp['name'])
        
        # wait for a second to ensure SP is stopped
        time.sleep(2)
        print(f"Deleting SP {sp['name']}...")
        delete_sp(workspaceName, sp['name'])

def main():
    # test get_sps function
    print(f"Getting stream processes...")
    for sp in get_sps(WORKSPACE_NAME):
        print(sp['name'])
        if sp['state'] == 'STARTED':
            print(f"Stopping stream processor {sp['name']}...")
            stop_sp(WORKSPACE_NAME, sp['name'])
        if sp['state'] == 'STOPPED':
            print(f"Starting stream processor {sp['name']}...")
            start_sp(WORKSPACE_NAME, sp['name'])

if __name__ == "__main__":
    main()