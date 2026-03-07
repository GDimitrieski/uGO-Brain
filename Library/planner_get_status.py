import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_status(url: str, token: str) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            status_response = requests.get(f"{url}/api/status", headers=headers)
            status_response.raise_for_status()
            print('Status:', status_response.json())
            return status_response.json()
        except requests.RequestException as e:
            print(f"Failed to retrieve status: {e}")
    else:
        print("Token cannot be None")
    return None  

def get_ugo_system_status_to_planner(url: str, token: str) -> Optional[Dict[str, Any]]:
    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/system-manager", headers=headers)
        request_status_response.raise_for_status()
        print('System Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve System status: {e}")
        return None  

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    #get_status(credentials["url"], token)
    get_ugo_system_status_to_planner(credentials["url"], token)

    
