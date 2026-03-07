import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_ugo_devices_status_to_planner(url: str, token: str) -> Optional[Dict[str, Any]]:

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/system-devices", headers=headers)
        request_status_response.raise_for_status()
        print('Device Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve Devices status: {e}")
        return None
    
def get_ugo_ulm_status_to_planner(url: str, token: str) -> Optional[Dict[str, Any]]:
    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/system-ulm", headers=headers)
        request_status_response.raise_for_status()
        print('ULM Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve ulm status: {e}")
        return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    get_ugo_devices_status_to_planner(credentials["url"], token) # Adjusted to only provide the ulm status without the gripper
    get_ugo_ulm_status_to_planner(credentials["url"], token)

    
