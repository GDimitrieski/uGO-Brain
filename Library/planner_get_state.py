import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_planner_state(url: str, token: str) -> Optional[Dict[str, Any]]:

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/state", headers=headers, timeout=10)
        request_status_response.raise_for_status()
        print('Planner State:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve planner state: {e}")
        return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    get_planner_state(credentials["url"], token)


    
