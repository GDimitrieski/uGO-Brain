import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def planner_get_event(url: str, token: str) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            status_response = requests.get(f"{url}/api/planner/event", headers=headers)
            status_response.raise_for_status()
            print('Event:', status_response.json())
            return status_response.json()
        except requests.RequestException as e:
            print(f"Failed to retrieve status: {e}")
    else:
        print("Token cannot be None")
    return None  


if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    planner_get_event(credentials["url"], token)

    
