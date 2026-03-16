import requests
from typing import Dict, Any, Optional, Union
from login import login
from credentials import credentials
from planner_control import SystemState


def planner_post_state(url: str, token: str, state: Union[int, SystemState]) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {"state": int(state)}
        try:
            state_response = requests.post(f"{url}/api/planner/state", headers=headers, json=payload, timeout=10)
            state_response.raise_for_status()
            print('State sent successfully.')
            print(state_response.json())
            return state_response.json()
        except requests.RequestException as e:
            print(f"Failed to send state: {e}")
    else:
        print("Token cannot be None")
    return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    planner_post_state(credentials["url"], token, SystemState.EXECUTE)



    
