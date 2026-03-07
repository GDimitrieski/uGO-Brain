import requests
from typing import Dict, Any, Optional, Union
from Library.login import login
from Library.credentials import credentials
from Library.planner_control import PlannerEvent


def planner_post_event(url: str, token: str, event: Union[int, PlannerEvent]) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {"event": int(event)}
        try:
            event_response = requests.post(f"{url}/api/planner/event", headers=headers, json=payload, timeout=10)
            event_response.raise_for_status()
            print('Event sent successfully.')
            print(event_response.json())
            return event_response.json()
        except requests.RequestException as e:
            print(f"Failed to send event: {e}")
    else:
        print("Token cannot be None")
    return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    planner_post_event(credentials["url"], token, PlannerEvent.RESET)



    
