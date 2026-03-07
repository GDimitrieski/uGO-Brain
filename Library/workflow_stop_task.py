
import requests
from time import sleep
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials
from Library.workflow_get_request_status import get_request_status

def post_request_event(url: str, token: str, request_id: str, event: str) -> Optional[Dict[str, Any]]:
    if not request_id:
        print("RequestID cannot be None")
        return None
    if not token:
        print("Token cannot be None")
        return None

    headers = {'Authorization': f'Bearer {token}'}

    event_payload = {"uuid": request_id,
                     "event": event
                     }
    try:
        request_status_response = requests.post(f"{url}/api/send-event", headers=headers, json=event_payload )
        request_status_response.raise_for_status()
        print('Request Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve task status: {e}")
        return None
    
def stop_request(url: str, access_token: str, request_id: str) -> Optional[Dict[str, Any]]:
    stop_result = post_request_event(url, access_token, request_id, "STOP")
    if stop_result is None:
        return None
        
    status = get_request_status(url, access_token, request_id)
    if status is None:
        return None
        
    while status.get("data", {}).get("status") != "STOPPED":
        status = get_request_status(url, access_token, request_id)
        if status is None:
            return None
            
    sleep(0.5)
    return post_request_event(url, access_token, request_id, "RESET")


if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    if token is not None:
        stop_request(credentials["url"], token, "0bf4154a-e433-4304-af79-441352510e18")
    else:
        print("Failed to obtain access token. Exiting.")


    
