import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_request_status(url: str, token: str, request_id: str) -> Optional[Dict[str, Any]]:
    if not request_id:
        print("RequestID cannot be None")
        return None
    if not token:
        print("Token cannot be None")
        return None

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/task/{request_id}", headers=headers)
        request_status_response.raise_for_status()
        response_json = request_status_response.json()
        print('Request Status:', response_json)
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve task status: {e}")
        return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    get_request_status(credentials["url"], token,"d42c8d04-41eb-42ad-98bd-8ef4d39966e5")


    
