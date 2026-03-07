import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_error_status(url: str, token: str, errorId: str) -> Optional[Dict[str, Any]]:
    if not errorId:
        print("RequestID cannot be None")
        return None
    if not token:
        print("Token cannot be None")
        return None

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/error/{errorId}", headers=headers)
        request_status_response.raise_for_status()
        print('Error Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve error status: {e}")
        return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    get_error_status(credentials["url"], token, '59ea5540-dec4-48fc-a68d-128e9bd1eb5f' )


    
