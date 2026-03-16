import requests
from typing import Dict, Any, Optional
from login import login
from credentials import credentials

def clear_planner_error(url: str, token: str, errorID: str):
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {}
        try:
            error_response = requests.put(f"{url}/api/planner/error/{errorID}", headers=headers, json=payload)
            error_response.raise_for_status()
            print('Error sent successfully.')
            print(error_response.json())
            return error_response.json()
        except requests.RequestException as e:
            print(f"Failed to send error: {e}")
    else:
        print("Token cannot be None")
    return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    clear_planner_error(credentials["url"], token, '15763ee4-d5cf-40fb-b0fc-f68f8da02bfe')
    
