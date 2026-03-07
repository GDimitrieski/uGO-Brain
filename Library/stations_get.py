import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_stations(url: str, token: str):

    if not token:
        print("Token cannot be None")
        return None

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/stations", headers=headers)
        request_status_response.raise_for_status()
        print('Station Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve station status: {e}")
        return None
    
def get_station_by_id(url: str, token: str, referenceId: int) -> Optional[Dict[str, Any]]:
    if not referenceId:
        print("RequestID cannot be None")
        return None
    if not token:
        print("Token cannot be None")
        return None

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/stations/{referenceId}", headers=headers)
        request_status_response.raise_for_status()
        print('Station Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve station status: {e}")
        return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    station = 1
    get_stations(credentials["url"], token)
    get_station_by_id(credentials["url"], token, station)


    
