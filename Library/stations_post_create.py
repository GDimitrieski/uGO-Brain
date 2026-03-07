import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def stations_post_create(url: str, token: str, station: Dict[str, Any]) -> Optional[str]:
    if "referenceId" not in station:
        print("referenceId name is a mandatory field in the task")
        return None
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            station_response = requests.post(f"{url}/api/stations", headers=headers, json=station)
            station_response.raise_for_status()
            print('Station request successful.')
            print(station_response.json())
            return station_response.json().get('data', {}).get('RequestId')
        except requests.RequestException as e:
            print(f"Failed to create Station: {e}")
    else:
        print("Token cannot be None")
    return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    station = {
        "referenceId": 2,
        "name": "Centrifuge",
        "available": True
        }
    stations_post_create(credentials["url"], token, station)


    
