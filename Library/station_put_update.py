import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def station_put_update(url: str, token: str, station: Dict[str, Any]) -> Optional[str]:
    if "referenceId" not in station:
        print("referenceId name is a mandatory field in the task")
        return None
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            station_response = requests.put(f"{url}/api/stations/{1}", headers=headers, json=station)
            station_response.raise_for_status()
            print('Station request successful.')
            print(station_response.json())
            return station_response.json().get('data', {}).get('RequestId')
        except requests.RequestException as e:
            print(f"Failed to update Station: {e}")
    else:
        print("Token cannot be None")
    return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    station = {
        "referenceId": 1,
        "name": "TestStation",
        "available": False
        }
    station_put_update(credentials["url"], token, station)


    
