import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials
from time import sleep

def post_planner_error(url: str, token: str, error: str, message = "", action = "") -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {"errorCode": error,
                   "message": message,
                   "action": action}
        try:
            error_response = requests.post(f"{url}/api/planner/error", headers=headers, json=payload)
            error_response.raise_for_status()
            print(error_response.json())
            return error_response.json()["data"]["uuid"]
        except requests.RequestException as e:
            print(f"Failed to send error: {e}")
    else:
        print("Token cannot be None")
    return None

def clear_planner_error(url: str, token: str, errorID: str):
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload: Dict[str, Any] = {}
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
    if token is None:
        print("Failed to obtain token")
        exit(1)
    print("Sending error to planner...")

    #'{"errorCode":"113", "message":" WISE Modul Drop off 1 nicht online ","action":"Pruefen Sie, ob das WISE Modul eingeschaltet ist "}'
    error_id = post_planner_error(credentials["url"], token, "133000", " WISE Modul Drop off 2 nicht online ", "Pruefen Sie, ob das WISE Modul eingeschaltet ist ")
    #sleep(10)  # Wait for a while before clearing the error
    print("Clear error from planner...")
    #clear_planner_error(credentials["url"], token, error_id)


    
