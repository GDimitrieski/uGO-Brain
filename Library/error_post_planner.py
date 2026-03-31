import json
import requests
from typing import Dict, Any, Optional
from login import login
from credentials import credentials
from time import sleep

def post_planner_error(url: str, token: str, error: str, message = "", action = "") -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {"errorCode": error,
                   "message": message,
                   "action": action}
        try:
            print(f"post_planner_error payload: {json.dumps(payload, ensure_ascii=True)}")
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
    #error_id = post_planner_error(credentials["url"], token, " ", "Navigation to Staion 5", "Pending Centrifugation")
    #Message_id = post_planner_message(credentials["url"], token, " ", "Navigation to Staion 5", "Pending Centrifugation"), { 1: "Message 1", 2 : "Message 2" , 3 : "Message 3"}
    #sleep(10)  # Wait for a while before clearing the error
    print("Clear error from planner...")
    #message is shown and id is returned
    #Message_id = post_planner_message(credentials["url"], token, " ", "Navigation to Staion 5", "Pending Centrifugation"), { 1: "Message 1", 2 : "Message 2" , 3 : "Message 3"}
    #The user response to the message if any response was sent is requested by this API method
    #UserResponse = get_user_action(credentials["url"], token, Message_id)

    
