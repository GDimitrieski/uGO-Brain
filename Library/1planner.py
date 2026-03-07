import requests
from typing import Dict, Any, Optional
from Library.credentials import credentials
from time import sleep

def login(url: str, user: str, password: str) -> Optional[str]:
    login_url = f"{url}/api/auth/login"
    login_payload = {"name": user, "password": password}

    try:
        login_response = requests.post(login_url, json=login_payload)
        login_response.raise_for_status()
        print('Login successful.')
        print(login_response.json())
        token = login_response.json().get('data', {}).get('token')
        return token
    except requests.RequestException as e:
        print(f"Login failed: {e}")
        return None
    

def planner_get_event(url: str, token: str) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            status_response = requests.get(f"{url}/api/planner/event", headers=headers)
            status_response.raise_for_status()
            print('Event:', status_response.json())
            return status_response.json()
        except requests.RequestException as e:
            print(f"Failed to retrieve status: {e}")
    else:
        print("Token cannot be None")
    return None 

def get_ugo_devices_status_to_planner(url: str, token: str) -> Optional[Dict[str, Any]]:

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/system-devices", headers=headers)
        request_status_response.raise_for_status()
        print('Device Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve Devices status: {e}")
        return None
    
def get_ugo_ulm_status_to_planner(url: str, token: str) -> Optional[Dict[str, Any]]:
    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/system-ulm", headers=headers)
        request_status_response.raise_for_status()
        print('ULM Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve ulm status: {e}")
        return None

def post_planner_error(url: str, token: str, error: str) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {"errorCode": error}
        try:
            error_response = requests.post(f"{url}/api/planner/error", headers=headers, json=payload)
            error_response.raise_for_status()
            print('Error sent successfully.')
            print(error_response.json())
            return error_response.json()
        except requests.RequestException as e:
            print(f"Failed to send error: {e}")
    else:
        print("Token cannot be None")
    return None

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
    
def get_mode(url: str) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(f"{url}/api/planner")
        response.raise_for_status()
        print('Mode:', response.json())
        return response.json()
    except requests.RequestException as e:
        print(f"Failed to get mode: {e}")
        return None
    

def planner_get_event(url: str, token: str) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            status_response = requests.get(f"{url}/api/planner/event", headers=headers)
            status_response.raise_for_status()
            print('Event:', status_response.json())
            return status_response.json()
        except requests.RequestException as e:
            print(f"Failed to retrieve status: {e}")
    else:
        print("Token cannot be None")
    return None  

def get_planner_state(url: str, token: str) -> Optional[Dict[str, Any]]:

    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/state", headers=headers)
        request_status_response.raise_for_status()
        print('Planner State:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve planner state: {e}")
        return None

def get_ugo_system_status_to_planner(url: str, token: str) -> Optional[Dict[str, Any]]:
    headers = {'Authorization': f'Bearer {token}'}
    try:
        request_status_response = requests.get(f"{url}/api/planner/system-manager", headers=headers)
        request_status_response.raise_for_status()
        print('System Status:', request_status_response.json())
        return request_status_response.json()
    except requests.RequestException as e:
        print(f"Failed to retrieve System status: {e}")
        return None  

def planner_post_event(url: str, token: str, event: int) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {"event": event}
        try:
            event_response = requests.post(f"{url}/api/send-planner-event", headers=headers, json=payload)
            event_response.raise_for_status()
            print('Event sent successfully.')
            print(event_response.json())
            return event_response.json()
        except requests.RequestException as e:
            print(f"Failed to send event: {e}")
    else:
        print("Token cannot be None")
    return None

def planner_post_state(url: str, token: str, state: int) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {"state": state}
        try:
            state_response = requests.post(f"{url}/api/planner/state", headers=headers, json=payload)
            state_response.raise_for_status()
            print('State sent successfully.')
            print(state_response.json())
            return state_response.json()
        except requests.RequestException as e:
            print(f"Failed to send state: {e}")
    else:
        print("Token cannot be None")
    return None

def task_post_send(url: str, token: str, task: Dict[str, Any]) -> Optional[str]:
    if "taskName" not in task and "name" not in task:
        print("Task name is a mandatory field in the task")
        return None
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            task_response = requests.post(f"{url}/api/task", headers=headers, json=task)
            task_response.raise_for_status()
            print('Task request successful.')
            print(task_response.json())
            return task_response.json().get('data', {}).get('RequestId')
        except requests.RequestException as e:
            print(f"Failed to send task: {e}")
    else:
        print("Token cannot be None")
    return None


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
    
def stop_request(url: str, token: str, request_id: str) -> Optional[Dict[str, Any]]:

    post_request_event(url, token, request_id, "STOP")
    status = get_request_status(url, token, request_id)
    while status["data"]["status"] != "STOPPED":
        status = get_request_status(url, token, request_id)
        sleep(0.5)
    post_request_event(url, token, request_id, "RESET")

def workflow_post_send(url: str, token: str, template_name: str) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        payload = {'id': template_name}
        try:
            workflow_response = requests.post(f"{url}/api/workflow", headers=headers, json=payload)
            workflow_response.raise_for_status()
            print('Workflow request successful.')
            print(workflow_response.json())
            return workflow_response.json()
        except requests.RequestException as e:
            print(f"Failed to send workflow request: {e}")
    else:
        print("Token cannot be None")
    return None

def workflow_post_dynamic_task_send(url: str, token: str, task: Dict[str, Any]) -> Optional[str]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            task_response = requests.post(f"{url}/api/dyn-workflow", headers=headers, json=task)
            task_response.raise_for_status()
            print('Dynamic Task request successful.')
            print(task_response.json())
            return task_response.json().get('data', {}).get('RequestId')
        except requests.RequestException as e:
            print(f"Failed to send Dynamic Task: {e}")
    else:
        print("Token cannot be None")
    return None
    
if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    task =  {"taskName":"UnloadReagent",
             "ReagentType":"CH",
             "ReagentName":"1",
             "ReagentSlot":"1",
             "RemainingQuantity":"1"}
    task_post_send(credentials["url"], token, task)

    workflow_post_send(credentials["url"], token, "Test_Template")

    dyn_task =  {
        "name": "EricTest",
        "params": [
            {"name": "ReagentName", "value": "b"},
            {"name": "ReagentSlot", "value": "1"},
            {"name": "ReagentSlotsAvailable", "value": "b"},
            {"name": "ReagentType", "value": "CH"},
            {"name": "RemainingQuantity", "value": "1"}
        ],
    }
    workflow_post_dynamic_task_send(credentials["url"], "wrongToken", dyn_task)
