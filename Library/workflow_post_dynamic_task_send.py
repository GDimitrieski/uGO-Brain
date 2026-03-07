import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

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
    dyn_task =  {
        "name": "testDin",
        "params": [
            {"name": "ReagentName", "value": "b"},
            {"name": "ReagentSlotsAvailable", "value": "b"},
            {"name": "ReagentType", "value": "CH"},
        ],
    }
    workflow_post_dynamic_task_send(credentials["url"], token, dyn_task)
