import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

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

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    task =  {"taskName":"Navigate",
             "AMR_PosTarget":"1",
             "AMR_Footprint":"1",
             "AMR_DOCK":"1"}
    task_post_send(credentials["url"], token, task)


    
