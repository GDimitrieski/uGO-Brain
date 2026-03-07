import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_all_dynamic_task(url: str, token: str) -> Optional[str]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            task_response = requests.get(f"{url}/api/dynamic-templates", headers=headers)
            task_response.raise_for_status()
            print('All Dynamic Task request successful.')
            print(task_response.json())
            return task_response.json()#.get('data', {}).get('RequestId')
        except requests.RequestException as e:
            print(f"Failed to send Dynamic Task: {e}")
    else:
        print("Token cannot be None")
    return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    get_all_dynamic_task(credentials["url"], token)


    
