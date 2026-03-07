import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def workflow_post_send(url: str, token: str, template_name: int) -> Optional[Dict[str, Any]]:
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

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    workflow_post_send(credentials["url"], token, 57)
   


    
