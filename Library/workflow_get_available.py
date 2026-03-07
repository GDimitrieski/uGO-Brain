import requests
from typing import Dict, Any, Optional
from Library.login import login
from Library.credentials import credentials

def get_available_workflows(url: str, token: str) -> Optional[Dict[str, Any]]:
    if token:
        headers = {'Authorization': f'Bearer {token}'}
        try:
            templates_response = requests.get(f"{url}/api/template", headers=headers)
            templates_response.raise_for_status()
            print('Templates:', templates_response.json())
            return templates_response.json()
        except requests.RequestException as e:
            print(f"Failed to retrieve templates: {e}")
    else:
        print("Token cannot be None")
    return None

if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    get_available_workflows(credentials["url"], token)


    
