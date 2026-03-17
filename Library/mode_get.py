import requests
from typing import Dict, Any, Optional
from credentials import credentials

def get_mode(url: str) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(f"{url}/api/planner")
        response.raise_for_status()
        print('Mode:', response.json())
        return response.json()
    except requests.RequestException as e:
        print(f"Failed to get mode: {e}")
        return None
    

if __name__ == "__main__":
    get_mode(credentials["url"])

    
