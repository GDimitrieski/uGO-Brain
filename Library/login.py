import requests
from typing import Dict, Any, Optional
from Library.credentials import credentials


def login(url: str, user: str, password: str, timeout_s: float = 10.0) -> Optional[str]:
    login_url = f"{url}/api/auth/login"
    login_payload = {"name": user, "password": password}

    try:
        login_response = requests.post(login_url, json=login_payload, timeout=timeout_s)
        login_response.raise_for_status()
        print('Login successful.')
        print(login_response.json())
        token = login_response.json().get('data', {}).get('token')
        return token
    except requests.RequestException as e:
        print(f"Login failed: {e}")
        return None
    

if __name__ == "__main__":
    login(credentials["url"], credentials["user"], credentials["password"])
    
