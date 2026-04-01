import json
import requests
from typing import Dict, Any, Optional, List, Union
from login import login
from credentials import credentials
from time import sleep, time


def post_planner_prompt(url: str, token: str, title: str, body: str,
                        actions: Union[List[Dict[str, str]], Dict[str, str], None] = None) -> Optional[str]:
    """Post a prompt to the control system. Returns the prompt UUID."""
    if not token:
        print("Token cannot be None")
        return None
    headers = {'Authorization': f'Bearer {token}'}
    payload: Dict[str, Any] = {"title": title, "body": body}
    if actions is not None:
        payload["actions"] = actions
    else:
        payload["actions"] = []
    try:
        print(f"post_planner_prompt payload: {json.dumps(payload, ensure_ascii=True)}")
        response = requests.post(f"{url}/api/control-system/prompt", headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        print(data)
        return data["data"]["uuid"]
    except requests.RequestException as e:
        print(f"Failed to post prompt: {e}")
    return None


def dismiss_planner_prompt(url: str, token: str, prompt_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Dismiss the active prompt, or a specific one by ID."""
    if not token:
        print("Token cannot be None")
        return None
    headers = {'Authorization': f'Bearer {token}'}
    payload: Dict[str, Any] = {}
    if prompt_id:
        payload["promptId"] = prompt_id
    try:
        response = requests.post(f"{url}/api/control-system/dismiss", headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        print(data)
        return data
    except requests.RequestException as e:
        print(f"Failed to dismiss prompt: {e}")
    return None


def get_prompt_response(url: str, token: str, prompt_id: str) -> Optional[Dict[str, Any]]:
    """Get a prompt by ID. Returns the full prompt object."""
    if not token:
        print("Token cannot be None")
        return None
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(f"{url}/api/control-system/prompt/{prompt_id}", headers=headers)
        response.raise_for_status()
        return response.json()["data"]
    except requests.RequestException as e:
        print(f"Failed to get prompt: {e}")
    return None


def wait_for_user_response(url: str, token: str, prompt_id: str,
                           poll_interval: float = 2, timeout: float = 300) -> Optional[str]:
    """Poll until the user responds to a prompt. Returns the chosen actionId, or None on timeout/dismissal."""
    start = time()
    consecutive_errors = 0
    while time() - start < timeout:
        prompt = get_prompt_response(url, token, prompt_id)
        if prompt is None:
            # API error (e.g. 403) — keep polling, don't give up
            consecutive_errors += 1
            if consecutive_errors > 5:
                print(f"wait_for_user_response: too many consecutive errors polling prompt {prompt_id}")
            sleep(poll_interval)
            continue
        consecutive_errors = 0
        if prompt.get("respondedAt"):
            response = prompt.get("response")
            if response and response.get("actionId"):
                return response["actionId"]
            # respondedAt is set but response is null -> dismissed
            return None
        if prompt.get("dismissed"):
            return None
        sleep(poll_interval)
    print(f"Timeout waiting for user response on prompt {prompt_id}")
    return None


def get_prompt_history(url: str, token: str, limit: int = 20) -> Optional[List[Dict[str, Any]]]:
    """Get prompt history from the control system."""
    if not token:
        print("Token cannot be None")
        return None
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(f"{url}/api/control-system/history", headers=headers, params={"limit": limit})
        response.raise_for_status()
        return response.json()["data"]
    except requests.RequestException as e:
        print(f"Failed to get prompt history: {e}")
    return None


if __name__ == "__main__":
    token = login(credentials["url"], credentials["user"], credentials["password"])
    if token is None:
        print("Failed to obtain token")
        exit(1)

    # Example: post a prompt with action choices and wait for user response
    prompt_id = post_planner_prompt(
        credentials["url"], token,
        "Step Failed",
        "Centrifuge unreachable. Retry?",
        [{"id": "retry", "label": "Retry"}, {"id": "abort", "label": "Abort"}]
    )
    if prompt_id:
        print(f"Prompt posted: {prompt_id}")
        action = wait_for_user_response(credentials["url"], token, prompt_id, timeout=120)
        print(f"User chose: {action}")
