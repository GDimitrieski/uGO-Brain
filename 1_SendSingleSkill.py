from Library.login import login
from Library.credentials import credentials
from Library.workflow_post_task_send import task_post_send
from Library.workflow_get_request_status import get_request_status
import time
from typing import Any, Dict, Optional

token = login(credentials["url"], credentials["user"], credentials["password"])


def wait_for_complete(
    url: str,
    token: str,
    task_id: str,
    timeout_s: float = 120.0,
    poll_s: float = 1.0,
) -> Optional[Dict[str, Any]]:
    start_ts = time.time()
    last_status: Optional[Dict[str, Any]] = None

    while (time.time() - start_ts) < timeout_s:
        status = get_request_status(url, token, task_id)
        if not isinstance(status, dict):
            time.sleep(poll_s)
            continue

        last_status = status
        data = status.get("data", {})
        state = str(data.get("status", "")).upper()

        if state in {"COMPLETE", "ABORTED", "STOPPED"}:
            return status

        time.sleep(poll_s)

    print(f"Timed out waiting for task '{task_id}' to complete after {timeout_s}s")
    return last_status


if __name__ == "__main__":

    payload = {
        "taskName": "SingleTask",
        "ITM_ID": 1,
        "JIG_ID": 2,
        "OBJ_Nbr": 1,
        "ACT": 1,
        "OBJ_Type": 520,
    }
    print(payload)

    task_id = task_post_send(credentials["url"], token, payload)
    if not task_id:
        raise RuntimeError("Task submission failed; no RequestId returned.")
    final_status = wait_for_complete(credentials["url"], token, task_id)
    print("Final status:", final_status)
