# uGO Task State Handling Class Diagram

```mermaid
classDiagram
direction LR

class TaskCatalog {
  +raw: Dict[str, Any]
  +from_file(path) TaskCatalog
  +get_receiver(task_key) str
  +dispatch_path(task_key) List[str]
  +build_payload(task_key, overrides, include_meta=False) Dict[str, Any]
}

class UgoRobotClient {
  +base_url: str
  +token: str
  +send_task(payload) str
  +wait_task(task_id, timeout_s=120, poll_s=1, max_consecutive_none=5) Dict[str, Any]
  +post_error(code, message, action) str
  +clear_error(error_id) None
  +get_planner_state() Optional[int]
}

class CommandSender {
  +robot: UgoRobotClient
  +catalog: TaskCatalog
  +default_timeout_s: float
  +poll_s: float
  +max_attempts: int
  +post_error_on_fail: bool
  +clear_error_immediately: bool
  +run(task_key, overrides=None, timeout_s=None, task_name=None) Dict[str, Any]
}

class RackProbeTransferWorkflow <<module>> {
  +_run_task(task_key, overrides, task_name) Tuple[bool, Dict[str, Any]]
  +_run_single_task_action(...) bool
}

class get_request_status <<function>> {
  +get_request_status(url, token, request_id) Optional[Dict[str, Any]]
}

class post_planner_error <<function>>
class clear_planner_error <<function>>

CommandSender --> TaskCatalog : build_payload()
CommandSender --> UgoRobotClient : send_task + wait_task
RackProbeTransferWorkflow --> CommandSender : sender.run()
UgoRobotClient ..> get_request_status : poll /api/task/{id}
UgoRobotClient ..> post_planner_error : post_error()
UgoRobotClient ..> clear_planner_error : clear_error()

note for UgoRobotClient
  wait_task state mapping:
  COMPLETE -> succeeded
  ABORTED, STOPPED -> failed
  running states -> keep polling
  unknown state x3 -> failed
  None x5 -> failed
  timeout -> failed
end note

note for CommandSender
  run() accepts only status == "succeeded".
  timeout -> STEP_TIMEOUT (ABORT)
  other failures -> STEP_FAILED (RETRY/ABORT)
end note

note for RackProbeTransferWorkflow
  _run_task sets ok = (result.status == "succeeded").
  if not ok, BT step returns FAILURE.
end note
```
