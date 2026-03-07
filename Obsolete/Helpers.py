import json
from typing import Dict, Any

def build_payload_with_defaults(tasks: Dict[str, Any], task_key: str) -> Dict[str, Any]:
    task = tasks["Available_Tasks"][task_key]
    template = task["payload_template"]
    params = task.get("parameters", {})

    # defaults
    values = {k: meta.get("default") for k, meta in params.items() if isinstance(meta, dict)}

    # deep copy template
    payload = json.loads(json.dumps(template))

    # substitute placeholders
    for k, v in values.items():
        placeholder = "{" + k + "}"
        for field, field_value in payload.items():
            if isinstance(field_value, str) and placeholder in field_value:
                payload[field] = field_value.replace(placeholder, str(v))

    return payload