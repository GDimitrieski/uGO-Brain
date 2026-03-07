# Project Agent Rules

## Workflow Consistency Rules (Prompt-Time)
- Before changing any workflow from a user prompt, first parse the requested steps and validate them against:
  - `world/world_config.json`
  - latest `world/world_occupancy_trace.jsonl`
  - `Available_Tasks.json`
- If any prerequisite is missing or inconsistent, do not edit the workflow yet. Report the inconsistency first and ask for the minimum clarification needed.
- Do not silently assume missing physical/logical steps when a prompt is incomplete.
- When edits are made, keep assumptions explicit in the response.

## Mandatory Sequence Rule: "Place InputRack Back"
Mandatory prerequsites for a Task to take place i.e. be planned in the workflow
1. When working on a station different than the uLM Plate, a CameraLandmarkScan Task must be sent to reference the robot coordinate system at the station

- If any step fails, stop the sequence and report the failed prerequisite.

ERRORS:
- Any error from the backend or from uGO shall not be tried to be auto repaired. It has to be prompted for clarification !