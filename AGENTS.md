# Project Agent Rules

## Workflow Consistency Rules (Prompt-Time)
- Before changing any workflow from a user prompt, first parse the requested steps and validate them against:
  - `world/world_config.json`
  - `Available_Tasks.json`
- If any prerequisite is missing or inconsistent, do not edit the workflow yet. Report the inconsistency first and ask for the further clarification.
- Do not silently assume missing physical/logical steps when a prompt is incomplete.
- When edits are made, keep assumptions explicit in the response.

## Mandatory Sequence Rules
1. When working on a station different than the uLM Plate, a CameraLandmarkScan Task must be sent to reference the robot coordinate system at the station


ERRORS:
- Any error from the backend or from uGO shall not be tried to be auto repaired. It has to be prompted for clarification !
- If any step fails, stop the sequence and report the failed prerequisite.
