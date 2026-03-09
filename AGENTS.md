# Project Agent Rules

## Workflow Consistency Rules (Prompt-Time)
1. Before changing any workflow from a user prompt, first parse the requested steps and validate them against:
  - `world/world_config.json`
  - `Available_Tasks.json`
2. If any prerequisite is missing or inconsistent, do not edit the workflow yet. Report the inconsistency first and ask for the further clarification.
3. Do not silently assume missing physical/logical steps when a prompt is incomplete.
4. When edits are made, keep assumptions explicit in the response.

## Mandatory Sequence Rules
1. When working on a station different than the uLM Plate, a CameraLandmarkScan Task must be sent to reference the robot coordinate system at the station

## Consider. Be Aware
1. There might be multiple devices of the same or different types that will do the same type of processing on the samples
2. Once identified i.e. classified, the Sample is a living object in the world that will undergo certain transformations physical (like decapping, capping) or analytic (where a certain device will analyze aliquote from the sample and make analysis or scanner will read it's barcode or camera will recognize its cap colour) -> This is just awareness and the how and what will be specifically defined.

## Errors while prompting, code changes
1. Any error from the backend or from uGO shall not be tried to be auto repaired. It has to be prompted for clarification !
2. If any step fails, stop the sequence and report the failed prerequisite.
