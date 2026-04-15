# Claude Refactor Playbook for uGO Brain vNext

Date: 2026-04-13
Purpose: Force consistent, architecture-aligned refactors with explicit safety checks
Use with: `docs/New Version/vNext_Architecture_Refactor_Guideline.md`

## 1. How to Run Refactors with Claude
Use a strict "contract prompt" each time. Do not ask for broad free-form refactoring.

Required pattern:
1. Give Claude one bounded scope (single module or single phase).
2. Require preflight validation against:
   - `AGENTS.md`
   - `world/world_config.json`
   - `Available_Tasks.json`
3. Require no behavior change unless explicitly requested.
4. Require tests or trace checks for changed behavior.
5. Require explicit output structure.

## 2. Master Prompt Template
Copy-paste this into Claude:

```text
You are refactoring uGO-Brain under strict architecture constraints.

Authoritative rules:
1) docs/New Version/vNext_Architecture_Refactor_Guideline.md
2) docs/New Version/uGO-Brain-2.0-Greenfield-Specification.md
3) AGENTS.md

Task scope (strict):
<PASTE SCOPE HERE>

Hard constraints:
- Validate requested changes against world/world_config.json and Available_Tasks.json before edits.
- Do not change runtime behavior unless explicitly listed in "Allowed behavior changes".
- Do not auto-repair backend/uGO errors.
- If prerequisite is missing/inconsistent, stop and report instead of guessing.
- Keep IDs and naming stable.
- Enforce landmark/navigation rules as configured, not by station-name shortcuts.

Allowed behavior changes:
<NONE or LIST>

Required process:
1) Preflight check: list constraints and impacted files.
2) Minimal refactor plan with explicit module boundaries.
3) Implement in small commits/patches with clear ownership.
4) Run validation/tests and show results.
5) Produce migration notes.

Required output format:
- Preflight Validation
- Proposed File Changes
- Invariants Preserved
- Tests/Checks Run
- Risks/Follow-ups
- Final Changed Files List
```

## 3. Refactor Ticket Template
Use this for each iteration:

```text
Refactor Ticket
- Goal:
- In scope:
- Out of scope:
- Allowed behavior changes:
- Must preserve:
- Acceptance checks:
- Files expected to change:
```

## 4. Guardrail Add-On Prompt
Append this when Claude starts drifting into broad rewrites:

```text
Guardrail:
- Do not rewrite unrelated modules.
- Touch only listed files.
- Keep function signatures stable unless explicitly approved.
- If you need a signature change, stop and justify before editing.
```

## 5. Prompt for "No Hidden Logic Moves"
Use this when moving code out of monolithic workflow file:

```text
When extracting logic:
- Keep old call path and add thin wrapper first.
- Move implementation behind same contract.
- Keep old behavior tests green.
- Show exact before/after function mapping.
```

## 6. Prompt for Planner/Executor Separation
Use this when splitting decision and execution:

```text
Separation rule:
- Planner returns one intent object only.
- Executor consumes intent and performs actions.
- Planner must not call backend/device APIs.
- Executor must not choose business process order.
Enforce this in code boundaries and tests.
```

## 7. Prompt for Policy-Driven Constants
Use this for hardcoded arrays like IH500 slot constants:

```text
Policy migration:
- Identify hardcoded process/device constants (example: IH500_KREUZPROBE_SLOT_INDEXES).
- Move them to policy/config layer.
- Keep defaults backward-compatible.
- Add validation for missing/invalid policy values.
```

## 8. Review Checklist for Claude Output
Accept only if all are true:
1. Preflight references AGENTS + world config + task catalog.
2. Scope was respected.
3. Invariants were listed and preserved.
4. Test/check evidence was provided.
5. Changed files are minimal and intentional.
6. No hidden behavior change outside allowed list.

## 9. Suggested Refactor Order
1. Extract world mutation boundary.
2. Extract planner intent API.
3. Extract executor action API.
4. Move process-specific logic to handlers.
5. Migrate hardcoded policies to config.
6. Add lease manager and scoring.

## 10. Example Short Prompt (Ready to Use)
```text
Refactor only planning/planner.py:
- Extract IH500 slot index rules from code into process policy config.
- Keep current defaults behavior-identical.
- No workflow changes.
- Add validation and tests for policy loading.

Use preflight against AGENTS.md, world/world_config.json, Available_Tasks.json.
Output in the required format from the master template.
```

