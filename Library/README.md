# uGOAPITestingPythonScripts

## Description

This is a series of python scripts intended to communicate with uGO REST API.

## Installation

In order to run this scripts you need Python 3 installed.

## Usage

- Adjust your credentials in the credentials.py file with the user name and the password you have configured

- Adjust tasks payload in file workflow_post_task_send.py to match an existing task in uGO. A task always has a taskName parameter and from 0 to many parameters.
TaskExample = {"taskName":"UnloadReagent",
            "ReagentType":"CH",
            "ReagentName":"1",
            "ReagentSlot":"1",
            "RemainingQuantity":"1"} 

- Adjust dynamic tasks payload in file workflow_post_dynamic_task_send to match an existing dynamic task in uGO. A dynamic task always has a name param, and a params param which is a list of parameters as the following example.
DynamicTaskExample =  { "name": "Test",
        "params": [
            {"name": "ReagentName", "value": "b"},
            {"name": "ReagentSlot", "value": "1"},
            {"name": "ReagentSlotsAvailable", "value": "b"},
            {"name": "ReagentType", "value": "CH"},
            {"name": "RemainingQuantity", "value": "1"}
        ],
    }

- Adjust workflow payload in the workflow_post_send.py. The payload of a workflow is simply an id, so just change this id in the function that is executed in the main of the script.

- Open a terminal and cd the folder where the scripts are and run the specific script you need. You can also add them to your project and use them as utils for an interface with uGO.
