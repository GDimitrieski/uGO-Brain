# Project Agent Rules

## General
1. Always resume world unless not prompted to specifically reset to baseline

## Workflow Consistency Rules (Prompt-Time)
1. Before changing any workflow from a user prompt, first parse the requested steps and validate them against:
  - `world/world_config.json`
  - `Available_Tasks.json`
2. If any prerequisite is missing or inconsistent, do not edit the workflow yet. Report the inconsistency first and ask for the further clarification.
3. Do not silently assume missing physical/logical steps when a prompt is incomplete.
4. When edits are made, keep assumptions explicit in the response.

## Mandatory Sequence Rules
1. When working on a station different than the uLM Plate and charge, a CameraLandmarkScan Task must be sent to reference the robot coordinate system at the station

## Consider. Be Aware
1. There might be multiple devices of the same or different types that will do the same type of processing on the samples


## Errors while prompting, code changes
1. Any error from the backend or from uGO shall not be tried to be auto repaired. It has to be prompted for clarification !
2. If any step fails, stop the sequence and report the failed prerequisite.

## How to report the reults (in which structure) as summury after workflow execution
    Executed successfully in the requested order:

    Navigate to InputStation
    SingleDeviceAction landmark scan (ACT=30) at InputStation
    Return URGRack (RACK_INPUT_URG_01) from uLMPlateStation.URGRackSlot to InputStation.URGRackSlot via SingleTask pick/place
    Result:

    InputStation.URGRackSlot now has RACK_INPUT_URG_01
    uLMPlateStation.URGRackSlot is now empty
    Robot station state is InputStation

## The Laboratory Physical WORLD:
  # AnalyzerDevice
   1. An AnalyzerDevice is an entity in the laboratory that is linked to a station i.e. placed in a station 
   2. The AnalyzerDevice can perform certain processes on the sample (classified as : centrifugation, checmical analisys, coagulation, hematology analysis, immunohematology analysis, Cooling, Archiving)
   3. Each AnalyzerDevice has limited capacity
   4. Each AnalyzerDevice can work with with certain type of racks (predefined in the laborytory world)
   5. Each AnalyzerDevice is represented by the classical PACKML approach
      5.1 Supports the Classical PACKML transitions
      5.2 Has a simple interface for the transitions i.e. the planner can request the state change by using the packML model conditions
 # Stations
    1. Stations are physical entities in the laboratory
    2. Stations have limited capacity
    3. Stations have JIG slots and can "host" certain type of Racks
    4. Each station has a Landmark
    5. Stations are identified by the ITM_ID
    6. Stations have coordinates in the LabWorld so that they can be accurately represented on the layout
    7. The Stations have landmarks for referencing
        7.1 When the uLM comes at the station coordinates, it first have to scan the landmark by using the predefined action
        7.2. With the results of the LandMark scan, the coordinate system of the Robot is adjusted at the station. It can be explained simply by, the Station becomes accessible in the BaseCoordinate System of the robot
    10. One Specific Station is the uLM BasePlate which is located on the MobileRobot itself.
        10.1 It has the ITM_ID = 1 and is always accessible to the Robot regardles of its position.
        10.2 The position of this station updates as the robot moves i.e. follows the robot
        10.3. This station has no landmark
    11. The charge station is another exception, it has no landmakrk, no objects, no devices, no jigs. It s to to be considered only like a Navigation point !
    ** One ITM_ID can hold mutiple JIG_IDs i.e. JIG Types with multiple JIG Slots
 # JIGs
    1. JIGs are holders for objects
    2. The type of the JIG is defined by JIG_ID
    3. A JIG has limited capacity, so called JIG Slots
    4. A JIG can hold only one type of object directly
      4.1. If the object is already a Rack, than the samples in the Rack are actually going to be held in the same JIG
  # Racks
    1. The Rack is container for samples
    2. The Rack has limited capacity
    3. The Rack has a pattern
    4. The Rack has a strategy for loading, not all racks can be loaded in the same way
      4.1 Centrifuge Racks sets must be loaded in a ROBIN Method. This is done to have a optimized balancing success
    5. The Rack can have blocked positions  
    6. The Racks have gripping pins, where the uLM arm can grip to transfer them 
    7. The Racks have unique virtual (logical) IDs in the world for inventory tracking
  # Samples / Blood Samples
    1. The Samples are objects with geometry : length, diametar
    2. They have caps in different colours (usually indicating the sample type i.e. what analysis the sample needs )
    3. They have barcode labels which are unique identifiers for each sample. The final classification of the type of a sample is done based on the Barcode content. Once identified i.e. classified, the Sample is a living object in the world that will undergo:
      3.1  physical transfomrations (like decapping, capping) or 
      3.2  analytic (where a certain device will analyze aliquote from the sample and make analysis) or
      3.3. Camera will recognize its cap colour which shall trigger an update in the world
    4. The processing steps for a sample can change during its life in the system
    5. The sample shall always "know" its position in the world (which rack, which station it is at)
    6. The sample shall always "know" which the transformations were done to it

## Planner:
  # It is certain that the plan for getting the samples into the world will be done with the "GETTING_NEW_SAMPLES" plan
    1. This plan may later vary in how the samples are identified and placed into the world
  # The main goal of the planning shall be then to reroute the samples based on the needed processes for each sample
    1. The dynamic plan must reconsider the world before making decisions
    2. The decisions of the dynamic planner shall be incremental and move the samples along their process maps towards the end
    3. The dynamic plan must obey to the limitation posed by the Racks, Stations, Devices, Strategies