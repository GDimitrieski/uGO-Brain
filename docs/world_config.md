# World Model Config

`workflows/rack_probe_transfer_workflow.py` loads lab topology from:
- Entrypoint: `python -m workflows.rack_probe_transfer_workflow`

- `world/world_config.json` (default)

Optional YAML is supported by `world/lab_world.py` when `PyYAML` is installed.
A YAML template is provided in:

- `world/world_config.yaml.example`

## Edit Interface

Use the CLI editor:

```powershell
python -m world.world_config_editor --config world/world_config.json show
python -m world.world_config_editor --config world/world_config.json validate
```

### Typical Updates

```powershell
python -m world.world_config_editor --config world/world_config.json upsert-station --id InputStation --itm-id 2 --kind EXTERNAL --amr-pos-target 2
python -m world.world_config_editor --config world/world_config.json upsert-station --id uLMPlateStation --itm-id 1 --kind ON_ROBOT_PLATE
python -m world.world_config_editor --config world/world_config.json upsert-slot --station-id uLMPlateStation --slot-id CentrifugeRacksSlot1 --kind CENTRIFUGE_RACK_SLOT --jig-id 2 --itm-id 1 --rack-capacity 4 --rack-pattern 1x4 --rack-rows 1 --rack-cols 4 --rack-index 1 --obj-nbr-offset 0 --accepted-rack-types CENTRIFUGE_RACK
python -m world.world_config_editor --config world/world_config.json upsert-slot --station-id uLMPlateStation --slot-id CentrifugeRacksSlot2 --kind CENTRIFUGE_RACK_SLOT --jig-id 2 --itm-id 1 --rack-capacity 4 --rack-pattern 1x4 --rack-rows 1 --rack-cols 4 --rack-index 2 --obj-nbr-offset 9 --accepted-rack-types CENTRIFUGE_RACK
python -m world.world_config_editor --config world/world_config.json upsert-slot --station-id uLMPlateStation --slot-id CentrifugeRacksSlot3 --kind CENTRIFUGE_RACK_SLOT --jig-id 2 --itm-id 1 --rack-capacity 4 --rack-pattern 1x4 --rack-rows 1 --rack-cols 4 --rack-index 3 --obj-nbr-offset 18 --accepted-rack-types CENTRIFUGE_RACK
python -m world.world_config_editor --config world/world_config.json upsert-slot --station-id uLMPlateStation --slot-id CentrifugeRacksSlot4 --kind CENTRIFUGE_RACK_SLOT --jig-id 2 --itm-id 1 --rack-capacity 4 --rack-pattern 1x4 --rack-rows 1 --rack-cols 4 --rack-index 4 --obj-nbr-offset 27 --accepted-rack-types CENTRIFUGE_RACK
python -m world.world_config_editor --config world/world_config.json upsert-rack --id RACK_NEW_01 --rack-type URG_RACK --capacity 28 --pattern URG_4x8_PIN2 --pin-obj-type 9001 --rows 8 --cols 4 --blocked-slots 15,18
python -m world.world_config_editor --config world/world_config.json set-placement --station-id InputStation --slot-id URGRackSlot --rack-id RACK_INPUT_URG_01
python -m world.world_config_editor --config world/world_config.json clear-placement --station-id uLMPlateStation --slot-id URGRackSlot
```
