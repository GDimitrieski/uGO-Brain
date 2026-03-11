"""World package exports."""


def load_last_world_state(*args, **kwargs):
    from world.state_resume import load_last_world_state as _load_last_world_state

    return _load_last_world_state(*args, **kwargs)


def restore_world_from_state(*args, **kwargs):
    from world.state_resume import restore_world_from_state as _restore_world_from_state

    return _restore_world_from_state(*args, **kwargs)


def load_world_with_resume(*args, **kwargs):
    from world.state_resume import load_world_with_resume as _load_world_with_resume

    return _load_world_with_resume(*args, **kwargs)


def prepare_input_rack_for_new_batch(*args, **kwargs):
    from world.state_resume import prepare_input_rack_for_new_batch as _prepare_input_rack_for_new_batch

    return _prepare_input_rack_for_new_batch(*args, **kwargs)


def parse_update_world_device_statuses(*args, **kwargs):
    from world.update_world_mapper import parse_update_world_device_statuses as _parse_update_world_device_statuses

    return _parse_update_world_device_statuses(*args, **kwargs)


def map_update_world_devices_to_assigned_world_devices(*args, **kwargs):
    from world.update_world_mapper import (
        map_update_world_devices_to_assigned_world_devices as _map_update_world_devices_to_assigned_world_devices,
    )

    return _map_update_world_devices_to_assigned_world_devices(*args, **kwargs)


def mapped_packml_state_by_device_id(*args, **kwargs):
    from world.update_world_mapper import mapped_packml_state_by_device_id as _mapped_packml_state_by_device_id

    return _mapped_packml_state_by_device_id(*args, **kwargs)


__all__ = [
    "load_last_world_state",
    "restore_world_from_state",
    "load_world_with_resume",
    "prepare_input_rack_for_new_batch",
    "parse_update_world_device_statuses",
    "map_update_world_devices_to_assigned_world_devices",
    "mapped_packml_state_by_device_id",
]
