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


__all__ = ["load_last_world_state", "restore_world_from_state", "load_world_with_resume", "prepare_input_rack_for_new_batch"]
