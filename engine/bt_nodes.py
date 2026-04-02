from __future__ import annotations

from enum import Enum, auto
from typing import Any, Callable, Iterable, List, Optional


class Status(Enum):
    RUNNING = auto()
    SUCCESS = auto()
    FAILURE = auto()


class Blackboard(dict):
    """Shared mutable state for behavior-tree nodes."""


class Node:
    def __init__(self, name: str) -> None:
        self.name = name

    def tick(self, bb: Blackboard) -> Status:
        raise NotImplementedError

    def reset(self) -> None:
        # Override in stateful nodes.
        return


class SequenceNode(Node):
    def __init__(self, name: str, children: Iterable[Node]) -> None:
        super().__init__(name)
        self.children: List[Node] = list(children)
        self._index = 0

    def tick(self, bb: Blackboard) -> Status:
        if self._index >= len(self.children):
            self.reset()
            return Status.SUCCESS

        status = self.children[self._index].tick(bb)
        if status == Status.SUCCESS:
            self._index += 1
            if self._index >= len(self.children):
                self.reset()
                return Status.SUCCESS
            return Status.RUNNING
        if status == Status.FAILURE:
            self.reset()
            return Status.FAILURE
        return Status.RUNNING

    def reset(self) -> None:
        self._index = 0
        for child in self.children:
            child.reset()


class ConditionNode(Node):
    def __init__(self, name: str, predicate: Callable[[Blackboard], bool]) -> None:
        super().__init__(name)
        self.predicate = predicate

    def tick(self, bb: Blackboard) -> Status:
        try:
            return Status.SUCCESS if self.predicate(bb) else Status.FAILURE
        except Exception as exc:
            import traceback
            print(f"[ConditionNode:{self.name}] EXCEPTION: {exc}", flush=True)
            traceback.print_exc()
            return Status.FAILURE


class ActionNode(Node):
    def __init__(
        self,
        name: str,
        send_fn: Callable[[str, dict[str, Any], str], Any],
        task_key: str,
        overrides_fn: Optional[Callable[[Blackboard], dict[str, Any]]] = None,
    ) -> None:
        super().__init__(name)
        self.send_fn = send_fn
        self.task_key = task_key
        self.overrides_fn = overrides_fn or (lambda _bb: {})

    def tick(self, bb: Blackboard) -> Status:
        try:
            overrides = self.overrides_fn(bb) or {}
            result = self.send_fn(self.task_key, overrides, self.name)
        except Exception:
            return Status.FAILURE

        if isinstance(result, dict):
            status = str(result.get("status", "")).lower()
            if status in {"succeeded", "success", "ok"}:
                return Status.SUCCESS
            if status in {"failed", "failure", "error", "aborted", "timeout"}:
                return Status.FAILURE

        # If no explicit status contract is provided, assume success on no exception.
        return Status.SUCCESS


class RetryNode(Node):
    def __init__(self, name: str, child: Node, max_attempts: int = 3) -> None:
        super().__init__(name)
        self.child = child
        self.max_attempts = max(1, int(max_attempts))
        self._attempts = 0

    def tick(self, bb: Blackboard) -> Status:
        while self._attempts < self.max_attempts:
            status = self.child.tick(bb)
            if status == Status.SUCCESS:
                self.reset()
                return Status.SUCCESS
            if status == Status.RUNNING:
                return Status.RUNNING

            self._attempts += 1
            self.child.reset()

        self.reset()
        return Status.FAILURE

    def reset(self) -> None:
        self._attempts = 0
        self.child.reset()


class UserInteractionRetryNode(Node):
    """Retries child up to max_attempts. On final failure, posts a prompt to
    the control-system UI and waits for the user to pick an action.
    If the user picks "retry", attempts reset and the child is retried.
    Any other response (abort, timeout, dismiss) returns FAILURE."""

    def __init__(
        self,
        name: str,
        child: Node,
        prompt_fn: Callable[[str, str, list], Optional[str]],
        max_attempts: int = 3,
        prompt_title: Optional[str] = None,
        prompt_body: Optional[str] = None,
        actions: Optional[List[dict]] = None,
    ) -> None:
        super().__init__(name)
        self.child = child
        self.max_attempts = max(1, int(max_attempts))
        self.prompt_fn = prompt_fn
        self.prompt_title = prompt_title
        self.prompt_body = prompt_body
        self.actions = actions or [
            {"id": "retry", "label": "Retry"},
            {"id": "skip", "label": "Skip"},
            {"id": "abort", "label": "Abort"},
        ]
        self._attempts = 0

    def tick(self, bb: Blackboard) -> Status:
        while True:
            while self._attempts < self.max_attempts:
                status = self.child.tick(bb)
                if status == Status.SUCCESS:
                    self.reset()
                    return Status.SUCCESS
                if status == Status.RUNNING:
                    return Status.RUNNING
                self._attempts += 1
                self.child.reset()

            title = self.prompt_title or f"{self.name} Failed"
            body = (
                self.prompt_body
                or f"{self.name} failed after {self.max_attempts} attempt(s). Choose an action."
            )
            action_id = self.prompt_fn(title, body, self.actions)

            if action_id == "retry":
                self._attempts = 0
                self.child.reset()
                continue

            if action_id == "skip":
                self.reset()
                return Status.SUCCESS

            self.reset()
            return Status.FAILURE

    def reset(self) -> None:
        self._attempts = 0
        self.child.reset()


class ForEachNode(Node):
    def __init__(self, name: str, list_key: str, build_child: Callable[[Any], Node]) -> None:
        super().__init__(name)
        self.list_key = list_key
        self.build_child = build_child
        self._index = 0
        self._active_child: Optional[Node] = None
        self._items: Optional[list[Any]] = None

    def tick(self, bb: Blackboard) -> Status:
        if self._items is None:
            raw = bb.get(self.list_key, [])
            self._items = list(raw) if raw is not None else []

        while self._index < len(self._items):
            if self._active_child is None:
                self._active_child = self.build_child(self._items[self._index])

            status = self._active_child.tick(bb)
            if status == Status.RUNNING:
                return Status.RUNNING
            if status == Status.FAILURE:
                self.reset()
                return Status.FAILURE

            self._active_child.reset()
            self._active_child = None
            self._index += 1

        self.reset()
        return Status.SUCCESS

    def reset(self) -> None:
        self._index = 0
        self._items = None
        if self._active_child is not None:
            self._active_child.reset()
        self._active_child = None
