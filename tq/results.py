import enum
from typing import NamedTuple, Optional
from collections.abc import Iterable
import inspect
from .actions import Action, Process, Reject, Select, Wait, WaitForCompletion

_ALLOWED = "Wait(seconds), WaitForCompletion(), Process(), Reject(reason), Select(files). Or, combine with a tuple, e.g. (Process(), Select(files))"


class Kind(enum.StrEnum):
    CONTINUE = "continue"
    READY = "ready"
    REJECT = "reject"
    WAIT_FOR_COMPLETION = "wait_for_completion"


class Outcome(NamedTuple):
    kind: Kind
    reason: Optional[str] = None
    select: Optional[tuple] = None
    wait: Optional[float] = None


def _drain(gen):
    items = []
    while True:
        try:
            items.append(next(gen))
        except StopIteration as stop:
            if stop.value is not None:
                raise TypeError(f"Inspector returned {stop.value!r} from inside a generator, where it is ignored. Use yield instead.") from None
            return items

def parse(result):
    if isinstance(result, Action):
        items = [result]
    elif inspect.isgenerator(result):
        items = _drain(result)
    elif isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
        items = list(result)
    else:
        raise TypeError(f"Inspector returned {result!r}. Return one of: {_ALLOWED}")

    action, select = None, None
    for item in items:
        if isinstance(item, Select):
            if select is not None:
                raise TypeError("Inspector returned more than one Select")
            select = item.files
        elif isinstance(item, (Wait, WaitForCompletion, Process, Reject)):
            if action is not None:
                raise TypeError("Inspector returned more than one action. Actions are Wait(seconds), WaitForCompletion(), Process(), Reject(reason).")
            action = item
        else:
            raise TypeError(f"Inspector returned {item!r}. Return one of: {_ALLOWED}")

    if action is None:
        raise TypeError(f"Inspector returned no action. Return one of: {_ALLOWED}")

    if isinstance(action, Reject):
        if select is not None:
            raise TypeError("Reject cannot be combined with other results")
        return Outcome(Kind.REJECT, reason=action.reason)
    if isinstance(action, Process):
        return Outcome(Kind.READY, select=select)
    if isinstance(action, WaitForCompletion):
        return Outcome(Kind.WAIT_FOR_COMPLETION, select=select)
    return Outcome(Kind.CONTINUE, select=select, wait=action.seconds)