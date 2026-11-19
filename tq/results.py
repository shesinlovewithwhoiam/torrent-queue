import enum
from typing import NamedTuple, Optional

from .verdicts import Process, Reject, Select, Wait, WaitForCompletion


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



_ALLOWED = "Wait(delay=seconds), WaitForCompletion(), Process(), Reject(reason), Select(files). Or, combine with a tuple, e.g. (Process(), Select(files))"


def parse(result):
    items = result if isinstance(result, tuple) else (result,)
    if not items:
        raise TypeError(f"Inspector returned an empty tuple. Return one of: {_ALLOWED}")

    verdicts, select = [], None
    for item in items:
        if isinstance(item, (Wait, WaitForCompletion, Process, Reject)):
            verdicts.append(item)
        elif isinstance(item, Select):
            if select is not None:
                raise TypeError("Inspector returned more than one Select")
            select = item.files
        else:
            raise TypeError(f"Inspector returned {item!r}. Return one of: {_ALLOWED}")

    if len(verdicts) > 1:
        raise TypeError("Inspector returned more than one verdict. Verdicts are Wait(delay), WaitForCompletion(), Process(), Reject(reason).")
    if not verdicts:
        raise TypeError(f"Inspector returned no verdict. Return one of: {_ALLOWED}")

    verdict = verdicts[0]

    if isinstance(verdict, Reject):
        if select is not None:
            raise TypeError("Reject cannot be combined with other results")
        return Outcome(Kind.REJECT, reason=verdict.reason)
    
    if isinstance(verdict, Process):
        return Outcome(Kind.READY, select=select)

    if isinstance(verdict, WaitForCompletion):
        return Outcome(Kind.WAIT_FOR_COMPLETION, select=select)
    
    return Outcome(Kind.CONTINUE, select=select, wait=verdict.delay)