import enum
import math
from dataclasses import dataclass
from typing import NamedTuple, Optional


class Kind(enum.StrEnum):
    CONTINUE = "continue"
    READY = "ready"
    REJECT = "reject"


@dataclass(frozen=True)
class Continue:
    wait: Optional[float] = None

    def __post_init__(self):
        s = self.wait
        if s is not None and (isinstance(s, bool) or not isinstance(s, (int, float)) or not 0 < s < math.inf):
            raise TypeError(f"Continue needs a positive number of seconds, got {s!r}")

@dataclass(frozen=True)
class Ready:
    pass

@dataclass(frozen=True)
class Reject:
    reason: str

    def __post_init__(self):
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise TypeError("Reject needs a non-empty reason string")


@dataclass(frozen=True)
class Select:
    files: tuple

    def __post_init__(self):
        try:
            raw = list(self.files)
        except TypeError:
            raise TypeError(f"Select needs an iterable of file indices, got {self.files!r}") from None
        indices = []
        for f in raw:
            i = getattr(f, "index", f)
            if isinstance(i, bool) or not isinstance(i, int) or i < 0:
                raise TypeError(f"Select got an invalid file index: {f!r}")
            indices.append(i)
        if not indices:
            raise TypeError("Select needs at least one file. If you wanted to drop the torrent, return Reject(reason) instead.")
        object.__setattr__(self, "files", tuple(sorted(set(indices))))


class Outcome(NamedTuple):
    kind: Kind
    reason: Optional[str] = None
    select: Optional[tuple] = None
    wait: Optional[float] = None


_ALLOWED = "Continue() or Continue(wait=seconds), Ready(), Reject(reason), Select(files). Or, combine with a tuple, e.g. (Ready(), Select(files))"


def parse(result):
    items = result if isinstance(result, tuple) else (result,)
    if not items:
        raise TypeError(f"Inspector returned an empty tuple. Return one of: {_ALLOWED}")

    verdicts, select = [], None
    for item in items:
        if isinstance(item, (Continue, Ready, Reject)):
            verdicts.append(item)
        elif isinstance(item, Select):
            if select is not None:
                raise TypeError("Inspector returned more than one Select")
            select = item.files
        else:
            raise TypeError(f"Inspector returned {item!r}. Return one of: {_ALLOWED}")

    if len(verdicts) > 1:
        raise TypeError("Inspector returned more than one verdict. Verdicts are Continue(), Ready(), Reject(reason).")
    verdict = verdicts[0] if verdicts else Continue()

    if isinstance(verdict, Reject):
        if select is not None:
            raise TypeError("Reject cannot be combined with other results")
        return Outcome(Kind.REJECT, reason=verdict.reason)
    if isinstance(verdict, Ready):
        return Outcome(Kind.READY, select=select)
    return Outcome(Kind.CONTINUE, select=select, wait=verdict.wait)