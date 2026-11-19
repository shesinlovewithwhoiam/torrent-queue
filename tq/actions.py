import math
from dataclasses import dataclass
from typing import Protocol, TypeAlias
from collections.abc import Iterable



class Action:
    pass

class HasIndex(Protocol):
    @property
    def index(self) -> int: ...


@dataclass(frozen=True)
class Wait(Action):
    seconds: float

    def __post_init__(self):
        s = self.seconds
        if isinstance(s, bool) or not isinstance(s, (int, float)) or not 0 < s < math.inf:
            raise TypeError(f"Wait needs a positive number of seconds, got {s!r}")


@dataclass(frozen=True)
class WaitForCompletion(Action):
    pass

@dataclass(frozen=True)
class Process(Action):
    pass


@dataclass(frozen=True)
class Reject(Action):
    reason: str

    def __post_init__(self):
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise TypeError("Reject needs a non-empty reason string")


@dataclass(frozen=True)
class Select(Action):
    files: Iterable[int | HasIndex]

    def __post_init__(self):
        try:
            raw = list(self.files)
        except TypeError:
            raise TypeError(f"Select needs an iterable of file indices, got {self.files!r}") from None
        indices = []
        for f in raw:
            i = f if isinstance(f, int) else f.index
            if isinstance(i, bool) or not isinstance(i, int) or i < 0:
                raise TypeError(f"Select got an invalid file index: {f!r}")
            indices.append(i)
        if not indices:
            raise TypeError("Select needs at least one file. If you wanted to drop the torrent, return Reject(reason) instead.")
        object.__setattr__(self, "files", tuple(sorted(set(indices))))


Actions: TypeAlias = Action | Iterable[Action]