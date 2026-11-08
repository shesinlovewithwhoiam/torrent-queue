from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class Valid:
    kind: ClassVar[str] = "valid"


@dataclass(frozen=True)
class Ready:
    kind: ClassVar[str] = "ready"


@dataclass(frozen=True)
class Reject:
    reason: str
    kind: ClassVar[str] = "reject"

    def __post_init__(self):
        if not isinstance(self.reason, str) or not self.reason:
            raise ValueError("Reject.reason must be a non-empty string")


@dataclass(frozen=True)
class Wait:
    seconds: float
    kind: ClassVar[str] = Valid.kind

    def __post_init__(self):
        if not isinstance(self.seconds, (int, float)) or self.seconds <= 0:
            raise ValueError("Wait.seconds must be a positive number")


RESULT_TYPES = (Valid, Ready, Reject, Wait)


def check_result(result):
    if not isinstance(result, RESULT_TYPES):
        raise TypeError(
            f"inspector returned {result!r}. Must return one of: Valid(), Ready(), Reject(reason), Wait(seconds)"
        )
    return result
