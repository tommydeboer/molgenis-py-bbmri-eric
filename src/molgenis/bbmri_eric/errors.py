from dataclasses import dataclass


class EricError(Exception):
    pass


@dataclass
class EricWarning:
    message: str


@dataclass(frozen=True)
class ConstraintViolation:
    message: str
