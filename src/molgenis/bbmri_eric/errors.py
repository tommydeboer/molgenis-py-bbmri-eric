from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, List

from molgenis.bbmri_eric._model import Node


@dataclass(frozen=True)
class EricWarning:
    message: str


class EricError(Exception):
    pass


@dataclass
class ErrorReport:
    nodes: List[Node]
    errors: DefaultDict[Node, EricError] = field(
        default_factory=lambda: defaultdict(list)
    )
    warnings: DefaultDict[Node, List[EricWarning]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_error(self, node: Node, error: EricError):
        self.errors[node] = error

    def add_warnings(self, node: Node, warnings: List[EricWarning]):
        if warnings:
            self.warnings[node].extend(warnings)

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0
