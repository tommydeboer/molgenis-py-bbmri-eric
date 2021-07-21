from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, List

from molgenis.bbmri_eric._model import Node
from molgenis.client import MolgenisRequestError


@dataclass(frozen=True)
class EricWarning:
    message: str


@dataclass
class ErrorReport:
    errors: DefaultDict[Node, MolgenisRequestError] = field(
        default_factory=lambda: defaultdict(list)
    )
    warnings: DefaultDict[Node, List[EricWarning]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_error(self, node: Node, error: MolgenisRequestError):
        self.errors[node] = error

    def add_warnings(self, node: Node, warnings: List[EricWarning]):
        self.warnings[node].extend(warnings)

    def has_errors(self):
        return len(self.errors) > 0

    def has_warnings(self):
        return len(self.warnings) > 0
