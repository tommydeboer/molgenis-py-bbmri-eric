from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, List, Optional

import requests

from molgenis.bbmri_eric.model import Node


@dataclass(frozen=True)
class EricWarning:
    """
    Class that contains a warning message. Use this when a problem occurs that
    shouldn't cancel the current action (for example staging or publishing).
    """

    message: str


class EricError(Exception):
    """
    Raise this exception when an error occurs that we can not recover from.
    """

    pass


@dataclass
class ErrorReport:
    """
    Summary object. Stores errors and warnings that occurred for each node.
    """

    nodes: List[Node]
    node_errors: DefaultDict[str, EricError] = field(
        default_factory=lambda: defaultdict(list)
    )
    node_warnings: DefaultDict[str, List[EricWarning]] = field(
        default_factory=lambda: defaultdict(list)
    )
    error: Optional[EricError] = None

    def get_node(self, code: str) -> Optional[Node]:
        return next((node for node in self.nodes if node.code == code), None)

    def add_node_error(self, node: Node, error: EricError):
        self.node_errors[node.code] = error

    def add_node_warnings(self, node: Node, warnings: List[EricWarning]):
        if warnings:
            self.node_warnings[node.code].extend(warnings)

    def set_publishing_error(self, error: EricError):
        self.error = error

    def has_errors(self) -> bool:
        return len(self.node_errors) > 0 or self.error

    def has_warnings(self) -> bool:
        return len(self.node_warnings) > 0


def requests_error_handler(func):
    """
    Decorator that catches RequestExceptions and wraps them in an EricError.
    """

    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            raise EricError("Request failed") from e

    return inner_function
